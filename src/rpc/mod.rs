pub mod failure;
pub mod format;
pub mod http;
pub mod response;
pub mod websocket;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::core::val::Datetime;

use futures::stream::FuturesUnordered;
use opentelemetry::Context as TelemetryContext;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::core::kvs::Datastore;
use crate::core::rpc::Data;
use crate::core::val::Value;
use crate::rpc::websocket::Websocket;
use crate::telemetry::metrics::ws::NotificationContext;

static CONN_CLOSED_ERR: &str = "Connection closed normally";

/// Information about a running query
#[derive(Debug, Clone)]
pub struct RunningQuery {
	pub query_id: Uuid,
	pub query_text: String,
	pub connection_id: Uuid,
	pub started: Datetime,
	pub cancellation_token: CancellationToken,
}

/// Information about a WebSocket connection
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
	pub connection_id: Uuid,
	pub started: Datetime,
	pub ip_address: Option<String>,
	pub namespace: Option<String>,
	pub database: Option<String>,
	pub auth: Option<crate::core::val::Value>,
	pub token: Option<crate::core::val::Value>,
}

/// A type alias for an RPC Connection
type WebSocket = Arc<Websocket>;
/// Mapping of WebSocket ID to WebSocket
type WebSockets = RwLock<HashMap<Uuid, WebSocket>>;
/// Mapping of LIVE Query ID to WebSocket ID
type LiveQueries = RwLock<HashMap<Uuid, Uuid>>;
/// Mapping of Query ID to Running Query details
type RunningQueries = RwLock<HashMap<Uuid, RunningQuery>>;
/// Mapping of Connection ID to Connection Info
type ConnectionInfos = RwLock<HashMap<Uuid, ConnectionInfo>>;

pub struct RpcState {
	/// Stores the currently connected WebSockets
	pub web_sockets: WebSockets,
	/// Stores the currently initiated LIVE queries
	pub live_queries: LiveQueries,
	/// Stores all currently running queries (including live and regular queries)
	pub running_queries: RunningQueries,
	/// Stores connection metadata (IP, auth, token, etc.)
	pub connection_infos: ConnectionInfos,
}

impl RpcState {
	pub fn new() -> Self {
		RpcState {
			web_sockets: WebSockets::default(),
			live_queries: LiveQueries::default(),
			running_queries: RunningQueries::default(),
			connection_infos: ConnectionInfos::default(),
		}
	}

	/// Start tracking a running query
	pub async fn start_query(&self, query_id: Uuid, query_text: String, connection_id: Uuid) -> CancellationToken {
		let cancellation_token = CancellationToken::new();
		let running_query = RunningQuery {
			query_id,
			query_text,
			connection_id,
			started: Datetime::now(),
			cancellation_token: cancellation_token.clone(),
		};
		self.running_queries.write().await.insert(query_id, running_query);
		tracing::debug!("Started tracking query {} on connection {}", query_id, connection_id);
		cancellation_token
	}

	/// Stop tracking a running query
	pub async fn stop_query(&self, query_id: Uuid) {
		if let Some(query) = self.running_queries.write().await.remove(&query_id) {
			tracing::debug!("Stopped tracking query {} on connection {}", query_id, query.connection_id);
		}
	}

	/// Get all running queries for a specific connection
	pub async fn get_queries_for_connection(&self, connection_id: Uuid) -> Vec<RunningQuery> {
		self.running_queries
			.read()
			.await
			.values()
			.filter(|query| query.connection_id == connection_id)
			.cloned()
			.collect()
	}

	/// Register a new connection with metadata
	pub async fn register_connection(&self, connection_info: ConnectionInfo) {
		let connection_id = connection_info.connection_id;
		self.connection_infos.write().await.insert(connection_id, connection_info);
		tracing::debug!("Registered connection {}", connection_id);
	}

	/// Unregister a connection
	pub async fn unregister_connection(&self, connection_id: Uuid) {
		self.connection_infos.write().await.remove(&connection_id);
		tracing::debug!("Unregistered connection {}", connection_id);
	}
}

/// Server-side connection provider implementation
pub struct RpcConnectionProvider {
	state: Arc<RpcState>,
}

impl RpcConnectionProvider {
	pub fn new(state: Arc<RpcState>) -> Self {
		Self { state }
	}
}

impl crate::core::catalog::ConnectionProvider for RpcConnectionProvider {
	fn list_connection_ids(&self) -> Vec<String> {
		// Snapshot keys from the WebSockets map
		let connection_ids: Vec<String> = futures::executor::block_on(async {
			self
				.state
				.web_sockets
				.read()
				.await
				.keys()
				.map(|u| u.to_string())
				.collect()
		});
		tracing::debug!("RpcConnectionProvider found {} active connections: {:?}", connection_ids.len(), connection_ids);
		connection_ids
	}

	fn get_connection_details(&self) -> std::collections::HashMap<String, crate::core::catalog::ConnectionDetails> {
		futures::executor::block_on(async {
			let mut result = std::collections::HashMap::new();

			// Get all WebSocket connections and their metadata
			let web_sockets = self.state.web_sockets.read().await;
			let running_queries = self.state.running_queries.read().await;
			let connection_infos = self.state.connection_infos.read().await;

			for (connection_id, websocket) in web_sockets.iter() {
				let connection_id_str = connection_id.to_string();

				// Get all running queries for this connection
				let mut queries = std::collections::HashMap::new();
				for (query_id, running_query) in running_queries.iter() {
					if running_query.connection_id == *connection_id {
						let query_details = crate::core::catalog::QueryDetails {
							query: running_query.query_text.clone(),
							connection: *connection_id,
							started: Value::from(running_query.started.clone()),
						};
						queries.insert(query_id.to_string(), query_details);
					}
				}

				// Get current session data from the WebSocket
				let current_session = websocket.session.load_full();
				
				// Get connection metadata if available
				let connection_info = connection_infos.get(connection_id);
				let (started, ip_address) = if let Some(info) = connection_info {
					(
						Value::from(info.started.clone()),
						info.ip_address.clone(),
					)
				} else {
					// Fallback to current time if no metadata available
					(
						Value::from(Datetime::now()),
						None,
					)
				};

				// Get current namespace, database, auth and token from the session
				let namespace = current_session.ns.clone();
				let database = current_session.db.clone();
				let auth = current_session.rd.clone();
				let token = current_session.tk.clone();

				// Create connection details
				let connection_details = crate::core::catalog::ConnectionDetails {
					started,
					ip_address,
					namespace,
					database,
					auth,
					token,
					queries,
				};

				result.insert(connection_id_str, connection_details);
			}

			tracing::debug!("RpcConnectionProvider returning details for {} connections", result.len());
			result
		})
	}

	fn kill_query(&self, query_id: uuid::Uuid) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
		Box::pin(async move {
			// Try to find and kill the query
			if let Some(query) = self.state.running_queries.write().await.remove(&query_id) {
				// Cancel the query execution
				query.cancellation_token.cancel();
				tracing::info!("Killed query {} on connection {}", query_id, query.connection_id);
				true
			} else {
				tracing::warn!("Query {} not found for killing", query_id);
				false
			}
		})
	}
}

/// Performs notification delivery to the WebSockets
pub(crate) async fn notifications(
	ds: Arc<Datastore>,
	state: Arc<RpcState>,
	canceller: CancellationToken,
) {
	// Store messages being delivered
	let mut futures = FuturesUnordered::new();
	// Listen to the notifications channel
	if let Some(channel) = ds.notifications() {
		// Loop continuously
		loop {
			tokio::select! {
				//
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Process any buffered messages
				Some(_) = futures.next() => continue,
				// Receive a notification on the channel
				Ok(notification) = channel.recv() => {
					// Get the id for this notification
					let id = notification.id.as_ref();
					// Get the WebSocket for this notification
					let websocket = {
						state.live_queries.read().await.get(id).copied()
					};
					// Ensure the specified WebSocket exists
					if let Some(id) = websocket.as_ref() {
						// Get the WebSocket for this notification
						let websocket = {
							state.web_sockets.read().await.get(id).cloned()
						};
						// Ensure the specified WebSocket exists
						if let Some(rpc) = websocket {
							// Serialize the message to send
							let message = response::success(None, Data::Live(notification));
							// Add telemetry metrics
							let cx = TelemetryContext::new();
							let not_ctx = NotificationContext::default()
							      .with_live_id(id.to_string());
							let cx = Arc::new(cx.with_value(not_ctx));
							// Get the WebSocket output format
							let format = rpc.format;
							// Get the WebSocket sending channel
							let sender = rpc.channel.clone();
							// Send the notification to the client
							let future = message.send(cx, format, sender);
							// Pus the future to the pipeline
							futures.push(future);
						}
					}
				},
			}
		}
	}
}

/// Closes all WebSocket connections, waiting for graceful shutdown
pub(crate) async fn graceful_shutdown(state: Arc<RpcState>) {
	// Close WebSocket connections, ensuring queued messages are processed
	for (_, rpc) in state.web_sockets.read().await.iter() {
		rpc.shutdown.cancel();
	}
	// Wait for all existing WebSocket connections to finish sending
	while !state.web_sockets.read().await.is_empty() {
		tokio::time::sleep(Duration::from_millis(250)).await;
	}
}

/// Forces a fast shutdown of all WebSocket connections
pub(crate) fn shutdown(state: Arc<RpcState>) {
	// Close all WebSocket connections immediately
	if let Ok(mut writer) = state.web_sockets.try_write() {
		writer.drain();
	}
}
