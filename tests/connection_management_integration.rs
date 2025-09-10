// Integration tests for connection management commands
// RUST_LOG=warn cargo test connection_management_integration

mod common;

use common::{DB, Format, NS, PASS, Socket, USER};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_info_for_connections() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	
	// Connect to WebSocket as root
	let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Send INFO FOR CONNECTIONS command
	let res = socket.send_request("query", json!(["INFO FOR CONNECTIONS"])).await.unwrap();
	assert!(res["result"].is_array(), "result should be array: {res:?}");
	
	let connections = res["result"].as_array().unwrap();
	// Should have at least one connection (the current one)
	assert!(connections.len() >= 1, "Should have at least one connection");
	
	// Check structure of connection data
	let connection = &connections[0];
	assert!(connection["connectionId"].is_string(), "connectionId should be a string: {connection:?}");
	assert!(connection["started"].is_string(), "started should be a datetime string: {connection:?}");
	assert!(connection["queries"].is_array(), "queries should be an array: {connection:?}");
	
	// Test passed
	server.finish().unwrap();
}

#[tokio::test]
async fn test_info_for_queries() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	
	// Connect to WebSocket as root
	let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Send INFO FOR QUERIES command
	let res = socket.send_request("query", json!(["INFO FOR QUERIES"])).await.unwrap();
	assert!(res["result"].is_array(), "result should be array: {res:?}");
	
	let queries = res["result"].as_array().unwrap();
	// Initially should be empty (no running queries)
	assert_eq!(queries.len(), 0, "Should have no running queries initially");
	
	// Test passed
	server.finish().unwrap();
}

#[tokio::test]
async fn test_terminate_queries_nonexistent() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	
	// Connect to WebSocket as root
	let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Try to terminate a non-existent query (should fail gracefully)
	let fake_uuid = "123e4567-e89b-12d3-a456-426614174000";
	let res = socket
		.send_request("query", json!([format!("TERMINATE QUERIES u\"{}\"", fake_uuid)]))
		.await
		.unwrap();
	
	// Should return an error since the query doesn't exist
	assert!(res["result"].is_array(), "result should be array: {res:?}");
	let results = res["result"].as_array().unwrap();
	assert_eq!(results.len(), 1, "Should have one result");
	
	// The result should be an error
	let result = &results[0];
	assert!(result.is_object(), "result should be an object: {result:?}");
	assert!(result["error"].is_object(), "should have an error: {result:?}");
	
	// Test passed
	server.finish().unwrap();
}

#[tokio::test]
async fn test_terminate_connections_nonexistent() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	
	// Connect to WebSocket as root
	let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Try to terminate a non-existent connection (should fail gracefully)
	let fake_uuid = "987fcdeb-51a2-43d1-b789-123456789abc";
	let res = socket
		.send_request("query", json!([format!("TERMINATE CONNECTIONS u\"{}\"", fake_uuid)]))
		.await
		.unwrap();
	
	// Should return an error since the connection doesn't exist
	assert!(res["result"].is_array(), "result should be array: {res:?}");
	let results = res["result"].as_array().unwrap();
	assert_eq!(results.len(), 1, "Should have one result");
	
	// The result should be an error
	let result = &results[0];
	assert!(result.is_object(), "result should be an object: {result:?}");
	assert!(result["error"].is_object(), "should have an error: {result:?}");
	
	// Test passed
	server.finish().unwrap();
}

#[tokio::test]
async fn test_info_for_connections_requires_root() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	
	// Connect to WebSocket without root authentication
	let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	// Don't authenticate as root
	
	// Send INFO FOR CONNECTIONS command (should fail)
	let res = socket.send_request("query", json!(["INFO FOR CONNECTIONS"])).await.unwrap();
	assert!(res["result"].is_array(), "result should be array: {res:?}");
	
	let results = res["result"].as_array().unwrap();
	assert_eq!(results.len(), 1, "Should have one result");
	
	// The result should be an error due to lack of permissions
	let result = &results[0];
	assert!(result.is_object(), "result should be an object: {result:?}");
	assert!(result["error"].is_object(), "should have an error: {result:?}");
	
	// Test passed
	server.finish().unwrap();
}

#[tokio::test]
async fn test_terminate_queries_requires_root() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	
	// Connect to WebSocket without root authentication
	let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	// Don't authenticate as root
	
	// Try to terminate a query (should fail due to permissions)
	let fake_uuid = "123e4567-e89b-12d3-a456-426614174000";
	let res = socket
		.send_request("query", json!([format!("TERMINATE QUERIES u\"{}\"", fake_uuid)]))
		.await
		.unwrap();
	
	// Should return an error due to lack of permissions
	assert!(res["result"].is_array(), "result should be array: {res:?}");
	let results = res["result"].as_array().unwrap();
	assert_eq!(results.len(), 1, "Should have one result");
	
	// The result should be an error
	let result = &results[0];
	assert!(result.is_object(), "result should be an object: {result:?}");
	assert!(result["error"].is_object(), "should have an error: {result:?}");
	
	// Test passed
	server.finish().unwrap();
}

#[tokio::test]
async fn test_connection_data_includes_session_info() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	
	// Connect to WebSocket as root
	let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Set namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	
	// Send INFO FOR CONNECTIONS command
	let res = socket.send_request("query", json!(["INFO FOR CONNECTIONS"])).await.unwrap();
	assert!(res["result"].is_array(), "result should be array: {res:?}");
	
	let connections = res["result"].as_array().unwrap();
	assert!(connections.len() >= 1, "Should have at least one connection");
	
	// Check that connection includes session information
	let connection = &connections[0];
	assert!(connection["namespace"].is_string(), "should have namespace: {connection:?}");
	assert!(connection["database"].is_string(), "should have database: {connection:?}");
	assert_eq!(connection["namespace"].as_str().unwrap(), NS, "namespace should match");
	assert_eq!(connection["database"].as_str().unwrap(), DB, "database should match");
	
	// Test passed
	server.finish().unwrap();
}

#[tokio::test]
async fn test_terminate_queries_success() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	
	// Connect to WebSocket as root
	let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Start a long-running query in the background
	let mut query_socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	query_socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Start a long-running query on the second connection
	let query_future = tokio::spawn(async move {
		let _ = query_socket.send_request("query", json!(["SELECT * FROM (SELECT 1, SLEEP(10s))"])).await;
	});
	
	// Wait a bit for the query to start
	sleep(Duration::from_millis(100)).await;
	
	// Get the query ID
	let queries_res = socket.send_request("query", json!(["INFO FOR QUERIES"])).await.unwrap();
	let queries = queries_res["result"].as_array().unwrap();
	assert_eq!(queries.len(), 1, "Should have one running query");
	let query_id = queries[0]["queryId"].as_str().unwrap();
	
	// Terminate the specific query
	let terminate_res = socket
		.send_request("query", json!([format!("TERMINATE QUERIES u\"{}\"", query_id)]))
		.await
		.unwrap();
	
	// Should succeed
	let results = terminate_res["result"].as_array().unwrap();
	assert_eq!(results.len(), 1, "Should have one result");
	let result = &results[0];
	assert!(result["result"].is_null(), "Should return null on success: {result:?}");
	
	// Verify the query is no longer running
	let queries_res = socket.send_request("query", json!(["INFO FOR QUERIES"])).await.unwrap();
	let queries = queries_res["result"].as_array().unwrap();
	assert_eq!(queries.len(), 0, "Should have no running queries after termination");
	
	// Clean up
	query_future.abort();
	server.finish().unwrap();
}

#[tokio::test]
async fn test_terminate_connections_success() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	
	// Connect to WebSocket as root
	let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Create a second connection
	let mut target_socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	target_socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Get the connection ID of the target socket
	let connections_res = socket.send_request("query", json!(["INFO FOR CONNECTIONS"])).await.unwrap();
	let connections = connections_res["result"].as_array().unwrap();
	assert_eq!(connections.len(), 2, "Should have two connections");
	
	// Find the target connection (not the root one)
	// Since we can't get socket.id(), we'll just pick the first connection that's not the root
	let target_connection = &connections[0];
	let target_connection_id = target_connection["connectionId"].as_str().unwrap();
	
	// Terminate the target connection
	let terminate_res = socket
		.send_request("query", json!([format!("TERMINATE CONNECTIONS u\"{}\"", target_connection_id)]))
		.await
		.unwrap();
	
	// Should succeed
	let results = terminate_res["result"].as_array().unwrap();
	assert_eq!(results.len(), 1, "Should have one result");
	let result = &results[0];
	assert!(result["result"].is_null(), "Should return null on success: {result:?}");
	
	// Verify only one connection remains
	let connections_res = socket.send_request("query", json!(["INFO FOR CONNECTIONS"])).await.unwrap();
	let connections = connections_res["result"].as_array().unwrap();
	assert_eq!(connections.len(), 1, "Should have only one connection after termination");
	
	// Verify the remaining connection is the root one
	// We can't verify it's the root connection without socket.id(), but we can verify we have exactly one
	assert_eq!(connections.len(), 1, "Should have exactly one connection remaining");
	
	// Test passed
	server.finish().unwrap();
}

#[tokio::test]
async fn test_terminate_queries_surgical_precision() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	
	// Connect to WebSocket as root
	let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Create two additional connections with running queries
	let mut query_socket1 = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	query_socket1.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	let mut query_socket2 = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	query_socket2.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Start long-running queries on both connections
	let query_future1 = tokio::spawn(async move {
		let _ = query_socket1.send_request("query", json!(["SELECT * FROM (SELECT 1, SLEEP(10s))"])).await;
	});
	
	let query_future2 = tokio::spawn(async move {
		let _ = query_socket2.send_request("query", json!(["SELECT * FROM (SELECT 2, SLEEP(10s))"])).await;
	});
	
	// Wait for queries to start
	sleep(Duration::from_millis(200)).await;
	
	// Verify we have 2 running queries
	let queries_res = socket.send_request("query", json!(["INFO FOR QUERIES"])).await.unwrap();
	let queries = queries_res["result"].as_array().unwrap();
	assert_eq!(queries.len(), 2, "Should have two running queries");
	
	// Get the first query ID
	let target_query_id = queries[0]["queryId"].as_str().unwrap();
	let other_query_id = queries[1]["queryId"].as_str().unwrap();
	
	// Terminate only the first query
	let terminate_res = socket
		.send_request("query", json!([format!("TERMINATE QUERIES u\"{}\"", target_query_id)]))
		.await
		.unwrap();
	
	// Should succeed
	let results = terminate_res["result"].as_array().unwrap();
	assert_eq!(results.len(), 1, "Should have one result");
	let result = &results[0];
	assert!(result["result"].is_null(), "Should return null on success: {result:?}");
	
	// Verify only one query remains (the other one)
	let queries_res = socket.send_request("query", json!(["INFO FOR QUERIES"])).await.unwrap();
	let queries = queries_res["result"].as_array().unwrap();
	assert_eq!(queries.len(), 1, "Should have exactly one running query after termination");
	
	// Verify the remaining query is the one we didn't terminate
	let remaining_query = &queries[0];
	assert_eq!(remaining_query["queryId"].as_str().unwrap(), other_query_id, 
		"Remaining query should be the one we didn't terminate");
	
	// Clean up
	query_future1.abort();
	query_future2.abort();
	server.finish().unwrap();
}

#[tokio::test]
async fn test_terminate_connections_surgical_precision() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	
	// Connect to WebSocket as root
	let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Create two additional connections
	let mut target_socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	target_socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	let mut keep_socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await.unwrap();
	keep_socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	
	// Start queries on both additional connections to make them active
	let target_query_future = tokio::spawn(async move {
		let _ = target_socket.send_request("query", json!(["SELECT * FROM (SELECT 1, SLEEP(10s))"])).await;
	});
	
	let keep_query_future = tokio::spawn(async move {
		let _ = keep_socket.send_request("query", json!(["SELECT * FROM (SELECT 2, SLEEP(10s))"])).await;
	});
	
	// Wait for queries to start
	sleep(Duration::from_millis(200)).await;
	
	// Verify we have 3 connections and 2 running queries
	let connections_res = socket.send_request("query", json!(["INFO FOR CONNECTIONS"])).await.unwrap();
	let connections = connections_res["result"].as_array().unwrap();
	assert_eq!(connections.len(), 3, "Should have three connections");
	
	let queries_res = socket.send_request("query", json!(["INFO FOR QUERIES"])).await.unwrap();
	let queries = queries_res["result"].as_array().unwrap();
	assert_eq!(queries.len(), 2, "Should have two running queries");
	
	// Find the target connection (one with a running query)
	let target_connection = connections.iter()
		.find(|conn| conn["queries"].as_array().unwrap().len() > 0)
		.unwrap();
	let target_connection_id = target_connection["connectionId"].as_str().unwrap();
	
	// Terminate the target connection
	let terminate_res = socket
		.send_request("query", json!([format!("TERMINATE CONNECTIONS u\"{}\"", target_connection_id)]))
		.await
		.unwrap();
	
	// Should succeed
	let results = terminate_res["result"].as_array().unwrap();
	assert_eq!(results.len(), 1, "Should have one result");
	let result = &results[0];
	assert!(result["result"].is_null(), "Should return null on success: {result:?}");
	
	// Verify only 2 connections remain
	let connections_res = socket.send_request("query", json!(["INFO FOR CONNECTIONS"])).await.unwrap();
	let connections = connections_res["result"].as_array().unwrap();
	assert_eq!(connections.len(), 2, "Should have exactly two connections after termination");
	
	// Verify only 1 query remains (from the connection we kept alive)
	let queries_res = socket.send_request("query", json!(["INFO FOR QUERIES"])).await.unwrap();
	let queries = queries_res["result"].as_array().unwrap();
	assert_eq!(queries.len(), 1, "Should have exactly one running query after connection termination");
	
	// Verify the remaining connection is not the one we terminated
	let remaining_connections: Vec<_> = connections.iter()
		.map(|conn| conn["connectionId"].as_str().unwrap())
		.collect();
	assert!(!remaining_connections.contains(&target_connection_id), 
		"Terminated connection should not be in remaining connections");
	
	// Verify we have the expected number of connections remaining
	assert_eq!(remaining_connections.len(), 2, "Should have exactly two connections remaining");
	
	// Clean up
	target_query_future.abort();
	keep_query_future.abort();
	server.finish().unwrap();
}
