use std::fmt;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::{Action, Options};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Base, ResourceKind};
use crate::expr::{Expr, FlowResultExt as _};
use crate::val::{Uuid, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum TerminateStatement {
	// Terminate a specific query by ID
	Query(Expr),
	// Terminate an entire connection by ID
	Connection(Expr),
}

impl TerminateStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Only root can terminate queries/connections
		opt.is_allowed(Action::Edit, ResourceKind::Any, &Base::Root)?;
		// Valid options?
		opt.valid_for_db()?;
		
		match self {
			TerminateStatement::Query(id) => {
				// Resolve query id
				let qid = match stk
					.run(|stk| id.compute(stk, ctx, opt, None))
					.await
					.catch_return()?
					.cast_to::<Uuid>()
				{
					Err(_) => {
						bail!(Error::TerminateStatement {
							value: id.to_string(),
						})
					}
					Ok(id) => id,
				};
				
				// Try to terminate the query via the connection provider
				if let Some(provider) = ctx.get_connection_provider() {
					if provider.kill_query(qid.0).await {
						Ok(Value::None)
					} else {
						bail!(Error::TerminateStatement {
							value: id.to_string(),
						});
					}
				} else {
					bail!(Error::TerminateStatement {
						value: id.to_string(),
					});
				}
			}
			TerminateStatement::Connection(id) => {
				// Resolve connection id
				let cid = match stk
					.run(|stk| id.compute(stk, ctx, opt, None))
					.await
					.catch_return()?
					.cast_to::<Uuid>()
				{
					Err(_) => {
						bail!(Error::TerminateStatement {
							value: id.to_string(),
						})
					}
					Ok(id) => id,
				};
				
				// Try to terminate the connection via the connection provider
				if let Some(provider) = ctx.get_connection_provider() {
					if provider.kill_connection(cid.0).await {
						Ok(Value::None)
					} else {
						bail!(Error::TerminateStatement {
							value: id.to_string(),
						});
					}
				} else {
					bail!(Error::TerminateStatement {
						value: id.to_string(),
					});
				}
			}
		}
	}
}

impl fmt::Display for TerminateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			TerminateStatement::Query(id) => write!(f, "TERMINATE QUERIES {}", id),
			TerminateStatement::Connection(id) => write!(f, "TERMINATE CONNECTIONS {}", id),
		}
	}
}
