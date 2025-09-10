use std::fmt;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Expr, FlowResultExt as _};
use crate::val::{Uuid, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TerminateStatement {
	// Uuid of Query to terminate
	// or Param resolving to Uuid of Query
	pub id: Expr,
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
		// Valid options?
		opt.valid_for_db()?;
		// Resolve query id
		let qid = match stk
			.run(|stk| self.id.compute(stk, ctx, opt, None))
			.await
			.catch_return()?
			.cast_to::<Uuid>()
		{
			Err(_) => {
				bail!(Error::TerminateStatement {
					value: self.id.to_string(),
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
					value: self.id.to_string(),
				});
			}
		} else {
			bail!(Error::TerminateStatement {
				value: self.id.to_string(),
			});
		}
	}
}

impl fmt::Display for TerminateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "TERMINATE QUERY {}", self.id)
	}
}
