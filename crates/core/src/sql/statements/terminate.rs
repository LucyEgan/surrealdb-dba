use std::fmt;

use crate::sql::Expr;

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct TerminateStatement {
	// Uuid of Query to terminate
	// or Param resolving to Uuid of Query
	pub id: Expr,
}

impl fmt::Display for TerminateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "TERMINATE QUERY {}", self.id)
	}
}

impl From<TerminateStatement> for crate::expr::statements::TerminateStatement {
	fn from(v: TerminateStatement) -> Self {
		Self {
			id: v.id.into(),
		}
	}
}

impl From<crate::expr::statements::TerminateStatement> for TerminateStatement {
	fn from(v: crate::expr::statements::TerminateStatement) -> Self {
		Self {
			id: v.id.into(),
		}
	}
}
