use std::fmt;

use crate::sql::Expr;

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum TerminateStatement {
	// Terminate a specific query by ID
	Query(Expr),
	// Terminate an entire connection by ID
	Connection(Expr),
}

impl fmt::Display for TerminateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			TerminateStatement::Query(id) => write!(f, "TERMINATE QUERIES {}", id),
			TerminateStatement::Connection(id) => write!(f, "TERMINATE CONNECTIONS {}", id),
		}
	}
}

impl From<TerminateStatement> for crate::expr::statements::TerminateStatement {
	fn from(v: TerminateStatement) -> Self {
		match v {
			TerminateStatement::Query(id) => Self::Query(id.into()),
			TerminateStatement::Connection(id) => Self::Connection(id.into()),
		}
	}
}

impl From<crate::expr::statements::TerminateStatement> for TerminateStatement {
	fn from(v: crate::expr::statements::TerminateStatement) -> Self {
		match v {
			crate::expr::statements::TerminateStatement::Query(id) => Self::Query(id.into()),
			crate::expr::statements::TerminateStatement::Connection(id) => Self::Connection(id.into()),
		}
	}
}
