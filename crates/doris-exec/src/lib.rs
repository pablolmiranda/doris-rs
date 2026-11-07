//! doris-rs BE execution for the write path (M4): decoding the write fragments the FE sends
//! (`INSERT … VALUES`, stream load), evaluating their constant and cast expressions, and the
//! OLAP table sink that routes rows to tablets, writes rowsets and commits them.

pub mod block;
pub mod error_log;
pub mod eval;
pub mod expr;
pub mod file_scan;
pub mod fragment;
pub mod route;
pub mod row_text;
pub mod sink;
pub mod value;

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    /// A plan node, sink, type or expression outside the write path doris-rs runs.
    #[error("doris-rs does not support {what} yet")]
    Unsupported { what: String },
    #[error("bad fragment request: {0}")]
    BadRequest(String),
    /// A body over the BE's size limit (`EXCEEDED_LIMIT`).
    #[error("{0}")]
    ExceededLimit(String),
}

/// A row the sink filters: the C++ error message, and the source line (empty for VALUES).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowError {
    pub msg: String,
    pub line: String,
}

impl RowError {
    pub fn new(msg: impl Into<String>) -> RowError {
        RowError { msg: msg.into(), line: String::new() }
    }
}
