//! doris-rs BE execution for the write path (M4): decoding the write fragments the FE sends
//! (`INSERT … VALUES`, stream load), evaluating their constant and cast expressions, and the
//! OLAP table sink that routes rows to tablets, writes rowsets and commits them.

pub mod aggregate;
pub mod block;
pub mod error_log;
pub mod eval;
pub mod exchange;
pub mod expr;
pub mod file_scan;
pub mod fragment;
pub mod pipeline;
pub mod query;
pub mod route;
pub mod sort;
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
    /// A read the storage refused or failed.
    #[error("{0}")]
    Storage(#[from] doris_storage::StorageError),
    /// The query was cancelled, by the FE or by another of its instances failing.
    #[error("{0}")]
    Cancelled(String),
    /// A query that failed while it ran (an overflow, a malformed value).
    #[error("{0}")]
    Internal(String),
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
