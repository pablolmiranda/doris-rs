//! doris-rs BE execution for the write path (M4): decoding the write fragments the FE sends
//! (`INSERT … VALUES`, stream load), evaluating their constant and cast expressions, and the
//! OLAP table sink that routes rows to tablets, writes rowsets and commits them.

pub mod aggregate;
pub mod analytic;
pub mod arith;
pub mod block;
pub mod clock;
pub mod columnar;
pub mod cond;
pub mod datagen;
pub mod error_log;
pub mod eval;
pub mod exchange;
pub mod expr;
pub mod file_scan;
pub mod fragment;
pub mod funnel;
pub mod hash_join;
pub mod hll;
pub mod nested_loop;
pub mod partition_sort;
pub mod pipeline;
pub mod query;
pub mod regexp;
pub mod repeat;
pub mod round;
pub mod route;
pub mod sort;
pub mod row_text;
pub mod schema_scan;
pub mod set_op;
pub mod sink;
pub mod strings;
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
    /// A query failure with the C++ BE's own status: the `TStatusCode` value it sends, and the
    /// code name it prints in `[...]` (`Status::to_thrift`).
    #[error("{msg}")]
    Coded { code: i32, name: &'static str, msg: String },
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
