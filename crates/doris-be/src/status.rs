//! `TStatus` values as the C++ BE's `Status::to_thrift` writes them.

use doris_thrift::gen::status::{TStatus, TStatusCode};

pub fn ok() -> TStatus {
    TStatus { status_code: TStatusCode::OK, error_msgs: None }
}

/// `Status::to_thrift` for an error: one message, `(<localhost>)[<code name>]<message>`.
pub fn error(code: TStatusCode, code_name: &str, localhost: &str, msg: &str) -> TStatus {
    TStatus { status_code: code, error_msgs: Some(vec![format!("({localhost})[{code_name}]{msg}")]) }
}

pub fn internal_error(localhost: &str, msg: &str) -> TStatus {
    error(TStatusCode::INTERNAL_ERROR, "INTERNAL_ERROR", localhost, msg)
}

/// The status doris-rs answers with for anything it does not implement yet.
pub fn not_implemented(localhost: &str, what: &str) -> TStatus {
    error(TStatusCode::NOT_IMPLEMENTED_ERROR, "NOT_IMPLEMENTED_ERROR", localhost, &format!("doris-rs does not support {what} yet"))
}

/// A status code by its IDL name, as the C++ BE prints it in `[<code>]` (`TStatusCode(<n>)` for
/// a code the IDL does not name).
pub fn code_name(code: TStatusCode) -> String {
    let name = match code {
        TStatusCode::OK => "OK",
        TStatusCode::CANCELLED => "CANCELLED",
        TStatusCode::ANALYSIS_ERROR => "ANALYSIS_ERROR",
        TStatusCode::NOT_IMPLEMENTED_ERROR => "NOT_IMPLEMENTED_ERROR",
        TStatusCode::RUNTIME_ERROR => "RUNTIME_ERROR",
        TStatusCode::MEM_LIMIT_EXCEEDED => "MEM_LIMIT_EXCEEDED",
        TStatusCode::INTERNAL_ERROR => "INTERNAL_ERROR",
        TStatusCode::THRIFT_RPC_ERROR => "THRIFT_RPC_ERROR",
        TStatusCode::TIMEOUT => "TIMEOUT",
        TStatusCode::LIMIT_REACH => "LIMIT_REACH",
        TStatusCode::MEM_ALLOC_FAILED => "MEM_ALLOC_FAILED",
        TStatusCode::BUFFER_ALLOCATION_FAILED => "BUFFER_ALLOCATION_FAILED",
        TStatusCode::MINIMUM_RESERVATION_UNAVAILABLE => "MINIMUM_RESERVATION_UNAVAILABLE",
        TStatusCode::PUBLISH_TIMEOUT => "PUBLISH_TIMEOUT",
        TStatusCode::LABEL_ALREADY_EXISTS => "LABEL_ALREADY_EXISTS",
        TStatusCode::TOO_MANY_TASKS => "TOO_MANY_TASKS",
        TStatusCode::END_OF_FILE => "END_OF_FILE",
        TStatusCode::NOT_FOUND => "NOT_FOUND",
        TStatusCode::CORRUPTION => "CORRUPTION",
        TStatusCode::INVALID_ARGUMENT => "INVALID_ARGUMENT",
        TStatusCode::IO_ERROR => "IO_ERROR",
        TStatusCode::ALREADY_EXIST => "ALREADY_EXIST",
        TStatusCode::NETWORK_ERROR => "NETWORK_ERROR",
        TStatusCode::ILLEGAL_STATE => "ILLEGAL_STATE",
        TStatusCode::NOT_AUTHORIZED => "NOT_AUTHORIZED",
        TStatusCode::ABORTED => "ABORTED",
        TStatusCode::DIRECTORY_NOT_EMPTY => "DIRECTORY_NOT_EMPTY",
        TStatusCode::UNINITIALIZED => "UNINITIALIZED",
        TStatusCode::INCOMPLETE => "INCOMPLETE",
        TStatusCode::OLAP_ERR_VERSION_ALREADY_MERGED => "OLAP_ERR_VERSION_ALREADY_MERGED",
        TStatusCode::DATA_QUALITY_ERROR => "DATA_QUALITY_ERROR",
        TStatusCode::INVALID_JSON_PATH => "INVALID_JSON_PATH",
        TStatusCode::JNI_ERROR => "JNI_ERROR",
        TStatusCode::BINLOG_DISABLE => "BINLOG_DISABLE",
        TStatusCode::BINLOG_TOO_OLD_COMMIT_SEQ => "BINLOG_TOO_OLD_COMMIT_SEQ",
        TStatusCode::BINLOG_TOO_NEW_COMMIT_SEQ => "BINLOG_TOO_NEW_COMMIT_SEQ",
        TStatusCode::BINLOG_NOT_FOUND_DB => "BINLOG_NOT_FOUND_DB",
        TStatusCode::BINLOG_NOT_FOUND_TABLE => "BINLOG_NOT_FOUND_TABLE",
        TStatusCode::SNAPSHOT_NOT_EXIST => "SNAPSHOT_NOT_EXIST",
        TStatusCode::HTTP_ERROR => "HTTP_ERROR",
        TStatusCode::TABLET_MISSING => "TABLET_MISSING",
        TStatusCode::NOT_MASTER => "NOT_MASTER",
        TStatusCode::OBTAIN_LOCK_FAILED => "OBTAIN_LOCK_FAILED",
        TStatusCode::SNAPSHOT_EXPIRED => "SNAPSHOT_EXPIRED",
        TStatusCode::FINISHED => "FINISHED",
        TStatusCode::DELETE_BITMAP_LOCK_ERROR => "DELETE_BITMAP_LOCK_ERROR",
        TStatusCode::SC_COMPACTION_CONFLICT => "SC_COMPACTION_CONFLICT",
        other => return format!("{other:?}"),
    };
    name.to_string()
}
