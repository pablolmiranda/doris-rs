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
