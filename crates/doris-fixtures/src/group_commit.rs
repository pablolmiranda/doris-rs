//! Group-commit messages of four suites, extracted from the traffic corpus by
//! `crates/doris-wire/src/bin/extract-group-commit.rs` (`crates/doris-fixtures/group_commit`),
//! in capture order. Messages of several connections interleave: a report can come before the
//! fragment request it answers.
use std::collections::BTreeMap;
use std::path::PathBuf;

use doris_proto::doris::{PCancelPlanFragmentRequest, PExecPlanFragmentRequest};
use doris_thrift::gen::agent_service::TAgentTaskRequest;
use doris_thrift::gen::backend_service::BackendServiceSubmitTasksArgs;
use doris_thrift::gen::frontend_service::{
    FrontendServiceLoadTxnCommitArgs, FrontendServiceLoadTxnRollbackArgs, FrontendServiceReportExecStatusArgs, FrontendServiceStreamLoadPutArgs,
    FrontendServiceStreamLoadPutResult, TLoadTxnCommitRequest, TLoadTxnRollbackRequest, TReportExecStatusParams, TStreamLoadPutRequest,
    TStreamLoadPutResult,
};
use doris_thrift::gen::palo_internal_service::{TPipelineFragmentParams, TPipelineFragmentParamsList};
use prost::Message;
use thrift::protocol::{TCompactInputProtocol, TSerializable};

use crate::decode;

pub const TIMEOUT: &str = "insert_p0.test_group_commit_timeout.test_group_commit_timeout";
pub const INTERVAL: &str = "insert_p0.test_group_commit_interval_ms_property.test_group_commit_interval_ms_property";
pub const DATA_BYTES: &str = "insert_p0.test_group_commit_data_bytes_property.test_group_commit_data_bytes_property";
pub const LARGE_DATA: &str = "insert_p0.insert_group_commit_with_large_data.insert_group_commit_with_large_data";

/// Test fixtures, decoded once: the size of a variant does not matter.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Msg {
    /// The CREATE tasks of a `submit_tasks` call.
    Create(Vec<TAgentTaskRequest>),
    Exec(PExecPlanFragmentRequest),
    Cancel(PCancelPlanFragmentRequest),
    Put(TStreamLoadPutRequest),
    PutReply(TStreamLoadPutResult),
    Report(TReportExecStatusParams),
    Commit(TLoadTxnCommitRequest),
    Rollback(TLoadTxnRollbackRequest),
    /// A stream load: its headers (names lower-cased) and body.
    HttpRequest {
        headers: BTreeMap<String, String>,
        body: Vec<u8>,
    },
    /// The stream load's reply body.
    HttpResponse(String),
}

impl Msg {
    /// The one fragment of a request (compact Thrift).
    pub fn params(req: &PExecPlanFragmentRequest) -> TPipelineFragmentParams {
        let mut b = req.request.as_deref().expect("request bytes");
        let list = TPipelineFragmentParamsList::read_from_in_protocol(&mut TCompactInputProtocol::new(&mut b)).expect("params");
        list.params_list.expect("params_list").into_iter().next().expect("a fragment")
    }
}

pub fn group_commit_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("group_commit")
}

/// Splits an HTTP message into its head and body.
fn split_http(raw: &[u8]) -> (String, Vec<u8>) {
    let at = raw.windows(4).position(|w| w == b"\r\n\r\n").map_or(raw.len(), |i| i + 4);
    (String::from_utf8_lossy(&raw[..at]).into_owned(), raw[at..].to_vec())
}

/// An HTTP reply's body, past any `100 Continue`, gunzipped when the reply says so.
fn response_body(raw: &[u8]) -> String {
    let mut rest = raw;
    loop {
        let (head, body) = split_http(rest);
        if head.starts_with("HTTP/1.1 100") {
            rest = &rest[head.len()..];
            continue;
        }
        let body = if head.to_ascii_lowercase().contains("content-encoding: gzip") {
            let mut out = Vec::new();
            std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(body.as_slice()), &mut out).expect("gzip body");
            out
        } else {
            body
        };
        return String::from_utf8_lossy(&body).into_owned();
    }
}

/// Every recorded message of a suite, in capture order.
pub fn sequence(suite: &str) -> Vec<Msg> {
    let dir = group_commit_dir().join(suite);
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    files.sort();
    files
        .iter()
        .map(|p| {
            let b = std::fs::read(p).expect("fixture message");
            let name = p.file_stem().unwrap().to_string_lossy().into_owned();
            let kind = name.split_once('-').map(|(_, k)| k).unwrap_or_default();
            match kind {
                "create" => Msg::Create(decode(&b, BackendServiceSubmitTasksArgs::read_from_in_protocol).tasks),
                "exec" => Msg::Exec(PExecPlanFragmentRequest::decode(b.as_slice()).expect("exec")),
                "cancel" => Msg::Cancel(PCancelPlanFragmentRequest::decode(b.as_slice()).expect("cancel")),
                "put" => Msg::Put(decode(&b, FrontendServiceStreamLoadPutArgs::read_from_in_protocol).request),
                "put_reply" => Msg::PutReply(
                    decode(&b, FrontendServiceStreamLoadPutResult::read_from_in_protocol)
                        .result_value
                        .expect("put result"),
                ),
                "report" => Msg::Report(decode(&b, FrontendServiceReportExecStatusArgs::read_from_in_protocol).params),
                "commit" => Msg::Commit(decode(&b, FrontendServiceLoadTxnCommitArgs::read_from_in_protocol).request),
                "rollback" => Msg::Rollback(decode(&b, FrontendServiceLoadTxnRollbackArgs::read_from_in_protocol).request),
                "http_request" => {
                    let (head, body) = split_http(&b);
                    let headers = head
                        .lines()
                        .skip(1)
                        .filter_map(|l| l.split_once(':'))
                        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                        .collect();
                    Msg::HttpRequest { headers, body }
                }
                "http_response" => Msg::HttpResponse(response_body(&b)),
                other => panic!("{}: unknown kind {other}", p.display()),
            }
        })
        .collect()
}
