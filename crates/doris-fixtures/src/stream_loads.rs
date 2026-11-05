//! Stream loads sampled from the traffic corpus by
//! `crates/doris-wire/src/bin/extract-stream-loads.rs` (`crates/doris-fixtures/stream_loads`):
//! the HTTP request and reply the reference C++ BE exchanged with the client, and the FE calls
//! it made for the load (loadTxnBegin, streamLoadPut, loadTxnCommit or loadTxnRollback), with
//! the FE's replies.
use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;

use doris_thrift::gen::frontend_service::{
    FrontendServiceLoadTxnBeginArgs, FrontendServiceLoadTxnBeginResult, FrontendServiceLoadTxnCommitArgs,
    FrontendServiceLoadTxnCommitResult, FrontendServiceLoadTxnRollbackArgs, FrontendServiceLoadTxnRollbackResult,
    FrontendServiceStreamLoadPutArgs, FrontendServiceStreamLoadPutResult, TLoadTxnBeginRequest, TLoadTxnBeginResult,
    TLoadTxnCommitRequest, TLoadTxnCommitResult, TLoadTxnRollbackRequest, TLoadTxnRollbackResult, TStreamLoadPutRequest,
    TStreamLoadPutResult,
};
use serde::Deserialize;

use crate::decode;

#[derive(Debug, Deserialize)]
struct IndexEntry {
    dir: String,
    suite: String,
    end: String,
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)] // test fixtures
pub enum LoadEnd {
    Commit(TLoadTxnCommitRequest, TLoadTxnCommitResult),
    Rollback(TLoadTxnRollbackRequest, TLoadTxnRollbackResult),
}

#[derive(Debug, Clone)]
pub struct StreamLoadSample {
    pub dir: String,
    pub suite: String,
    /// `PUT /api/{db}/{table}/_stream_load`
    pub path: String,
    /// Header names lower-cased.
    pub headers: BTreeMap<String, String>,
    /// The body as the client sent it, de-chunked.
    pub body: Vec<u8>,
    /// The reply's JSON, gunzipped.
    pub reply: String,
    pub begin: (TLoadTxnBeginRequest, TLoadTxnBeginResult),
    pub put: (TStreamLoadPutRequest, TStreamLoadPutResult),
    pub end: LoadEnd,
}

pub fn stream_loads_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("stream_loads")
}

fn split_http(raw: &[u8]) -> (String, &[u8]) {
    let at = raw.windows(4).position(|w| w == b"\r\n\r\n").map_or(raw.len(), |i| i + 4);
    (String::from_utf8_lossy(&raw[..at]).into_owned(), &raw[at..])
}

fn dechunk(mut b: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(eol) = b.windows(2).position(|w| w == b"\r\n") {
        let size = usize::from_str_radix(String::from_utf8_lossy(&b[..eol]).trim(), 16).unwrap_or(0);
        if size == 0 {
            break;
        }
        b = &b[eol + 2..];
        out.extend_from_slice(&b[..size.min(b.len())]);
        b = &b[(size + 2).min(b.len())..];
    }
    out
}

/// Every sample, in index order.
pub fn stream_loads() -> Vec<StreamLoadSample> {
    let root = stream_loads_dir();
    let index: Vec<IndexEntry> = serde_json::from_str(&std::fs::read_to_string(root.join("index.json")).expect("index.json")).expect("index");
    index
        .into_iter()
        .map(|e| {
            let read = |f: &str| std::fs::read(root.join(&e.dir).join(f)).unwrap_or_else(|err| panic!("{}/{f}: {err}", e.dir));
            let request = read("http_request.bin");
            let (head, body) = split_http(&request);
            let mut lines = head.lines();
            let path = lines.next().unwrap_or_default().split(' ').nth(1).unwrap_or_default().to_string();
            let headers: BTreeMap<String, String> =
                lines.filter_map(|l| l.split_once(':')).map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string())).collect();
            let body = if headers.get("transfer-encoding").is_some_and(|t| t.contains("chunked")) { dechunk(body) } else { body.to_vec() };
            // the reply follows "HTTP/1.1 100 Continue"
            let response = read("http_response.bin");
            let text_at = response.windows(15).rposition(|w| w == b"HTTP/1.1 200 OK").unwrap_or(0);
            let (rhead, rbody) = split_http(&response[text_at..]);
            let reply = if rhead.to_ascii_lowercase().contains("content-encoding: gzip") {
                let mut b = Vec::new();
                flate2::read::GzDecoder::new(rbody).read_to_end(&mut b).expect("gzip reply");
                String::from_utf8_lossy(&b).into_owned()
            } else {
                String::from_utf8_lossy(rbody).into_owned()
            };
            let begin = (
                decode(&read("begin_call.bin"), FrontendServiceLoadTxnBeginArgs::read_from_in_protocol).request,
                decode(&read("begin_reply.bin"), FrontendServiceLoadTxnBeginResult::read_from_in_protocol).result_value.expect("begin result"),
            );
            let put = (
                decode(&read("put_call.bin"), FrontendServiceStreamLoadPutArgs::read_from_in_protocol).request,
                decode(&read("put_reply.bin"), FrontendServiceStreamLoadPutResult::read_from_in_protocol).result_value.expect("put result"),
            );
            let end_call = read("end_call.bin");
            let end = if e.end == "commit" {
                let a = decode(&end_call, FrontendServiceLoadTxnCommitArgs::read_from_in_protocol);
                LoadEnd::Commit(a.request, decode(&read("end_reply.bin"), FrontendServiceLoadTxnCommitResult::read_from_in_protocol).result_value.expect("commit result"))
            } else {
                LoadEnd::Rollback(
                    decode(&end_call, FrontendServiceLoadTxnRollbackArgs::read_from_in_protocol).request,
                    decode(&read("end_reply.bin"), FrontendServiceLoadTxnRollbackResult::read_from_in_protocol).result_value.expect("rollback result"),
                )
            };
            StreamLoadSample { dir: e.dir, suite: e.suite, path, headers, body, reply, begin, put, end }
        })
        .collect()
}
