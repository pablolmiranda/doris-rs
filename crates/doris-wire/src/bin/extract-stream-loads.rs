//! `extract-stream-loads OUT_DIR [N]`: samples stream loads from the traffic corpus
//! (`oracle/epic1/traffic`) for the M4 tests. A sample is one `PUT …/_stream_load` the reference
//! BE served without group commit, 2PC or partial update, in CSV or JSON, not into the FE's
//! own `__internal_schema` tables, with the FE calls it made for it, matched by
//! label and transaction id. Each record is saved raw:
//!   `<OUT>/<nn>/http_request.bin`, `http_response.bin`   the HTTP exchange as captured
//!   `<OUT>/<nn>/begin_call.bin`, `begin_reply.bin`       loadTxnBegin
//!   `<OUT>/<nn>/put_call.bin`, `put_reply.bin`           streamLoadPut
//!   `<OUT>/<nn>/end_call.bin`, `end_reply.bin`           loadTxnCommit or loadTxnRollback
//! and `<OUT>/index.json` lists `{dir, suite, end, format}`. Rolled-back loads come first, then
//! up to 10 JSON loads, then CSV loads, at most 3 per suite, N in all (default 60).
use std::collections::BTreeMap;
use std::path::PathBuf;

use doris_thrift::gen::frontend_service::{
    FrontendServiceLoadTxnBeginArgs, FrontendServiceLoadTxnBeginResult, FrontendServiceLoadTxnCommitArgs,
    FrontendServiceLoadTxnRollbackArgs, FrontendServiceStreamLoadPutArgs,
};
use doris_wire::suites::{corpus_dir, read_suite, suite_files, SuiteRecord};
use thrift::protocol::{TBinaryInputProtocol, TInputProtocol};

fn field(r: &SuiteRecord, k: &str) -> String {
    r.header.get(k).and_then(|v| v.as_str()).unwrap_or_default().to_string()
}

fn stream(r: &SuiteRecord) -> i64 {
    r.header.get("tcp_stream").and_then(|v| v.as_i64()).unwrap_or(-1)
}

macro_rules! binary {
    ($t:ty, $data:expr) => {{
        let mut b: &[u8] = $data;
        let mut i = TBinaryInputProtocol::new(&mut b, true);
        i.read_message_begin().ok().and_then(|_| <$t>::read_from_in_protocol(&mut i).ok())
    }};
}

/// The HTTP request's headers, lower-cased names.
fn headers(raw: &[u8]) -> BTreeMap<String, String> {
    let text = String::from_utf8_lossy(raw);
    let head = text.split("\r\n\r\n").next().unwrap_or_default();
    head.lines()
        .skip(1)
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect()
}

struct Sample {
    suite: String,
    end: &'static str,
    format: String,
    records: Vec<(&'static str, Vec<u8>)>,
}

fn main() {
    let mut args = std::env::args().skip(1);
    let out = PathBuf::from(args.next().expect("usage: extract-stream-loads OUT_DIR [N]"));
    let want: usize = args.next().map(|n| n.parse().expect("N")).unwrap_or(60);
    let mut found: Vec<Sample> = Vec::new();
    for path in suite_files(&corpus_dir()) {
        let suite = path.file_name().unwrap().to_string_lossy().trim_end_matches(".bin.gz").to_string();
        let recs = read_suite(&path).expect("read suite");
        for (i, r) in recs.iter().enumerate() {
            if field(r, "protocol") != "http" || field(r, "kind") != "request" || !field(r, "path").ends_with("/_stream_load") {
                continue;
            }
            let h = headers(&r.data);
            let format = h.get("format").cloned().unwrap_or_else(|| "csv".into()).to_ascii_lowercase();
            // outside M4: group commit, 2PC, partial update, and the FE's own audit log loads
            let internal = field(r, "path").contains("/__internal_schema/") || h.contains_key("skip_record_to_audit_log_table");
            if h.contains_key("group_commit")
                || !matches!(format.as_str(), "csv" | "json")
                || h.contains_key("two_phase_commit")
                || h.contains_key("partial_columns")
                || internal
            {
                continue;
            }
            let Some(label) = h.get("label") else { continue };
            let response = recs[i + 1..].iter().find(|x| field(x, "protocol") == "http" && field(x, "kind") == "response" && stream(x) == stream(r));
            // loadTxnBegin with this label, and its reply on the same connection
            let begin = recs.iter().position(|x| {
                field(x, "method") == "loadTxnBegin"
                    && field(x, "kind") == "call"
                    && binary!(FrontendServiceLoadTxnBeginArgs, &x.data).is_some_and(|a| a.request.label == *label)
            });
            let Some(begin) = begin else { continue };
            let reply_of = |call: usize| recs[call + 1..].iter().find(|x| field(x, "kind") == "reply" && stream(x) == stream(&recs[call]) && field(x, "method") == field(&recs[call], "method"));
            let Some(begin_reply) = reply_of(begin) else { continue };
            let Some(txn) = binary!(FrontendServiceLoadTxnBeginResult, &begin_reply.data).and_then(|r| r.result_value).and_then(|r| r.txn_id) else {
                continue;
            };
            let put = recs.iter().position(|x| {
                field(x, "method") == "streamLoadPut"
                    && field(x, "kind") == "call"
                    && binary!(FrontendServiceStreamLoadPutArgs, &x.data).is_some_and(|a| a.request.txn_id == txn)
            });
            let commit = recs.iter().position(|x| {
                field(x, "method") == "loadTxnCommit"
                    && field(x, "kind") == "call"
                    && binary!(FrontendServiceLoadTxnCommitArgs, &x.data).is_some_and(|a| a.request.txn_id == txn)
            });
            let rollback = recs.iter().position(|x| {
                field(x, "method") == "loadTxnRollback"
                    && field(x, "kind") == "call"
                    && binary!(FrontendServiceLoadTxnRollbackArgs, &x.data).is_some_and(|a| a.request.txn_id == txn)
            });
            let (Some(put), Some(response)) = (put, response) else { continue };
            let (end, end_name) = match (commit, rollback) {
                (Some(c), None) => (c, "commit"),
                (None, Some(r)) => (r, "rollback"),
                _ => continue,
            };
            let (Some(put_reply), Some(end_reply)) = (reply_of(put), reply_of(end)) else { continue };
            found.push(Sample {
                suite: suite.clone(),
                end: end_name,
                format,
                records: vec![
                    ("http_request.bin", r.data.clone()),
                    ("http_response.bin", response.data.clone()),
                    ("begin_call.bin", recs[begin].data.clone()),
                    ("begin_reply.bin", begin_reply.data.clone()),
                    ("put_call.bin", recs[put].data.clone()),
                    ("put_reply.bin", put_reply.data.clone()),
                    ("end_call.bin", recs[end].data.clone()),
                    ("end_reply.bin", end_reply.data.clone()),
                ],
            });
        }
    }
    let mut picked: Vec<usize> = Vec::new();
    let mut per_suite: BTreeMap<String, usize> = BTreeMap::new();
    // rolled-back loads, then up to 10 JSON loads, then CSV loads (94% of the corpus's)
    for pass in 0..3 {
        for (i, s) in found.iter().enumerate() {
            let json_taken = picked.iter().filter(|j| found[**j].format == "json").count();
            let take = match pass {
                0 => s.end == "rollback",
                1 => s.format == "json" && json_taken < 10,
                _ => s.format == "csv",
            };
            if !take || picked.len() >= want || picked.contains(&i) || per_suite.get(&s.suite).copied().unwrap_or(0) >= 3 {
                continue;
            }
            *per_suite.entry(s.suite.clone()).or_default() += 1;
            picked.push(i);
        }
    }
    let _ = std::fs::remove_dir_all(&out);
    let mut index = Vec::new();
    for (n, &i) in picked.iter().enumerate() {
        let s = &found[i];
        let dir = out.join(format!("{n:02}"));
        std::fs::create_dir_all(&dir).expect("mkdir");
        for (name, bytes) in &s.records {
            std::fs::write(dir.join(name), bytes).expect("write");
        }
        index.push(serde_json::json!({"dir": format!("{n:02}"), "suite": s.suite, "end": s.end, "format": s.format}));
    }
    std::fs::write(out.join("index.json"), serde_json::to_string_pretty(&index).unwrap() + "\n").expect("write index");
    let rollbacks = picked.iter().filter(|i| found[**i].end == "rollback").count();
    let json = picked.iter().filter(|i| found[**i].format == "json").count();
    eprintln!("{} stream loads in the corpus; kept {} ({rollbacks} rolled back, {json} JSON) from {} suites", found.len(), picked.len(), per_suite.len());
}
