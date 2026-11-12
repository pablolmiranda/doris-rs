//! `extract-group-commit OUT_DIR SUITE...`: the group-commit messages of the named suites, from
//! the traffic corpus (`oracle/epic1/traffic`), for the batch 4 tests. For each suite,
//! `<OUT>/<suite>/` holds, in capture order, `<nnn>-<kind>.bin` with the raw message:
//!   `create`     a `submit_tasks` call holding CREATE tasks
//!   `exec`       an `exec_plan_fragment` request whose sink is GROUP_COMMIT_BLOCK_SINK
//!   `cancel`     a `cancel_plan_fragment` request for one of those
//!   `put`/`put_reply`        `streamLoadPut`
//!   `report`     a `reportExecStatus` call of one of those fragments
//!   `commit`/`rollback`      `loadTxnCommit` with `group_commit` set, `loadTxnRollback`
//!   `http_request`/`http_response`   a stream load with a `group_commit` header
use std::collections::BTreeSet;
use std::path::PathBuf;

use doris_proto::doris::{PCancelPlanFragmentRequest, PExecPlanFragmentRequest};
use doris_thrift::gen::backend_service::BackendServiceSubmitTasksArgs;
use doris_thrift::gen::data_sinks::TDataSinkType;
use doris_thrift::gen::frontend_service::{FrontendServiceLoadTxnCommitArgs, FrontendServiceReportExecStatusArgs};
use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
use doris_thrift::gen::types::TTaskType;
use doris_wire::suites::{corpus_dir, read_suite, SuiteRecord};
use prost::Message;
use thrift::protocol::{TBinaryInputProtocol, TCompactInputProtocol, TInputProtocol, TSerializable};

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

/// The query id of a group-commit fragment request.
fn group_commit_exec(r: &SuiteRecord) -> Option<(i64, i64)> {
    let req = PExecPlanFragmentRequest::decode(&r.data[..]).ok()?;
    let mut b = req.request.as_deref()?;
    let list = if req.compact.unwrap_or(false) {
        TPipelineFragmentParamsList::read_from_in_protocol(&mut TCompactInputProtocol::new(&mut b)).ok()?
    } else {
        TPipelineFragmentParamsList::read_from_in_protocol(&mut TBinaryInputProtocol::new(&mut b, true)).ok()?
    };
    let [p] = list.params_list.as_deref()? else { return None };
    let sink = p.fragment.as_ref()?.output_sink.as_ref()?;
    (sink.type_ == TDataSinkType::GROUP_COMMIT_BLOCK_SINK).then_some((p.query_id.hi, p.query_id.lo))
}

fn kind(r: &SuiteRecord, queries: &BTreeSet<(i64, i64)>, http_streams: &mut BTreeSet<i64>) -> Option<&'static str> {
    let (protocol, method, k) = (field(r, "protocol"), field(r, "method"), field(r, "kind"));
    match (protocol.as_str(), method.as_str(), k.as_str()) {
        ("grpc", "exec_plan_fragment", "request") => group_commit_exec(r).map(|_| "exec"),
        ("grpc", "cancel_plan_fragment", "request") => {
            let c = PCancelPlanFragmentRequest::decode(&r.data[..]).ok()?;
            let q = c.query_id.map(|q| (q.hi, q.lo))?;
            queries.contains(&q).then_some("cancel")
        }
        ("thrift", "submit_tasks", "call") => {
            let a = binary!(BackendServiceSubmitTasksArgs, &r.data)?;
            a.tasks.iter().any(|t| t.task_type == TTaskType::CREATE).then_some("create")
        }
        ("thrift", "streamLoadPut", "call") => Some("put"),
        ("thrift", "streamLoadPut", "reply") => Some("put_reply"),
        ("thrift", "reportExecStatus", "call") => {
            let a = binary!(FrontendServiceReportExecStatusArgs, &r.data)?;
            let q = a.params.query_id.map(|q| (q.hi, q.lo))?;
            queries.contains(&q).then_some("report")
        }
        ("thrift", "loadTxnCommit", "call") => {
            let a = binary!(FrontendServiceLoadTxnCommitArgs, &r.data)?;
            (a.request.group_commit == Some(true)).then_some("commit")
        }
        ("thrift", "loadTxnRollback", "call") => Some("rollback"),
        ("http", _, "request") if field(r, "path").ends_with("/_stream_load") => {
            let text = String::from_utf8_lossy(&r.data);
            let head = text.split("\r\n\r\n").next().unwrap_or_default().to_ascii_lowercase();
            head.lines().any(|l| l.starts_with("group_commit:")).then(|| {
                http_streams.insert(stream(r));
                "http_request"
            })
        }
        ("http", _, "response") if http_streams.remove(&stream(r)) => Some("http_response"),
        _ => None,
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let out = PathBuf::from(args.next().expect("usage: extract-group-commit OUT_DIR SUITE..."));
    for suite in args {
        let recs = read_suite(&corpus_dir().join(format!("{suite}.bin.gz"))).expect("read suite");
        let dir = out.join(&suite);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        // records of several connections interleave: a report may come before its fragment
        let queries: BTreeSet<(i64, i64)> = recs
            .iter()
            .filter(|r| field(r, "method") == "exec_plan_fragment")
            .filter_map(group_commit_exec)
            .collect();
        let mut http = BTreeSet::new();
        let mut n = 0;
        for r in &recs {
            if let Some(k) = kind(r, &queries, &mut http) {
                std::fs::write(dir.join(format!("{n:03}-{k}.bin")), &r.data).expect("write");
                n += 1;
            }
        }
        eprintln!("{suite}: {n} messages, {} group-commit fragments", queries.len());
    }
}
