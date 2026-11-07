//! `extract-values OUT_DIR [N]`: samples `INSERT … VALUES` statements from the traffic corpus
//! (`oracle/epic1/traffic`) for the M4 write-path tests. A statement is an `exec_plan_fragment`
//! request whose one fragment is UNION_NODEs into an OLAP_TABLE_SINK. For each, the raw captured
//! messages that belong to it are saved as they are, decoded by the tests:
//!   `<OUT>/<nn>/fragment.bin`  the PExecPlanFragmentRequest
//!   `<OUT>/<nn>/report.bin`    the final reportExecStatus call (Thrift binary message)
//!   `<OUT>/<nn>/publish.bin`   the submit_tasks call holding its PUBLISH_VERSION task
//!   `<OUT>/<nn>/finish.bin`    the finishTask call answering that task
//! and `<OUT>/index.json` lists `{dir, suite, kinds}`. Statements that bring a constant
//! expression kind not yet sampled come first, then the rest, at most 3 per suite, N in all
//! (default 60).
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use doris_proto::doris::PExecPlanFragmentRequest;
use doris_thrift::gen::backend_service::BackendServiceSubmitTasksArgs;
use doris_thrift::gen::data_sinks::TDataSinkType;
use doris_thrift::gen::frontend_service::{FrontendServiceFinishTaskArgs, FrontendServiceReportExecStatusArgs};
use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
use doris_thrift::gen::plan_nodes::TPlanNodeType;
use doris_thrift::gen::types::TTaskType;
use doris_wire::suites::{corpus_dir, read_suite, suite_files, SuiteRecord};
use prost::Message;
use thrift::protocol::{TBinaryInputProtocol, TCompactInputProtocol, TInputProtocol, TSerializable};

fn field(r: &SuiteRecord, k: &str) -> String {
    r.header.get(k).and_then(|v| v.as_str()).unwrap_or_default().to_string()
}

/// A Thrift binary call's arguments (the generated `*Args` types are not `TSerializable`).
macro_rules! binary {
    ($t:ty, $data:expr) => {{
        let mut b: &[u8] = $data;
        let mut i = TBinaryInputProtocol::new(&mut b, true);
        i.read_message_begin().ok().and_then(|_| <$t>::read_from_in_protocol(&mut i).ok())
    }};
}

/// A VALUES statement: its query id, txn id and the kinds of its constant expressions.
fn values_insert(r: &SuiteRecord) -> Option<((i64, i64), i64, BTreeSet<String>)> {
    if field(r, "protocol") != "grpc" || field(r, "kind") != "request" || field(r, "method") != "exec_plan_fragment" {
        return None;
    }
    let req = PExecPlanFragmentRequest::decode(&r.data[..]).ok()?;
    let mut b = req.request.as_deref()?;
    let list = TPipelineFragmentParamsList::read_from_in_protocol(&mut TCompactInputProtocol::new(&mut b)).ok()?;
    let [p] = list.params_list.as_deref()? else { return None };
    let fragment = p.fragment.as_ref()?;
    let sink = fragment.output_sink.as_ref()?;
    let nodes = &fragment.plan.as_ref()?.nodes;
    if sink.type_ != TDataSinkType::OLAP_TABLE_SINK || nodes.is_empty() || nodes.iter().any(|n| n.node_type != TPlanNodeType::UNION_NODE) {
        return None;
    }
    let txn = sink.olap_table_sink.as_ref()?.txn_id;
    let mut kinds = BTreeSet::new();
    for n in nodes {
        for row in n.union_node.as_ref()?.const_expr_lists.iter() {
            for e in row {
                if let Some(root) = e.nodes.first() {
                    let ty = root.type_.types.as_ref().and_then(|t| t.first()).and_then(|t| t.scalar_type.as_ref()).map(|s| format!("{:?}", s.type_));
                    kinds.insert(format!("{:?}/{}", root.node_type, ty.unwrap_or_default()));
                }
            }
        }
    }
    Some(((p.query_id.hi, p.query_id.lo), txn, kinds))
}

fn main() {
    let mut args = std::env::args().skip(1);
    let out = PathBuf::from(args.next().expect("usage: extract-values OUT_DIR [N]"));
    let want: usize = args.next().map(|n| n.parse().expect("N")).unwrap_or(60);

    // every complete statement: (suite, kinds, the four records)
    let mut found: Vec<(String, BTreeSet<String>, [Vec<u8>; 4])> = Vec::new();
    for path in suite_files(&corpus_dir()) {
        let suite = path.file_name().unwrap().to_string_lossy().trim_end_matches(".bin.gz").to_string();
        let records = read_suite(&path).expect("read suite");
        for (i, r) in records.iter().enumerate() {
            let Some((qid, txn, kinds)) = values_insert(r) else { continue };
            let later = &records[i + 1..];
            let report = later.iter().find(|x| {
                field(x, "method") == "reportExecStatus"
                    && field(x, "kind") == "call"
                    && binary!(FrontendServiceReportExecStatusArgs, &x.data).is_some_and(|a| {
                        a.params.done == Some(true) && a.params.query_id.as_ref().is_some_and(|q| (q.hi, q.lo) == qid)
                    })
            });
            let publish = later.iter().find(|x| {
                field(x, "method") == "submit_tasks"
                    && field(x, "kind") == "call"
                    && binary!(BackendServiceSubmitTasksArgs, &x.data).is_some_and(|a| {
                        a.tasks.iter().any(|t| {
                            t.task_type == TTaskType::PUBLISH_VERSION && t.publish_version_req.as_ref().is_some_and(|p| p.transaction_id == txn)
                        })
                    })
            });
            let finish = later.iter().find(|x| {
                field(x, "method") == "finishTask"
                    && field(x, "kind") == "call"
                    && binary!(FrontendServiceFinishTaskArgs, &x.data)
                        .is_some_and(|a| a.request.task_type == TTaskType::PUBLISH_VERSION && a.request.signature == txn)
            });
            if let (Some(rep), Some(pb), Some(fin)) = (report, publish, finish) {
                found.push((suite.clone(), kinds, [r.data.clone(), rep.data.clone(), pb.data.clone(), fin.data.clone()]));
            }
        }
    }

    // new kinds first, then the rest; at most 3 per suite
    let mut picked: Vec<usize> = Vec::new();
    let mut per_suite: BTreeMap<String, usize> = BTreeMap::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for pass in 0..2 {
        for (i, (suite, kinds, _)) in found.iter().enumerate() {
            if picked.len() >= want || picked.contains(&i) || per_suite.get(suite).copied().unwrap_or(0) >= 3 {
                continue;
            }
            if pass == 0 && kinds.is_subset(&seen) {
                continue;
            }
            seen.extend(kinds.iter().cloned());
            *per_suite.entry(suite.clone()).or_default() += 1;
            picked.push(i);
        }
    }

    let _ = std::fs::remove_dir_all(&out);
    let mut index = Vec::new();
    for (n, &i) in picked.iter().enumerate() {
        let (suite, kinds, recs) = &found[i];
        let dir = out.join(format!("{n:02}"));
        std::fs::create_dir_all(&dir).expect("mkdir");
        for (name, bytes) in ["fragment.bin", "report.bin", "publish.bin", "finish.bin"].iter().zip(recs) {
            std::fs::write(dir.join(name), bytes).expect("write");
        }
        index.push(serde_json::json!({"dir": format!("{n:02}"), "suite": suite, "kinds": kinds}));
    }
    std::fs::write(out.join("index.json"), serde_json::to_string_pretty(&index).unwrap() + "\n").expect("write index");
    eprintln!(
        "{} complete statements in the corpus; kept {} from {} suites, {} expression kinds",
        found.len(),
        picked.len(),
        per_suite.len(),
        seen.len()
    );
}
