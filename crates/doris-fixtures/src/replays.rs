//! Query replays sampled from the traffic corpus by `crates/doris-wire/src/bin/extract-replays.rs`
//! (`crates/doris-fixtures/replays`): for each sampled suite, every message the reference C++ BE
//! was sent, in capture order, with its reply, plus the FE calls it made and the FE's answers.
//!
//! On disk, `replays/index.json` lists the suite directories; each `replays/<suite>/index.json`
//! lists `{service, method, file, reply}` per request, `reply` being the file of its answer (or
//! null when the capture holds none), and every file is one raw captured message.
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;

use doris_proto::doris::{
    PCancelPlanFragmentRequest, PExecPlanFragmentRequest, PExecPlanFragmentStartRequest,
    PFetchDataRequest, PFetchDataResult,
};
use doris_thrift::gen::agent_service::{TAgentTaskRequest, TCreateTabletReq, TTabletSchema};
use doris_thrift::gen::backend_service::BackendServiceSubmitTasksArgs;
use doris_thrift::gen::data_sinks::TDataSinkType;
use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
use doris_thrift::gen::plan_nodes::{TPlanNode, TPlanNodeType};
use doris_thrift::gen::types::{TKeysType, TStorageType};
use prost::Message;
use serde::Deserialize;
use thrift::protocol::{TCompactInputProtocol, TSerializable};

use crate::decode;

/// The FE's statistics table: the FE reads it in most suites, and no suite creates it.
pub const STATS_TABLE: &str = "column_statistics";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rpc {
    Exec,
    Prepare,
}

/// One message the FE sent to the BE.
#[derive(Debug, Clone)]
pub enum Step {
    SubmitTasks(Vec<TAgentTaskRequest>),
    ExecPlanFragment {
        rpc: Rpc,
        request: PExecPlanFragmentRequest,
    },
    Start(PExecPlanFragmentStartRequest),
    /// A fetch the reference answered (unanswered long polls, dropped by the FE, are left out).
    Fetch {
        request: PFetchDataRequest,
        reply: PFetchDataResult,
    },
    Cancel(PCancelPlanFragmentRequest),
    UpdateCache,
    FetchCache,
}

#[derive(Debug, Clone, Deserialize)]
struct Entry {
    service: String,
    method: String,
    file: String,
    reply: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Replay {
    pub suite: String,
    pub steps: Vec<Step>,
    fe: Vec<(String, Vec<u8>, Vec<u8>)>,
}

/// The FE calls the reference BE made, and the FE's replies: Thrift binary messages, header included.
#[derive(Debug, Clone, Default)]
pub struct FeReplies {
    exact: HashMap<(String, Vec<u8>), VecDeque<Vec<u8>>>,
    in_order: HashMap<String, VecDeque<Vec<u8>>>,
}

/// A Thrift binary message without its header: the arguments struct, which does not depend on the
/// caller's sequence id.
fn body(message: &[u8]) -> Vec<u8> {
    // strict header: i32 version|type, i32 name length, name, i32 seqid
    if message.len() < 8 {
        return message.to_vec();
    }
    let name_len = i32::from_be_bytes(message[4..8].try_into().unwrap()).max(0) as usize;
    message.get(8 + name_len + 4..).unwrap_or_default().to_vec()
}

impl FeReplies {
    /// The FE's reply to a call (a whole Thrift message): the recorded reply to the same
    /// arguments when there is one, else the method's next recorded reply.
    pub fn reply(&mut self, method: &str, call: &[u8]) -> Option<Vec<u8>> {
        let key = (method.to_string(), body(call));
        if let Some(r) = self.exact.get_mut(&key).and_then(|q| q.pop_front()) {
            return Some(r);
        }
        self.in_order.get_mut(method).and_then(|q| q.pop_front())
    }

    pub fn methods(&self) -> BTreeSet<String> {
        self.in_order.keys().cloned().collect()
    }
}

pub fn replays_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("replays")
}

/// Every replay, in index order.
pub fn all() -> Vec<Replay> {
    let index: Vec<String> = serde_json::from_str(
        &std::fs::read_to_string(replays_dir().join("index.json")).expect("replays/index.json"),
    )
    .expect("index");
    index.iter().map(|s| load(s)).collect()
}

pub fn load(suite: &str) -> Replay {
    let dir = replays_dir().join(suite);
    let entries: Vec<Entry> = serde_json::from_str(
        &std::fs::read_to_string(dir.join("index.json")).unwrap_or_else(|e| panic!("{suite}: {e}")),
    )
    .expect("suite index");
    let read = |f: &str| std::fs::read(dir.join(f)).unwrap_or_else(|e| panic!("{suite}/{f}: {e}"));
    let mut steps = Vec::new();
    let mut fe = Vec::new();
    for e in &entries {
        if e.service == "FrontendService" {
            if let Some(r) = &e.reply {
                fe.push((e.method.clone(), read(&e.file), read(r)));
            }
            continue;
        }
        let bytes = read(&e.file);
        let step = match e.method.as_str() {
            "submit_tasks" => Step::SubmitTasks(
                decode(&bytes, BackendServiceSubmitTasksArgs::read_from_in_protocol).tasks,
            ),
            "exec_plan_fragment" | "exec_plan_fragment_prepare" => Step::ExecPlanFragment {
                rpc: if e.method == "exec_plan_fragment" {
                    Rpc::Exec
                } else {
                    Rpc::Prepare
                },
                request: PExecPlanFragmentRequest::decode(bytes.as_slice()).expect("exec request"),
            },
            "exec_plan_fragment_start" => {
                Step::Start(PExecPlanFragmentStartRequest::decode(bytes.as_slice()).expect("start"))
            }
            "fetch_data" => {
                let Some(r) = &e.reply else { continue };
                Step::Fetch {
                    request: PFetchDataRequest::decode(bytes.as_slice()).expect("fetch"),
                    reply: PFetchDataResult::decode(read(r).as_slice()).expect("fetch reply"),
                }
            }
            "cancel_plan_fragment" => {
                Step::Cancel(PCancelPlanFragmentRequest::decode(bytes.as_slice()).expect("cancel"))
            }
            "update_cache" => Step::UpdateCache,
            "fetch_cache" => Step::FetchCache,
            other => panic!("{suite}: unexpected BE message {other}"),
        };
        steps.push(step);
    }
    Replay {
        suite: suite.to_string(),
        steps,
        fe,
    }
}

/// The fragments of an exec request.
pub fn plan_list(req: &PExecPlanFragmentRequest) -> Option<TPipelineFragmentParamsList> {
    let mut b = req.request.as_deref()?;
    TPipelineFragmentParamsList::read_from_in_protocol(&mut TCompactInputProtocol::new(&mut b)).ok()
}

/// Whether a plan returns rows to the client.
pub fn is_result_query(list: &TPipelineFragmentParamsList) -> bool {
    list.params_list.iter().flatten().any(|p| {
        p.fragment
            .as_ref()
            .and_then(|f| f.output_sink.as_ref())
            .is_some_and(|s| s.type_ == TDataSinkType::RESULT_SINK)
    })
}

impl Replay {
    /// Every plan the replay executes, in order.
    pub fn queries(&self) -> Vec<TPipelineFragmentParamsList> {
        self.steps
            .iter()
            .filter_map(|s| match s {
                Step::ExecPlanFragment { request, .. } => plan_list(request),
                _ => None,
            })
            .collect()
    }

    /// The plans that return rows, as their shapes.
    pub fn result_queries(&self) -> Vec<String> {
        self.queries()
            .iter()
            .filter(|q| is_result_query(q))
            .map(shape)
            .collect()
    }

    pub fn fe_replies(&self) -> FeReplies {
        let mut f = FeReplies::default();
        for (method, call, reply) in &self.fe {
            f.exact
                .entry((method.clone(), body(call)))
                .or_default()
                .push_back(reply.clone());
            f.in_order
                .entry(method.clone())
                .or_default()
                .push_back(reply.clone());
        }
        f
    }

    /// A CREATE for each tablet a scan reads that the replay never creates (the FE's statistics
    /// table), at the highest version scanned, from the scan node's columns and keys type.
    pub fn synthetic_tablets(&self) -> Vec<TCreateTabletReq> {
        let created: BTreeSet<i64> = self
            .steps
            .iter()
            .filter_map(|s| match s {
                Step::SubmitTasks(t) => Some(
                    t.iter()
                        .filter_map(|t| t.create_tablet_req.as_ref().map(|c| c.tablet_id)),
                ),
                _ => None,
            })
            .flatten()
            .collect();
        let mut out: Vec<TCreateTabletReq> = Vec::new();
        for q in self.queries() {
            for p in q.params_list.iter().flatten() {
                let Some(nodes) = p
                    .fragment
                    .as_ref()
                    .and_then(|f| f.plan.as_ref())
                    .map(|pl| &pl.nodes)
                else {
                    continue;
                };
                for (node_id, tablet, version) in scan_ranges(p) {
                    if created.contains(&tablet) {
                        continue;
                    }
                    let Some(scan) = nodes
                        .iter()
                        .find(|n| n.node_id == node_id)
                        .and_then(|n| n.olap_scan_node.as_ref())
                    else {
                        continue;
                    };
                    if let Some(old) = out.iter_mut().find(|c| c.tablet_id == tablet) {
                        old.version = old.version.max(Some(version));
                        continue;
                    }
                    let columns = scan.columns_desc.clone().unwrap_or_default();
                    let keys = columns.iter().filter(|c| c.is_key == Some(true)).count();
                    let schema = TTabletSchema::new(
                        keys.min(3) as i16,
                        0,
                        scan.key_type.unwrap_or(TKeysType::DUP_KEYS),
                        TStorageType::COLUMN,
                        columns,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    );
                    out.push(TCreateTabletReq::new(
                        tablet,
                        schema,
                        Some(version),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        scan.enable_unique_key_merge_on_write,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    ));
                }
            }
        }
        out
    }
}

/// `(scan node id, tablet id, version)` for every OLAP scan range of a fragment's instances.
pub fn scan_ranges(
    p: &doris_thrift::gen::palo_internal_service::TPipelineFragmentParams,
) -> Vec<(i32, i64, i64)> {
    let mut out = Vec::new();
    for l in p.local_params.iter().flatten() {
        for (node, ranges) in l.per_node_scan_ranges.iter() {
            for r in ranges {
                if let Some(pr) = &r.scan_range.palo_scan_range {
                    out.push((*node, pr.tablet_id, pr.version.parse().unwrap_or(0)));
                }
            }
        }
    }
    out
}

fn node_name(t: TPlanNodeType) -> String {
    let name = match t {
        TPlanNodeType::OLAP_SCAN_NODE => "OLAP_SCAN",
        TPlanNodeType::EXCHANGE_NODE => "EXCHANGE",
        TPlanNodeType::SORT_NODE => "SORT",
        TPlanNodeType::AGGREGATION_NODE => "AGG",
        TPlanNodeType::UNION_NODE => "UNION",
        TPlanNodeType::SCHEMA_SCAN_NODE => "SCHEMA_SCAN",
        TPlanNodeType::EMPTY_SET_NODE => "EMPTY_SET",
        TPlanNodeType::MATERIALIZATION_NODE => "MATERIALIZATION",
        TPlanNodeType::SELECT_NODE => "SELECT",
        TPlanNodeType::HASH_JOIN_NODE => "HASH_JOIN",
        TPlanNodeType::CROSS_JOIN_NODE => "CROSS_JOIN",
        _ => return format!("{t:?}"),
    };
    name.to_string()
}

/// Renders the pre-order node list as a tree: `SORT(OLAP_SCAN)`.
fn tree(nodes: &[TPlanNode], at: &mut usize) -> String {
    let n = &nodes[*at];
    *at += 1;
    let mut s = node_name(n.node_type);
    if n.num_children > 0 {
        let kids: Vec<String> = (0..n.num_children)
            .map(|_| {
                if *at < nodes.len() {
                    tree(nodes, at)
                } else {
                    "?".into()
                }
            })
            .collect();
        s += &format!("({})", kids.join(","));
    }
    s
}

/// A plan's shape: fragments in fragment-id order, each `tree->sink`, joined by ` | `
/// (research note [wire] §6).
pub fn shape(list: &TPipelineFragmentParamsList) -> String {
    let mut frags: Vec<_> = list
        .params_list
        .iter()
        .flatten()
        .filter_map(|p| Some((p.fragment_id.unwrap_or(0), p.fragment.as_ref()?)))
        .collect();
    frags.sort_by_key(|(id, _)| *id);
    frags
        .iter()
        .map(|(_, f)| {
            let nodes = f.plan.as_ref().map(|p| p.nodes.as_slice()).unwrap_or_default();
            let t = if nodes.is_empty() { String::new() } else { tree(nodes, &mut 0) };
            let sink = match f.output_sink.as_ref() {
                Some(s) if s.type_ == TDataSinkType::RESULT_SINK => "RESULT".to_string(),
                Some(s) if s.type_ == TDataSinkType::DATA_STREAM_SINK => {
                    let ds = s.stream_sink.as_ref();
                    if ds.and_then(|d| d.is_merge) == Some(true) {
                        "DS(merge)".to_string()
                    } else {
                        match ds.map(|d| d.output_partition.type_) {
                            Some(t) if t == doris_thrift::gen::partitions::TPartitionType::UNPARTITIONED => "DS".to_string(),
                            Some(t) if t == doris_thrift::gen::partitions::TPartitionType::HASH_PARTITIONED => "DS(HASH)".to_string(),
                            Some(t) => format!("DS({t:?})"),
                            None => "DS".to_string(),
                        }
                    }
                }
                Some(s) => format!("{:?}", s.type_),
                None => "NONE".to_string(),
            };
            format!("{t}->{sink}")
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

/// The letter of a shape in [wire] §6, if it is one of them.
pub fn shape_letter(shape: &str) -> Option<char> {
    Some(match shape {
        "OLAP_SCAN->RESULT" => 'A',
        "SORT(OLAP_SCAN)->DS(merge) | EXCHANGE->RESULT" => 'B',
        "SORT(OLAP_SCAN)->DS(merge) | MATERIALIZATION(EXCHANGE)->RESULT" => 'C',
        "AGG(OLAP_SCAN)->DS | AGG(EXCHANGE)->RESULT" => 'D',
        "UNION->RESULT" => 'E',
        "SCHEMA_SCAN->RESULT" => 'F',
        "EMPTY_SET->RESULT" => 'H',
        "AGG(OLAP_SCAN)->DS(HASH) | SORT(AGG(EXCHANGE))->DS(merge) | EXCHANGE->RESULT" => 'I',
        "SORT(AGG(OLAP_SCAN))->DS(merge) | EXCHANGE->RESULT" => 'J',
        "AGG(EMPTY_SET)->RESULT" => 'K',
        _ => return None,
    })
}

/// The function names a plan calls, lowercased.
pub fn functions(list: &TPipelineFragmentParamsList) -> BTreeSet<String> {
    let debug = format!("{list:?}");
    let key = "function_name: \"";
    let mut out = BTreeSet::new();
    let mut rest = debug.as_str();
    while let Some(at) = rest.find(key) {
        rest = &rest[at + key.len()..];
        let end = rest.find('"').unwrap_or(0);
        out.insert(rest[..end].to_lowercase());
    }
    out
}
