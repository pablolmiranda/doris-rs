//! `extract-replays OUT_DIR [N]`: samples whole gate suites from the traffic corpus
//! (`oracle/epic1/traffic`) for the M5 read-path tests (`doris_fixtures::replays`).
//!
//! A suite is kept when:
//! - every message the FE sent the BE is a replay step (`submit_tasks`, `exec_plan_fragment`,
//!   `_prepare`, `_start`, `fetch_data`, `cancel_plan_fragment`, `update_cache`, `fetch_cache`);
//! - its needs fall within M4's supported features plus M5's (spec §8);
//! - every tablet it scans or writes is created in the suite, or belongs to the FE's statistics
//!   table (created synthetically by the replay);
//! - it holds between 1 and 40 result queries, and at least one fetch the reference ended with eos.
//!
//! `nereids_p0.system.test_query_sys_tables` comes first, cut after the eos fetch of its sixth
//! result query (session_variables). Then the smallest suite for each shape ([wire] §6) or M5
//! function not yet covered, then the smallest remaining suites, N in all (default 60).
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use doris_fixtures::replays::{
    functions, is_result_query, scan_ranges, shape, shape_letter, STATS_TABLE,
};
use doris_proto::doris::{PExecPlanFragmentRequest, PFetchDataRequest, PFetchDataResult};
use doris_thrift::gen::backend_service::BackendServiceSubmitTasksArgs;
use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
use doris_wire::needs::needs_of;
use doris_wire::suites::{corpus_dir, read_suite, suite_files, SuiteRecord};
use prost::Message;
use thrift::protocol::{
    TBinaryInputProtocol, TCompactInputProtocol, TInputProtocol, TSerializable,
};

const SYS_TABLES: &str = "nereids_p0.system.test_query_sys_tables.test_query_sys_tables";
const STEP_METHODS: &[&str] = &[
    "exec_plan_fragment",
    "exec_plan_fragment_prepare",
    "exec_plan_fragment_start",
    "fetch_data",
    "cancel_plan_fragment",
    "update_cache",
    "fetch_cache",
];
const FE_METHODS: &[&str] = &[
    "finishTask",
    "reportExecStatus",
    "getDbNames",
    "getTableNames",
    "describeTables",
    "listTableStatus",
    "showVariables",
];
const M5_FEATURES: &[&str] = &[
    "rpc:PBackendService.exec_plan_fragment_prepare",
    "rpc:PBackendService.exec_plan_fragment_start",
    "rpc:PBackendService.fetch_data",
    "rpc:PBackendService.update_cache",
    "rpc:PBackendService.fetch_cache",
    "rpc:FrontendService.getDbNames",
    "rpc:FrontendService.getTableNames",
    "rpc:FrontendService.describeTables",
    "rpc:FrontendService.listTableStatus",
    "rpc:FrontendService.showVariables",
    "plan_node:OLAP_SCAN_NODE",
    "plan_node:EXCHANGE_NODE",
    "plan_node:SORT_NODE",
    "plan_node:MATERIALIZATION_NODE",
    "plan_node:EMPTY_SET_NODE",
    "plan_node:SELECT_NODE",
    "plan_node:AGGREGATION_NODE",
    "plan_node:SCHEMA_SCAN_NODE",
    "sink:RESULT_SINK",
    "sink:DATA_STREAM_SINK",
    "expr:BINARY_PRED",
    "expr:COMPOUND_PRED",
    "expr:IN_PRED",
    "expr:AGG_EXPR",
    "expr:FUNCTION_CALL",
    "function:eq",
    "function:ne",
    "function:lt",
    "function:gt",
    "function:le",
    "function:ge",
    "function:is_null_pred",
    "function:is_not_null_pred",
    "function:substring",
    "function:count",
    "function:sum",
    "function:min",
    "function:max",
    "function:avg",
    "function:multi_distinct_count",
];
const M5_FUNCTIONS: &[&str] = &[
    "eq",
    "ne",
    "lt",
    "gt",
    "le",
    "ge",
    "is_null_pred",
    "is_not_null_pred",
    "substring",
    "count",
    "sum",
    "min",
    "max",
    "avg",
    "multi_distinct_count",
];

fn s<'a>(r: &'a SuiteRecord, k: &str) -> &'a str {
    r.header.get(k).and_then(|v| v.as_str()).unwrap_or_default()
}
fn n(r: &SuiteRecord, k: &str) -> i64 {
    r.header.get(k).and_then(|v| v.as_i64()).unwrap_or(-1)
}
fn is_request(r: &SuiteRecord) -> bool {
    matches!(s(r, "kind"), "request" | "call")
}

fn plan(r: &SuiteRecord) -> Option<TPipelineFragmentParamsList> {
    let req = PExecPlanFragmentRequest::decode(&r.data[..]).ok()?;
    let mut b = req.request.as_deref()?;
    TPipelineFragmentParamsList::read_from_in_protocol(&mut TCompactInputProtocol::new(&mut b)).ok()
}

/// For each request, the index of its reply: gRPC by (tcp stream, h2 stream), the response with the
/// smallest frame after the request's; Thrift by (tcp stream, seqid), the next reply in file order.
fn pair(records: &[SuiteRecord]) -> HashMap<usize, usize> {
    let mut out = HashMap::new();
    let mut used = BTreeSet::new();
    for (i, r) in records.iter().enumerate() {
        if !is_request(r) {
            continue;
        }
        let grpc = s(r, "protocol") == "grpc";
        let found = records
            .iter()
            .enumerate()
            .filter(|(j, x)| {
                !used.contains(j)
                    && !is_request(x)
                    && s(x, "method") == s(r, "method")
                    && n(x, "tcp_stream") == n(r, "tcp_stream")
                    && if grpc {
                        n(x, "h2_stream") == n(r, "h2_stream") && n(x, "frame") > n(r, "frame")
                    } else {
                        n(x, "seqid") == n(r, "seqid") && *j > i
                    }
            })
            .min_by_key(|(j, x)| if grpc { n(x, "frame") } else { *j as i64 })
            .map(|(j, _)| j);
        if let Some(j) = found {
            used.insert(j);
            out.insert(i, j);
        }
    }
    out
}

/// sys_tables: everything up to the eos fetch of the sixth result query.
fn cut_sys_tables(records: Vec<SuiteRecord>) -> Vec<SuiteRecord> {
    let pairs = pair(&records);
    let mut results = 0;
    let mut sixth = None;
    for (i, r) in records.iter().enumerate() {
        if s(r, "kind") == "request"
            && s(r, "method").starts_with("exec_plan_fragment")
            && s(r, "method") != "exec_plan_fragment_start"
        {
            if let Some(p) = plan(r).filter(is_result_query) {
                results += 1;
                if results == 6 {
                    sixth = p
                        .params_list
                        .as_ref()
                        .and_then(|l| l.first())
                        .map(|f| (f.query_id.hi, f.query_id.lo));
                }
            }
        }
        if let Some(q) = sixth {
            if s(r, "kind") == "request" && s(r, "method") == "fetch_data" {
                let ours = PFetchDataRequest::decode(&r.data[..])
                    .is_ok_and(|f| (f.finst_id.hi, f.finst_id.lo) == q);
                if let Some(&j) = pairs.get(&i).filter(|_| ours) {
                    if PFetchDataResult::decode(&records[j].data[..])
                        .is_ok_and(|x| x.eos == Some(true))
                    {
                        let end = i.max(j);
                        return records.into_iter().take(end + 1).collect();
                    }
                }
            }
        }
    }
    panic!("sys_tables: no eos for its sixth result query");
}

struct Candidate {
    suite: String,
    records: Vec<SuiteRecord>,
    /// (request index, reply index)
    entries: Vec<(usize, Option<usize>)>,
    bytes: usize,
    shapes: BTreeSet<char>,
    functions: BTreeSet<String>,
}

fn candidate(
    suite: &str,
    records: Vec<SuiteRecord>,
    allowed: &BTreeSet<String>,
) -> Result<Candidate, String> {
    let needs = needs_of(&records)?;
    for (kind, names) in &needs {
        for name in names {
            let f = format!("{kind}:{name}");
            if !allowed.contains(&f) {
                return Err(format!("needs {f}"));
            }
        }
    }
    let pairs = pair(&records);
    let mut entries = Vec::new();
    let mut created = BTreeSet::new();
    let mut touched: Vec<(i64, bool)> = Vec::new();
    let mut results = 0;
    let mut shapes = BTreeSet::new();
    let mut fns = BTreeSet::new();
    for (i, r) in records.iter().enumerate() {
        if !is_request(r) {
            continue;
        }
        let (service, method) = (s(r, "service"), s(r, "method"));
        match (s(r, "protocol"), service) {
            ("grpc", _) => {
                if !STEP_METHODS.contains(&method) {
                    return Err(format!("BE message {method}"));
                }
            }
            ("thrift", "BackendService") if method == "submit_tasks" => {
                let mut b = &r.data[..];
                let mut p = TBinaryInputProtocol::new(&mut b, true);
                p.read_message_begin().map_err(|e| e.to_string())?;
                let args = BackendServiceSubmitTasksArgs::read_from_in_protocol(&mut p)
                    .map_err(|e| e.to_string())?;
                created.extend(
                    args.tasks
                        .iter()
                        .filter_map(|t| t.create_tablet_req.as_ref().map(|c| c.tablet_id)),
                );
            }
            ("thrift", "FrontendService") => {
                if !FE_METHODS.contains(&method) {
                    continue;
                }
            }
            ("thrift", "BackendService") if method == "get_stream_load_record" => continue,
            (p, _) => return Err(format!("BE message {p} {service}.{method}")),
        }
        if method.starts_with("exec_plan_fragment") && method != "exec_plan_fragment_start" {
            let p = plan(r).ok_or("undecodable plan")?;
            if is_result_query(&p) {
                results += 1;
                if let Some(c) = shape_letter(&shape(&p)) {
                    shapes.insert(c);
                }
            }
            fns.extend(functions(&p));
            for f in p.params_list.iter().flatten() {
                let nodes = f
                    .fragment
                    .as_ref()
                    .and_then(|x| x.plan.as_ref())
                    .map(|x| x.nodes.clone())
                    .unwrap_or_default();
                for (node, tablet, _) in scan_ranges(f) {
                    let stats = nodes
                        .iter()
                        .find(|x| x.node_id == node)
                        .and_then(|x| x.olap_scan_node.as_ref())
                        .and_then(|o| o.table_name.as_deref())
                        .is_some_and(|t| t.split('(').next() == Some(STATS_TABLE));
                    touched.push((tablet, stats));
                }
                if let Some(sink) = f
                    .fragment
                    .as_ref()
                    .and_then(|x| x.output_sink.as_ref())
                    .and_then(|x| x.olap_table_sink.as_ref())
                {
                    touched.extend(sink.location.tablets.iter().map(|t| (t.tablet_id, false)));
                }
            }
        }
        entries.push((i, pairs.get(&i).copied()));
    }
    // a scan or write of a tablet created before its CREATE in this suite is caught by the replay
    for (t, stats) in touched {
        if !stats && !created.contains(&t) {
            return Err(format!("tablet {t} is not created in the suite"));
        }
    }
    let eos = entries.iter().any(|(i, j)| {
        s(&records[*i], "method") == "fetch_data"
            && j.is_some_and(|j| {
                PFetchDataResult::decode(&records[j].data[..]).is_ok_and(|x| x.eos == Some(true))
            })
    });
    if !eos {
        return Err("no eos fetch".into());
    }
    if !(1..=40).contains(&results) {
        return Err(format!("{results} result queries"));
    }
    let bytes = entries
        .iter()
        .map(|(i, j)| records[*i].data.len() + j.map_or(0, |j| records[j].data.len()))
        .sum();
    Ok(Candidate {
        suite: suite.to_string(),
        records,
        entries,
        bytes,
        shapes,
        functions: fns,
    })
}

fn write(out: &Path, c: &Candidate) {
    let dir = out.join(&c.suite);
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut index = Vec::new();
    for (k, (i, j)) in c.entries.iter().enumerate() {
        let r = &c.records[*i];
        let file = format!("{k:04}.bin");
        std::fs::write(dir.join(&file), &r.data).expect("write");
        let reply = j.map(|j| {
            let f = format!("{k:04}.reply.bin");
            std::fs::write(dir.join(&f), &c.records[j].data).expect("write");
            f
        });
        index.push(serde_json::json!({"service": s(r, "service"), "method": s(r, "method"), "file": file, "reply": reply}));
    }
    std::fs::write(
        dir.join("index.json"),
        serde_json::to_string_pretty(&index).unwrap() + "\n",
    )
    .expect("write index");
}

fn main() {
    let mut args = std::env::args().skip(1);
    let out = PathBuf::from(args.next().expect("usage: extract-replays OUT_DIR [N]"));
    let want: usize = args.next().map(|n| n.parse().expect("N")).unwrap_or(60);
    let oracle = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../oracle");
    let supported: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(oracle.join("scores/epic1-supported.json"))
            .expect("supported.json"),
    )
    .expect("json");
    let mut allowed: BTreeSet<String> = supported["features"]
        .as_array()
        .expect("features")
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    allowed.extend(M5_FEATURES.iter().map(|s| s.to_string()));
    let gate: BTreeSet<String> = std::fs::read_to_string(oracle.join("epic1/suites.txt"))
        .expect("suites.txt")
        .lines()
        .map(str::to_string)
        .collect();

    let mut found: Vec<Candidate> = Vec::new();
    let mut why: BTreeMap<String, usize> = BTreeMap::new();
    for path in suite_files(&corpus_dir()) {
        let suite = path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .trim_end_matches(".bin.gz")
            .to_string();
        if !gate.contains(&suite) {
            continue;
        }
        let mut records = read_suite(&path).expect("read suite");
        if suite == SYS_TABLES {
            records = cut_sys_tables(records);
        }
        match candidate(&suite, records, &allowed) {
            Ok(c) => found.push(c),
            Err(e) => {
                if suite == SYS_TABLES {
                    panic!("sys_tables is not replayable: {e}");
                }
                let key = e
                    .split(' ')
                    .take(if e.starts_with("BE") { 5 } else { 2 })
                    .collect::<Vec<_>>()
                    .join(" ");
                *why.entry(if key.starts_with("tablet") {
                    "tablet not created".into()
                } else {
                    key
                })
                .or_default() += 1;
            }
        }
    }
    found.sort_by_key(|c| (c.suite != SYS_TABLES, c.bytes));

    let mut picked: Vec<usize> = Vec::new();
    let mut shapes: BTreeSet<char> = BTreeSet::new();
    let mut fns: BTreeSet<String> = BTreeSet::new();
    let mut take = |i: usize, picked: &mut Vec<usize>| {
        shapes.extend(found[i].shapes.iter().copied());
        fns.extend(found[i].functions.iter().cloned());
        picked.push(i);
    };
    if found.first().is_some_and(|c| c.suite == SYS_TABLES) {
        take(0, &mut picked);
    }
    for c in "ABCDEFHIJK".chars() {
        if !found
            .iter()
            .enumerate()
            .any(|(i, _)| picked.contains(&i) && found[i].shapes.contains(&c))
        {
            if let Some(i) =
                (0..found.len()).find(|&i| !picked.contains(&i) && found[i].shapes.contains(&c))
            {
                take(i, &mut picked);
            }
        }
    }
    for f in M5_FUNCTIONS {
        if !picked.iter().any(|&i| found[i].functions.contains(*f)) {
            if let Some(i) =
                (0..found.len()).find(|&i| !picked.contains(&i) && found[i].functions.contains(*f))
            {
                take(i, &mut picked);
            }
        }
    }
    for i in 0..found.len() {
        if picked.len() >= want {
            break;
        }
        if !picked.contains(&i) {
            take(i, &mut picked);
        }
    }

    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).expect("mkdir");
    let mut index = Vec::new();
    for &i in &picked {
        write(&out, &found[i]);
        index.push(found[i].suite.clone());
    }
    std::fs::write(
        out.join("index.json"),
        serde_json::to_string_pretty(&index).unwrap() + "\n",
    )
    .expect("write index");
    let all_shapes: BTreeSet<char> = picked
        .iter()
        .flat_map(|&i| found[i].shapes.iter().copied())
        .collect();
    let all_fns: BTreeSet<&str> = M5_FUNCTIONS
        .iter()
        .copied()
        .filter(|f| picked.iter().any(|&i| found[i].functions.contains(*f)))
        .collect();
    eprintln!(
        "{} replayable gate suites; kept {}; shapes {:?}; M5 functions {:?}",
        found.len(),
        picked.len(),
        all_shapes,
        all_fns
    );
    eprintln!("rejected: {why:?}");
}
