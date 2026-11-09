//! Replays of whole corpus suites (`doris_fixtures::replays`) against an in-process BE: every
//! message the FE sent the reference C++ BE, in order, and every query's rows compared with the
//! rows the reference returned, byte for byte.
use std::collections::{HashMap, HashSet};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use doris_be::agent::AgentService;
use doris_be::brpc::is_write;
use doris_be::config::BeConfig;
use doris_be::load::LoadService;
use doris_be::master::MasterInfo;
use doris_be::query::QueryService;
use doris_be::testing::FakeFe;
use doris_fixtures::replays::{self, plan_list, shape, shape_letter, Replay, Rpc, Step};
use doris_proto::doris::{PFetchDataRequest, PFetchDataResult};
use doris_storage::{DataDir, TabletManager, TabletOptions};
use doris_thrift::gen::data::TResultBatch;
use doris_thrift::gen::frontend_service::TReportExecStatusParams;
use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
use doris_thrift::gen::plan_nodes::TPlanNodeType;
use doris_thrift::gen::types::TNetworkAddress;
use thrift::protocol::{TBinaryInputProtocol, TSerializable};

#[derive(Debug, Clone)]
pub struct Mismatch {
    pub suite: String,
    /// The query's position among the replay's result queries.
    pub query: usize,
    pub what: String,
}

struct Be {
    agent: AgentService,
    load: LoadService,
    query: QueryService,
    tablets: Arc<Mutex<TabletManager>>,
    fe: Arc<FakeFe>,
    _tmp: tempfile::TempDir,
}

fn be() -> Be {
    let tmp = tempfile::tempdir().unwrap();
    let tablets = Arc::new(Mutex::new(TabletManager::open(vec![DataDir::open(tmp.path(), "127.0.0.1").unwrap()], TabletOptions::default()).unwrap()));
    let master = MasterInfo::new("127.0.0.1");
    master.set_fe_address(TNetworkAddress::new("127.0.0.1".to_string(), 9020));
    master.set_backend_id_for_tests(10001);
    let fe = Arc::new(FakeFe::default());
    let agent = AgentService::new(&BeConfig::default(), tablets.clone(), master.clone(), fe.clone());
    let load = LoadService::new(tablets.clone(), fe.clone(), master.clone(), 8040);
    let query = QueryService::new(tablets.clone(), fe.clone(), master);
    Be { agent, load, query, tablets, fe, _tmp: tmp }
}

/// The rows of a reply's batch, each row's bytes as sent.
fn batch_rows(r: &PFetchDataResult) -> Vec<Vec<u8>> {
    let Some(b) = &r.row_batch else { return Vec::new() };
    let mut s = b.as_slice();
    TResultBatch::read_from_in_protocol(&mut TBinaryInputProtocol::new(&mut s, true)).map(|b| b.rows).unwrap_or_default()
}

/// How a query ended, and its rows.
#[derive(Debug, Default)]
struct Outcome {
    rows: Vec<Vec<u8>>,
    eos: bool,
    error: Option<String>,
}

fn outcome(replies: &[PFetchDataResult]) -> Outcome {
    let mut o = Outcome::default();
    for r in replies {
        if r.status.status_code != 0 {
            o.error = Some(r.status.error_msgs.join(" "));
            break;
        }
        o.rows.extend(batch_rows(r));
        if r.eos == Some(true) {
            o.eos = true;
            break;
        }
    }
    o
}

/// Fetches until eos or a failure; a fetch that takes 30 s is a failure.
fn drain(q: &QueryService, req: &PFetchDataRequest) -> Outcome {
    let mut replies = Vec::new();
    for _ in 0..10_000 {
        let (tx, rx) = mpsc::channel();
        let (q2, r2) = (q.clone(), *req);
        std::thread::spawn(move || {
            let _ = tx.send(q2.fetch_data(&r2));
        });
        let Ok(r) = rx.recv_timeout(Duration::from_secs(30)) else {
            return Outcome { error: Some("fetch_data hung for 30 s".into()), ..outcome(&replies) };
        };
        let end = r.eos == Some(true) || r.status.status_code != 0;
        replies.push(r);
        if end {
            break;
        }
    }
    outcome(&replies)
}

/// Whether a query's rows come in a fixed order: the result fragment's top node sorts (a SORT_NODE,
/// or an EXCHANGE_NODE merging sorted streams). A sort lower down (a window's) orders each
/// instance's rows, not the order the instances' rows reach the client.
fn ordered(plan: &TPipelineFragmentParamsList) -> bool {
    use doris_thrift::gen::data_sinks::TDataSinkType;
    plan.params_list
        .iter()
        .flatten()
        .filter_map(|p| p.fragment.as_ref())
        .filter(|f| f.output_sink.as_ref().is_some_and(|s| s.type_ == TDataSinkType::RESULT_SINK))
        .filter_map(|f| f.plan.as_ref()?.nodes.first())
        .any(|n| n.node_type == TPlanNodeType::SORT_NODE || n.exchange_node.as_ref().is_some_and(|e| e.sort_info.is_some()))
}

fn ids(plan: &TPipelineFragmentParamsList) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for p in plan.params_list.iter().flatten() {
        out.push((p.query_id.hi, p.query_id.lo));
        out.extend(p.local_params.iter().flatten().map(|l| (l.fragment_instance_id.hi, l.fragment_instance_id.lo)));
    }
    out
}

fn compare(suite: &str, query: usize, ordered: bool, got: &Outcome, want: &Outcome) -> Option<Mismatch> {
    let m = |what: String| Some(Mismatch { suite: suite.to_string(), query, what });
    if let Some(e) = &want.error {
        return if got.error.is_some() { None } else { m(format!("the reference failed ({e}); doris-rs returned {} rows", got.rows.len())) };
    }
    if let Some(e) = &got.error {
        return m(format!("doris-rs failed: {e}"));
    }
    let (mut g, mut w) = (got.rows.clone(), want.rows.clone());
    if !ordered {
        g.sort();
        w.sort();
    }
    let text = |rows: &[Vec<u8>]| rows.iter().take(5).map(|r| String::from_utf8_lossy(r).into_owned()).collect::<Vec<_>>();
    if want.eos {
        if g != w {
            return m(format!("{} rows, the reference {}: got {:?}, want {:?}", g.len(), w.len(), text(&g), text(&w)));
        }
    } else {
        // the FE stopped the reference early (LIMIT_REACH): its rows are a part of ours
        let ok = if ordered {
            g.starts_with(&w)
        } else {
            let mut left: HashMap<&Vec<u8>, usize> = HashMap::new();
            g.iter().for_each(|r| *left.entry(r).or_default() += 1);
            w.iter().all(|r| left.get_mut(r).is_some_and(|n| *n > 0 && {
                *n -= 1;
                true
            }))
        };
        if !ok {
            return m(format!("the reference's first {} rows are not in doris-rs's {}", w.len(), g.len()));
        }
    }
    None
}

fn query_id(plan: &TPipelineFragmentParamsList) -> (i64, i64) {
    plan.params_list.as_ref().and_then(|l| l.first()).map(|p| (p.query_id.hi, p.query_id.lo)).unwrap_or_default()
}

struct Driver<'a> {
    r: &'a Replay,
    be: Be,
    /// Queries that write a table (`INSERT … SELECT`): nothing fetches them, so the replay waits
    /// for each to end before going on (the reference's publish follows its report).
    writes: HashSet<(i64, i64)>,
    /// Every id a result query answers fetches to → (query number, plan).
    plans: HashMap<(i64, i64), (usize, TPipelineFragmentParamsList)>,
    /// The reference's replies, by fetched id.
    want: HashMap<(i64, i64), Vec<PFetchDataResult>>,
    executed: HashSet<(i64, i64)>,
    started: HashSet<(i64, i64)>,
    drained: HashSet<(i64, i64)>,
    out: Vec<Mismatch>,
}

impl Driver<'_> {
    fn mismatch(&mut self, query: usize, what: String) {
        self.out.push(Mismatch { suite: self.r.suite.clone(), query, what });
    }

    fn exec(&mut self, rpc: Rpc, request: &doris_proto::doris::PExecPlanFragmentRequest) {
        if is_write(request) {
            let reply = self.be.load.exec_plan_fragment(request);
            if reply.status.status_code != 0 {
                self.mismatch(usize::MAX, format!("write refused: {:?}", reply.status.error_msgs));
            }
            assert!(self.be.load.wait_idle(Duration::from_secs(60)), "{}: a write never finished", self.r.suite);
            return;
        }
        let Some(plan) = plan_list(request) else { return };
        if !self.executed.insert(query_id(&plan)) {
            return;
        }
        let reply = match rpc {
            Rpc::Exec => self.be.query.exec_plan_fragment(request),
            Rpc::Prepare => self.be.query.prepare(request),
        };
        if writes_table(&plan) {
            self.writes.insert(query_id(&plan));
            if !plan.params_list.iter().flatten().any(|p| p.need_wait_execution_trigger == Some(true)) {
                assert!(self.be.query.wait_idle(Duration::from_secs(60)), "{}: a write never finished", self.r.suite);
            }
        }
        if reply.status.status_code != 0 {
            let q = self.plans.get(&query_id(&plan)).map_or(usize::MAX, |(n, _)| *n);
            self.mismatch(q, format!("refused: {:?}", reply.status.error_msgs));
        }
    }

    fn start(&mut self, qid: (i64, i64)) {
        if self.started.contains(&qid) {
            return;
        }
        let r = self.r;
        if let Some(Step::Start(req)) = r.steps.iter().find(|s| matches!(s, Step::Start(q) if q.query_id.as_ref().map(|q| (q.hi, q.lo)) == Some(qid))) {
            self.started.insert(qid);
            self.be.query.start(req);
            if self.writes.contains(&qid) {
                assert!(self.be.query.wait_idle(Duration::from_secs(60)), "{}: a write never finished", r.suite);
            }
        }
    }

    /// Drains a query's results and compares them, once; its exec and start run first if the
    /// capture recorded them after the fetch.
    fn fetch(&mut self, request: &PFetchDataRequest) {
        let id = (request.finst_id.hi, request.finst_id.lo);
        if !self.drained.insert(id) {
            return;
        }
        let Some((query, plan)) = self.plans.get(&id).cloned() else { return };
        let qid = query_id(&plan);
        let r = self.r;
        if !self.executed.contains(&qid) {
            let exec = r.steps.iter().find_map(|s| match s {
                Step::ExecPlanFragment { rpc, request } if plan_list(request).is_some_and(|p| query_id(&p) == qid) => Some((*rpc, request.clone())),
                _ => None,
            });
            if let Some((rpc, req)) = exec {
                self.exec(rpc, &req);
            }
        }
        self.start(qid);
        let got = drain(&self.be.query, request);
        // replies recorded in one millisecond can be out of order: put them back in packet order
        let mut want = self.want[&id].clone();
        want.sort_by_key(|r| (r.status.status_code == 0, r.packet_seq.unwrap_or(0), r.eos == Some(true)));
        let reference = outcome(&want);
        // REPLAY_EXACT: compare every query's row order too (the strict gate compares it)
        let exact = std::env::var_os("REPLAY_EXACT").is_some();
        let m = compare(&r.suite, query, exact || ordered(&plan), &got, &reference);
        self.out.extend(m);
    }
}

fn writes_table(plan: &TPipelineFragmentParamsList) -> bool {
    use doris_thrift::gen::data_sinks::TDataSinkType;
    plan.params_list.iter().flatten().any(|p| p.fragment.as_ref().and_then(|f| f.output_sink.as_ref()).is_some_and(|s| s.type_ == TDataSinkType::OLAP_TABLE_SINK))
}

/// What the FE reads of a done write report, comparable across runs: the error log's shard and
/// the order of commit infos and instance reports set aside.
fn report_view(p: &TReportExecStatusParams) -> String {
    let mut commits: Vec<i64> = p.commit_infos.iter().flatten().map(|c| c.tablet_id).collect();
    commits.sort();
    let mut instances: Vec<String> = p
        .fragment_instance_reports
        .iter()
        .flatten()
        .map(|r| format!("{:?} rows={:?} bytes={:?} ranges={:?}", r.fragment_instance_id, r.loaded_rows, r.loaded_bytes, r.num_finished_range))
        .collect();
    instances.sort();
    let shard = |u: &str| {
        let mut out = String::new();
        let mut rest = u;
        while let Some(i) = rest.find("__shard_") {
            out.push_str(&rest[..i + 8]);
            rest = rest[i + 8..].trim_start_matches(|c: char| c.is_ascii_digit());
        }
        out + rest
    };
    format!(
        "fragment {:?} instance {:?} backend_num {:?} status {:?} done {:?} type {:?} counters {:?} url {:?} commits {commits:?} instances {instances:?} first_error {:?} loaded {:?}/{:?}",
        p.fragment_id,
        p.fragment_instance_id,
        p.backend_num,
        p.status,
        p.done,
        p.query_type,
        p.load_counters,
        p.tracking_url.as_deref().map(shard),
        p.first_error_msg,
        p.loaded_rows,
        p.loaded_bytes,
    )
}

/// Each write's done reports, by (query, fragment), as `report_view` shows them.
fn write_reports(calls: impl IntoIterator<Item = TReportExecStatusParams>, writes: &HashSet<(i64, i64)>) -> Vec<((i64, i64), i32, String)> {
    let mut out: Vec<_> = calls
        .into_iter()
        .filter(|p| p.done == Some(true))
        .filter_map(|p| {
            let q = p.query_id.as_ref().map(|q| (q.hi, q.lo))?;
            writes.contains(&q).then(|| (q, p.fragment_id.unwrap_or(-1), report_view(&p)))
        })
        .collect();
    out.sort();
    // the FE takes the first done report of each (query, fragment)
    out.dedup_by(|a, b| (a.0, a.1) == (b.0, b.1));
    out
}

/// Runs a replay; every query whose rows differ from the reference's is a mismatch.
pub fn replay(r: &Replay) -> Vec<Mismatch> {
    replay_writes(r).0
}

/// Runs a replay, and gives its writes' done reports beside the reference's.
#[allow(clippy::type_complexity)]
pub fn replay_writes(r: &Replay) -> (Vec<Mismatch>, Vec<((i64, i64), i32, String)>, Vec<((i64, i64), i32, String)>) {
    let be = be();
    let replies = Mutex::new(r.fe_replies());
    be.fe.answer_from(Box::new(move |method, call| replies.lock().unwrap().reply(method, call)));
    for c in r.synthetic_tablets() {
        be.tablets.lock().unwrap().create_tablet(&c).unwrap();
    }
    let mut plans = HashMap::new();
    let mut want: HashMap<(i64, i64), Vec<PFetchDataResult>> = HashMap::new();
    let mut n = 0;
    for s in &r.steps {
        match s {
            Step::ExecPlanFragment { request, .. } => {
                if let Some(p) = plan_list(request).filter(replays::is_result_query) {
                    for id in ids(&p) {
                        plans.insert(id, (n, p.clone()));
                    }
                    n += 1;
                }
            }
            Step::Fetch { request, reply } => want.entry((request.finst_id.hi, request.finst_id.lo)).or_default().push(reply.clone()),
            _ => {}
        }
    }
    let mut d = Driver { r, be, writes: HashSet::new(), plans, want, executed: HashSet::new(), started: HashSet::new(), drained: HashSet::new(), out: Vec::new() };
    for s in &r.steps {
        match s {
            Step::SubmitTasks(tasks) => {
                d.be.agent.submit_tasks(tasks.clone());
                assert!(d.be.agent.wait_idle(Duration::from_secs(60)), "{}: agent tasks never finished", r.suite);
            }
            Step::ExecPlanFragment { rpc, request } => d.exec(*rpc, request),
            Step::Start(req) => d.start(req.query_id.as_ref().map(|q| (q.hi, q.lo)).unwrap_or_default()),
            Step::Fetch { request, .. } => d.fetch(request),
            Step::Cancel(req) => {
                // a cancel recorded before the query's fetches: fetch first
                let qid = req.query_id.as_ref().map(|q| (q.hi, q.lo)).unwrap_or_default();
                let pending: Vec<PFetchDataRequest> = r
                    .steps
                    .iter()
                    .filter_map(|s| match s {
                        Step::Fetch { request, .. } if d.plans.get(&(request.finst_id.hi, request.finst_id.lo)).is_some_and(|(_, p)| query_id(p) == qid) => {
                            Some(*request)
                        }
                        _ => None,
                    })
                    .collect();
                for f in pending {
                    d.fetch(&f);
                }
                d.be.query.cancel(req);
                d.be.load.cancel_plan_fragment(req);
            }
            Step::UpdateCache => {
                d.be.query.update_cache();
            }
            Step::FetchCache => {
                d.be.query.fetch_cache();
            }
        }
    }
    let got = d.be.fe.load_calls().into_iter().filter_map(|c| match c {
        doris_be::testing::LoadCall::ExecStatus(_, p) => Some(p),
        _ => None,
    });
    let got = write_reports(got, &d.writes);
    let want = r.fe_calls("reportExecStatus").into_iter().filter_map(|m| {
        let mut b = m;
        let mut i = TBinaryInputProtocol::new(&mut b, true);
        use thrift::protocol::TInputProtocol;
        i.read_message_begin().ok()?;
        doris_thrift::gen::frontend_service::FrontendServiceReportExecStatusArgs::read_from_in_protocol(&mut i).ok().map(|a| a.params)
    });
    let want = write_reports(want, &d.writes);
    (d.out, got, want)
}

/// The replays whose every result query has one of these shapes ([wire] §6).
pub fn replays_with_shapes(shapes: &[char]) -> Vec<Replay> {
    replays::all()
        .into_iter()
        .filter(|r| {
            let q = r.queries();
            q.iter().filter(|p| replays::is_result_query(p)).all(|p| shape_letter(&shape(p)).is_some_and(|c| shapes.contains(&c)))
        })
        .collect()
}

fn check(shapes: &[char]) {
    let only = std::env::var("REPLAY").ok();
    let rs: Vec<Replay> = replays_with_shapes(shapes).into_iter().filter(|r| only.as_ref().is_none_or(|o| r.suite.contains(o.as_str()))).collect();
    assert!(!rs.is_empty(), "no replay of shapes {shapes:?}");
    let mut all = Vec::new();
    for r in &rs {
        all.extend(replay(r));
    }
    let lines: Vec<String> = all.iter().map(|m| format!("{} #{}: {}", m.suite, m.query, m.what)).collect();
    assert!(all.is_empty(), "{} of {} replays mismatch:\n{}", lines.len(), rs.len(), lines.join("\n"));
    eprintln!("{} replays of shapes {shapes:?} match the reference", rs.len());
}

#[test]
fn shapes_a_e_h_match_the_reference() {
    check(&['A', 'E', 'H']);
}

#[test]
fn shapes_b_c_match_the_reference() {
    check(&['A', 'B', 'C', 'E', 'H']);
}

#[test]
fn shapes_d_i_j_k_match_the_reference() {
    check(&['A', 'B', 'C', 'D', 'E', 'H', 'I', 'J', 'K']);
}

#[test]
fn sys_tables_matches_the_reference_up_to_the_set() {
    let r = replays::load("nereids_p0.system.test_query_sys_tables.test_query_sys_tables");
    let m = replay(&r);
    assert!(m.is_empty(), "{m:#?}");
}

#[test]
fn a_cte_matches_the_reference() {
    let r = replays::load("correctness_p0.test_subquery_grouping.test_subquery_grouping");
    let m = replay(&r);
    assert!(m.is_empty(), "{m:#?}");
}

#[test]
fn every_replay_matches_the_reference() {
    let mut all = Vec::new();
    let rs = replays::all();
    for r in &rs {
        all.extend(replay(r));
    }
    let lines: Vec<String> = all.iter().map(|m| format!("{} #{}: {}", m.suite, m.query, m.what)).collect();
    assert!(all.is_empty(), "{} mismatches over {} replays:\n{}", lines.len(), rs.len(), lines.join("\n"));
}

/// INSERT … SELECT: the rows later queries read back, and every write's done reports (the
/// counters, instance reports, commit infos and errors the FE commits from), match the reference.
#[test]
fn insert_selects_match_the_reference() {
    for s in [
        "insert_p0.test_insert_docs_demo.test_insert_docs_demo",
        "query_p0.keyword.test_role_mapping_keyword.test_role_mapping_keyword",
        "insert_p0.test_insert_partition_fail_url.test_insert_partition_fail_url",
        "nereids_p0.ddl.clean.test_clean_label_nereids.test_clean_label_nereids",
    ] {
        let r = replays::load(s);
        let (m, got, want) = replay_writes(&r);
        assert!(m.is_empty(), "{s}: {m:?}");
        assert!(!want.is_empty(), "{s}: no write reports in the reference");
        assert_eq!(got.len(), want.len(), "{s}: reports\n got {got:#?}\nwant {want:#?}");
        for (g, w) in got.iter().zip(&want) {
            assert_eq!(g, w, "{s}");
        }
    }
}

#[test]
fn set_operations_match_the_reference() {
    for s in [
        "correctness_p0.test_set_operation.test_set_operation",
        "nereids_p0.test_filter_pushdown_set.test_filter_pushdown_set",
        "query_p0.set_operations.set_with_null.set_with_null.set_with_null",
        "query_p0.set_operations.test_pullup_predicate_null.test_pullup_predicate_null",
    ] {
        let m = replay(&replays::load(s));
        assert!(m.is_empty(), "{s}: {m:#?}");
    }
}

#[test]
fn a_limit_on_a_set_node_applies_after_the_set_operation() {
    // limit 0 on every set node: each set query then returns no rows, so it differs from the
    // reference exactly where the reference has rows
    use doris_thrift::gen::plan_nodes::TPlanNodeType as P;
    use thrift::protocol::{TCompactOutputProtocol, TSerializable};
    let mut r = replays::load("query_p0.set_operations.test_pullup_predicate_null.test_pullup_predicate_null");
    let mut changed = 0;
    for s in &mut r.steps {
        if let Step::ExecPlanFragment { request, .. } = s {
            let Some(mut list) = plan_list(request) else { continue };
            let mut hit = false;
            for p in list.params_list.iter_mut().flatten() {
                for n in p.fragment.iter_mut().flat_map(|f| f.plan.iter_mut()).flat_map(|x| x.nodes.iter_mut()) {
                    if matches!(n.node_type, P::INTERSECT_NODE | P::EXCEPT_NODE) {
                        n.limit = 0;
                        hit = true;
                    }
                }
            }
            if hit {
                let mut buf = Vec::new();
                list.write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut buf)).unwrap();
                request.request = Some(buf);
                changed += 1;
            }
        }
    }
    assert!(changed > 0);
    let m = replay(&r);
    assert!(!m.is_empty(), "limit 0 changed nothing");
    for x in &m {
        assert!(x.what.starts_with("0 rows, the reference "), "{x:?}");
    }
}

const WINDOWS: [&str; 7] = [
    "correctness_p0.test_min_max_window.test_min_max_window",
    "nereids_p0.partition_topn.check_partitionkey.check_partitionkey",
    "nereids_p0.select_no_from.sql.withGroupOrderHaving.withGroupOrderHaving",
    "query_p0.dual.dual.dual",
    "query_p0.select_no_from.sql.withGroupOrderHaving.withGroupOrderHaving",
    "query_p0.sql_functions.window_functions.test_partition_topn.test_partition_topn",
    "correctness_p0.test_always_nullable_window_function.test_always_nullable_window_function",
];

#[test]
fn window_functions_match_the_reference() {
    let mut bad = Vec::new();
    for s in WINDOWS {
        let m = replay(&replays::load(s));
        if !m.is_empty() {
            bad.push(format!("{s}: {} mismatches, first: #{} {}", m.len(), m[0].query, m[0].what));
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

/// A suite replayed up to its first INSERT … SELECT: its tables created and loaded, and that
/// request, not yet sent.
fn before_insert_select(suite: &str) -> (Be, doris_proto::doris::PExecPlanFragmentRequest, TPipelineFragmentParamsList) {
    let r = replays::load(suite);
    let be = be();
    let replies = Mutex::new(r.fe_replies());
    be.fe.answer_from(Box::new(move |method, call| replies.lock().unwrap().reply(method, call)));
    for s in &r.steps {
        match s {
            Step::SubmitTasks(tasks) => {
                be.agent.submit_tasks(tasks.clone());
                assert!(be.agent.wait_idle(Duration::from_secs(60)));
            }
            Step::ExecPlanFragment { request, .. } if is_write(request) => {
                assert_eq!(be.load.exec_plan_fragment(request).status.status_code, 0);
                assert!(be.load.wait_idle(Duration::from_secs(60)));
            }
            Step::ExecPlanFragment { request, .. } => {
                if let Some(plan) = plan_list(request).filter(writes_table) {
                    return (be, request.clone(), plan);
                }
            }
            _ => {}
        }
    }
    panic!("{suite}: no INSERT … SELECT");
}

fn exec_reports(be: &Be) -> Vec<TReportExecStatusParams> {
    be.fe
        .load_calls()
        .into_iter()
        .filter_map(|c| match c {
            doris_be::testing::LoadCall::ExecStatus(_, p) => Some(p),
            _ => None,
        })
        .collect()
}

/// The FE cancels a write while it runs (`insert_timeout`, KILL): nothing is committed, and
/// every fragment reports CANCELLED, with no commit infos.
#[test]
fn a_write_the_fe_cancels_commits_nothing() {
    let (be, req, plan) = before_insert_select("insert_p0.test_insert_docs_demo.test_insert_docs_demo");
    let before = exec_reports(&be).len();
    let qid = query_id(&plan);
    {
        // the instances wait on the tablets while the cancel arrives
        let _held = be.tablets.lock().unwrap();
        assert_eq!(be.query.exec_plan_fragment(&req).status.status_code, 0);
        std::thread::sleep(Duration::from_millis(200));
        be.query.cancel(&doris_proto::doris::PCancelPlanFragmentRequest {
            finst_id: doris_proto::doris::PUniqueId { hi: qid.0, lo: qid.1 },
            query_id: Some(doris_proto::doris::PUniqueId { hi: qid.0, lo: qid.1 }),
            cancel_status: Some(doris_proto::doris::PStatus { status_code: 1, error_msgs: vec!["insert timeout".into()] }),
            ..Default::default()
        });
    }
    assert!(be.query.wait_idle(Duration::from_secs(30)));
    let reports = &exec_reports(&be)[before..];
    assert_eq!(reports.len(), plan.params_list.as_ref().unwrap().len(), "{reports:?}");
    for p in reports {
        assert_eq!(p.status.as_ref().map(|s| s.status_code), Some(doris_thrift::gen::status::TStatusCode::CANCELLED), "{p:?}");
        assert_eq!(p.commit_infos, None);
    }
}

/// One instance fails (its scan's tablet is gone) while the others run: nothing is committed,
/// and the write reports the error, with no commit infos.
#[test]
fn one_failed_instance_fails_the_whole_write() {
    let (be, req, plan) = before_insert_select("insert_p0.test_insert_docs_demo.test_insert_docs_demo");
    let before = exec_reports(&be).len();
    let first = plan.params_list.as_ref().unwrap()[0].local_params.as_ref().unwrap()[0].clone();
    let tablet = first.per_node_scan_ranges.values().flatten().find_map(|r| r.scan_range.palo_scan_range.as_ref()).unwrap().tablet_id;
    let schema_hash = be.tablets.lock().unwrap().tablet(tablet).unwrap().schema_hash.unwrap_or(0);
    be.tablets
        .lock()
        .unwrap()
        .drop_tablet(&doris_thrift::gen::agent_service::TDropTabletReq::new(tablet, Some(schema_hash), None, None))
        .unwrap();
    assert_eq!(be.query.exec_plan_fragment(&req).status.status_code, 0);
    assert!(be.query.wait_idle(Duration::from_secs(30)));
    let reports = &exec_reports(&be)[before..];
    assert!(!reports.is_empty());
    for p in reports {
        assert_ne!(p.status.as_ref().map(|s| s.status_code), Some(doris_thrift::gen::status::TStatusCode::OK), "{p:?}");
        assert_eq!(p.commit_infos, None);
        assert_eq!(p.query_type, Some(doris_thrift::gen::palo_internal_service::TQueryType::LOAD));
    }
}

/// A panic while a write reports (here in the FE client) still ends the query: the service is
/// idle again and forgets it.
#[test]
fn a_panic_while_reporting_ends_the_write() {
    let (be, req, _) = before_insert_select("insert_p0.test_insert_docs_demo.test_insert_docs_demo");
    be.fe.panic_on_exec_status(true);
    assert_eq!(be.query.exec_plan_fragment(&req).status.status_code, 0);
    assert!(be.query.wait_idle(Duration::from_secs(10)), "the running count was lost");
    assert_eq!(be.query.live_state().0, 0);
}

/// Story 5's replays: arithmetic, conditionals, like, concat, length, round.
const SCALARS: [&str; 32] = [
    "correctness_p0.test_char_default_value.test_char_default_value",
    "correctness_p0.test_implict_cast.test_implicit_cast",
    "correctness_p0.test_like_predicate_with_nullable.test_like_predicate_with_nullable",
    "correctness_p0.test_no_grouping_window.test_no_grouping_window",
    "correctness_p0.test_orderby_mod.test_order_by_mod",
    "datatype_p0.decimalv3.test_if_functions.test_if_functions",
    "nereids_p0.aggregate.agg_window_project.agg_window_project",
    "nereids_p0.aggregate.aggregate_groupby_constant.aggregate_groupby_constant",
    "nereids_p0.authorization.view_authorization.view_authorization",
    "nereids_p0.ddl.show_variables.show_variables_command.show_variables_command",
    "nereids_p0.expression.case_when_to_if.test_case_when_to_if",
    "nereids_p0.expression.fold_constant.fold_constant_cast.fold_constant_cast",
    "nereids_p0.select_no_from.sql.projectAggFuncs.projectAggFuncs",
    "nereids_p0.show.test_show_columns_command.test_show_columns_command",
    "nereids_p0.sql_functions.math_functions.test_multiply.test_multiply",
    "nereids_p0.sql_functions.math_functions.test_round_overflow.test_round_overflow",
    "nereids_p0.sql_functions.string_functions.test_string_function_like.test_string_function_like",
    "nereids_p0.sql_functions.string_functions.test_string_function_like_pushdown.test_string_function_like_pushdown",
    "query_p0.aggregate.support_type.count.count.count",
    "query_p0.select_no_from.sql.projectAggFuncs.projectAggFuncs",
    "query_p0.sql_functions.conditional_functions.test_ifnull.test_ifnull",
    "query_p0.sql_functions.math_functions.test_multiply.test_multiply",
    "query_p0.sql_functions.string_functions.test_like_escape.test_like_escapes",
    "query_p0.sql_functions.string_functions.test_like_literal.test_like_literal",
    "query_p0.sql_functions.string_functions.test_like_no_backslash_escapes_mode.test_like_no_backslash_escapes_mode",
    "query_p0.sql_functions.string_functions.test_like_table.test_like_table",
    "query_p0.sql_functions.string_functions.test_string_function_like_pushdown.test_string_function_like_pushdown",
    "query_p0.sql_functions.window_functions.test_window_float_sliding_frame.test_window_float_sliding_frame",
    "query_p0.union.test_union_decimal_agg.test_union_decimal_agg",
    "query_p0.view.test_view2.test_view2",
    "query_p0.virtual_slot_ref.char_type_shrink_before_project.char_type_shrink_before_projec",
    "query_p0.virtual_slot_ref.virtual_slot_ref_basic.virtual_slot_ref_basic",
];

#[test]
fn scalar_functions_match_the_reference() {
    let mut bad = Vec::new();
    for s in SCALARS {
        let m = replay(&replays::load(s));
        if !m.is_empty() {
            bad.push(format!("{s}: {} mismatches, first: #{} {}", m.len(), m[0].query, m[0].what));
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}
