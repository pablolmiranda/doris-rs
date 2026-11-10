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
/// or an EXCHANGE_NODE merging sorted streams), under nothing but nodes that keep their child's
/// order (a top-n's MATERIALIZATION_NODE, a SELECT_NODE). A sort lower down (a window's) orders
/// each instance's rows, not the order the instances' rows reach the client.
fn ordered(plan: &TPipelineFragmentParamsList) -> bool {
    use doris_thrift::gen::data_sinks::TDataSinkType;
    plan.params_list
        .iter()
        .flatten()
        .filter_map(|p| p.fragment.as_ref())
        .filter(|f| f.output_sink.as_ref().is_some_and(|s| s.type_ == TDataSinkType::RESULT_SINK))
        .filter_map(|f| f.plan.as_ref())
        .any(|p| {
            // pre-order: a single-child node's child is the next node
            let keeps_order = |n: &&doris_thrift::gen::plan_nodes::TPlanNode| {
                matches!(n.node_type, TPlanNodeType::MATERIALIZATION_NODE | TPlanNodeType::SELECT_NODE) && n.num_children == 1
            };
            p.nodes.iter().find(|n| !keeps_order(n)).is_some_and(|n| {
                n.node_type == TPlanNodeType::SORT_NODE || n.exchange_node.as_ref().is_some_and(|e| e.sort_info.is_some())
            })
        })
}

/// Whether the result fragment's top node is an exchange taking a LIMIT of rows it does not
/// sort, from more than one sender: which rows it keeps is the senders' arrival order (C++'s
/// varied across reference runs: the marks mask them), so such a reply is compared by its row
/// count, as the readback check compares `*_head_unordered`. With one sender the rows are fixed.
fn head_unordered(plan: &TPipelineFragmentParamsList) -> bool {
    use doris_thrift::gen::data_sinks::TDataSinkType;
    !ordered(plan)
        && plan.params_list.iter().flatten().any(|p| {
            let Some(f) = p.fragment.as_ref().filter(|f| f.output_sink.as_ref().is_some_and(|s| s.type_ == TDataSinkType::RESULT_SINK)) else {
                return false;
            };
            let keeps_order = |n: &&doris_thrift::gen::plan_nodes::TPlanNode| {
                matches!(n.node_type, TPlanNodeType::MATERIALIZATION_NODE | TPlanNodeType::SELECT_NODE) && n.num_children == 1
            };
            f.plan.iter().flat_map(|pl| pl.nodes.iter().find(|n| !keeps_order(n))).any(|n| {
                n.node_type == TPlanNodeType::EXCHANGE_NODE && n.limit >= 0 && p.per_exch_num_senders.get(&n.node_id).is_some_and(|s| *s > 1)
            })
        })
}

fn ids(plan: &TPipelineFragmentParamsList) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for p in plan.params_list.iter().flatten() {
        out.push((p.query_id.hi, p.query_id.lo));
        out.extend(p.local_params.iter().flatten().map(|l| (l.fragment_instance_id.hi, l.fragment_instance_id.lo)));
    }
    out
}

fn compare(suite: &str, query: usize, ordered: bool, by_count: bool, got: &Outcome, want: &Outcome) -> Option<Mismatch> {
    let m = |what: String| Some(Mismatch { suite: suite.to_string(), query, what });
    if let Some(e) = &want.error {
        return if got.error.is_some() { None } else { m(format!("the reference failed ({e}); doris-rs returned {} rows", got.rows.len())) };
    }
    if let Some(e) = &got.error {
        return m(format!("doris-rs failed: {e}"));
    }
    if by_count {
        // the reference may have stopped early (LIMIT_REACH): then it has at most our rows
        let ok = if want.eos { got.rows.len() == want.rows.len() } else { got.rows.len() >= want.rows.len() };
        return if ok { None } else { m(format!("{} rows, the reference {} (compared by count)", got.rows.len(), want.rows.len())) };
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
            return m(format!("the reference's first {} rows are not in doris-rs's {}: got {:?}, want {:?}", w.len(), g.len(), text(&g), text(&w)));
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
        // the reference's own answer: C++ fails exec_plan_fragment when a constant does
        let qid = query_id(&plan);
        let reference = self.r.steps.iter().find_map(|s| match s {
            Step::ExecPlanFragment { request, reply: Some(reply), .. } if plan_list(request).is_some_and(|p| query_id(&p) == qid) => Some(reply.status.clone()),
            _ => None,
        });
        let q = self.plans.get(&qid).map_or(usize::MAX, |(n, _)| *n);
        let failed_alike = reference.as_ref().is_some_and(|st| st.status_code == reply.status.status_code && st.error_msgs == reply.status.error_msgs);
        if reply.status.status_code != 0 && !failed_alike {
            self.mismatch(q, format!("refused: {:?}", reply.status.error_msgs));
        } else if reply.status.status_code == 0 && reference.as_ref().is_some_and(|st| st.status_code != 0) {
            self.mismatch(q, format!("accepted what the reference refused: {:?}", reference.map(|st| st.error_msgs)));
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
                Step::ExecPlanFragment { rpc, request, .. } if plan_list(request).is_some_and(|p| query_id(&p) == qid) => Some((*rpc, request.clone())),
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
        let m = compare(&r.suite, query, exact || ordered(&plan), !exact && head_unordered(&plan), &got, &reference);
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
            Step::ExecPlanFragment { rpc, request, .. } => d.exec(*rpc, request),
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

/// The M6 story 6 replays (joins, ASSERT_NUM_ROWS, CASE, `nullable`), as in the fixtures test.
const JOINS: [&str; 55] = [
    "correctness_p0.test_bucket_join_with_colocate_table.test_bucket_join_with_colocate_table",
    "correctness_p0.test_bugfix_block_reuse.test_bugfix_block_reuse",
    "correctness_p0.test_colocate_join_with_different_tablets.test_colocate_join_with_different_tablets",
    "correctness_p0.test_column_prune_in_hash_join.test_column_prune_in_hash_join",
    "correctness_p0.test_distinct_streaming_agg_local_shuffle.test_distinct_streaming_agg_local_shuffle",
    "correctness_p0.test_having_subquery_with_nullliteral.test_having_subquery_with_nullliteral",
    "correctness_p0.test_join_with_projection.test_join_with_projection",
    "correctness_p0.test_join_without_condition.test_join_without_condition",
    "correctness_p0.test_null_aware_left_anti_join.test_null_aware_left_anti_join",
    "correctness_p0.test_outer_join_with_cross_join.test_outer_join_with_cross_join",
    "correctness_p0.test_outer_join_with_empty_node.test_outer_join_with_empty_node",
    "correctness_p0.test_outer_join_with_order_by.test_outer_join_with_order_by",
    "correctness_p0.test_outer_join_with_subquery.test_outer_join_with_subquery",
    "correctness_p0.test_rf_in_list_not_erased_by_scope_range.test_rf_in_list_not_erased_by_scope_range",
    "correctness_p0.test_runtime_filter.test_runtime_filter",
    "correctness_p0.test_runtime_filter_outer_join_nullable_side.test_runtime_filter_outer_join_nullable_side",
    "datatype_p0.decimalv3.test_decimalv3_cast_to_string.test_decimalv3_cast_to_string",
    "datatype_p0.decimalv3.test_predicate_with_implicit_cast.test_predicate_with_implicit_cast",
    "nereids_p0.colocate.test_multi_cast_colocate.test_multi_cast_colocate",
    "nereids_p0.colocate.test_set_operation_colocate.test_set_operation_colocate",
    "nereids_p0.cte.costbasedrewrite_producer.costbasedrewrite_producer.costbasedrewrite_producer",
    "nereids_p0.cte.subquery_in_cte.subquery_in_cte.subquery_in_cte",
    "nereids_p0.eager_agg.scalar_agg_pushdown.scalar_agg_pushdown",
    "nereids_p0.join.asof.test_asof_join_large_bucket.test_asof_join_large_bucket",
    "nereids_p0.join.outer_join_with_empty_node.nereids_outer_join_with_empty_node",
    "nereids_p0.join.test_join2.test_join2",
    "nereids_p0.join.test_join3.test_join3",
    "nereids_p0.join.test_join4.test_join4",
    "nereids_p0.join.test_join_local_shuffle.test_join_local_shuffle",
    "nereids_p0.join.test_limit_join.test_limit_join",
    "nereids_p0.join.test_mark_join.test_mark_join",
    "nereids_p0.join.test_nestedloop_outer_join.test_nestedloop_outer_join",
    "nereids_p0.join.test_nestedloop_semi_anti_join.test_nestedloop_semi_anti_join",
    "nereids_p0.join.test_other_conjuncts_nullable.test_other_conjuncts_nullable",
    "nereids_p0.join.test_runtimefilter_on_datev2.test_runtimefilter_on_datev2",
    "nereids_p0.join.test_runtimefilter_on_decimal.test_runtimefilter_on_decimal",
    "nereids_p0.limit.OffsetInSubqueryWithJoin.test_offset_in_subquery_with_join",
    "nereids_p0.runtime_filter.expr-target.expr-target",
    "nereids_p0.runtime_filter.set_operator_in_filter.set_operator_in_filter",
    "nereids_p0.sql_functions.test_in_expr.test_in_expr",
    "nereids_p0.subquery.test_subquery_in_simple_case.test_subquery_in_simple_case",
    "query_p0.join.eliminate_const_join_condition.eliminate_const_join_condition",
    "query_p0.join.rqg.doris8317.doris8317.doris8317",
    "query_p0.join.rqg.doris9100.doris9100.doris9100",
    "query_p0.join.rqg.rqg4265.rqg4265.rqg4265",
    "query_p0.join.test_full_join_batch_size.test_full_join_batch_size",
    "query_p0.join.test_hash_join_probe_early_eos.test_hash_join_probe_early_eos",
    "query_p0.join.test_join3.test_join3",
    "query_p0.join.test_join4.test_join4",
    "query_p0.join.test_join6.test_join6",
    "query_p0.join.test_join_result_count.test_join_result_count",
    "query_p0.join.test_left_join_batch_size.test_left_join_batch_size",
    "query_p0.join.test_nestedloop_semi_anti_join.test_nestedloop_semi_anti_join",
    "query_p0.join.test_runtimefilter_2.test_runtimefilter_2",
    "query_p0.sql_functions.window_functions.test_qualify_query.test_qualify_query",
];

#[test]
fn joins_match_the_reference() {
    let mut bad = Vec::new();
    for s in JOINS {
        let m = replay(&replays::load(s));
        if !m.is_empty() {
            bad.push(format!("{s}: {} mismatches, first: #{} {}", m.len(), m[0].query, m[0].what));
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

/// The replays whose result queries sort somewhere (a window's sort, a sort under a parallel
/// result sink) but not at the result fragment's top: `ordered` compares their rows unordered.
/// Listed so that a change to the set shows in review.
const COMPARED_UNORDERED: &[&str] = &[
    // a window's sort over a lateral view, with no ORDER BY on the query (M6 s7 batch 2)
    "correctness_p0.test_inlineview_with_lateralview.test_inlineview_with_lateralview",
    // a scalar subquery's ORDER BY … LIMIT 1 under a join
    "correctness_p0.test_join_with_projection.test_join_with_projection",
    "correctness_p0.test_no_grouping_window.test_no_grouping_window",
    // a window's sort under an outer join, with no ORDER BY on the query (M6 s7)
    "correctness_p0.test_outer_join_with_window_function.test_outer_join_with_with_window_function",
    "correctness_p0.test_outer_join_with_window_function_datev2.test_outer_join_with_window_function_datev2",
    "nereids_p0.aggregate.agg_window_project.agg_window_project",
    // a subquery's ORDER BY … OFFSET under a join
    "nereids_p0.limit.OffsetInSubqueryWithJoin.test_offset_in_subquery_with_join",
    // QUALIFY's window sort, with no ORDER BY on the query
    "query_p0.sql_functions.window_functions.test_qualify_query.test_qualify_query",
    "query_p0.sql_functions.window_functions.test_window_float_sliding_frame.test_window_float_sliding_frame",
];

/// The replays with a result query whose top exchange takes a LIMIT of unsorted rows from several
/// senders: `head_unordered` compares those replies by row count. Listed so that a change to the set shows in review.
const COMPARED_BY_COUNT: &[&str] = &[
    // `GROUP BY t2.c1 LIMIT 1 OFFSET 1` (no ORDER BY) gathered from 10 instances: the group kept
    // is whichever instance's arrives first; the reference varied too (exchanges 29 and 31)
    "nereids_p0.join.test_limit_join.test_limit_join",
];

#[test]
fn replays_compared_by_count_are_listed() {
    let mut got: Vec<String> = replays::all()
        .iter()
        .filter(|r| r.queries().iter().filter(|q| replays::is_result_query(q)).any(head_unordered))
        .map(|r| r.suite.clone())
        .collect();
    got.sort();
    assert_eq!(got, COMPARED_BY_COUNT, "{got:#?}");
}

#[test]
fn replays_compared_unordered_are_listed() {
    let any_sort = |plan: &TPipelineFragmentParamsList| {
        plan.params_list.iter().flatten().filter_map(|p| p.fragment.as_ref()?.plan.as_ref()).flat_map(|p| &p.nodes).any(|n| {
            n.node_type == TPlanNodeType::SORT_NODE || n.exchange_node.as_ref().is_some_and(|e| e.sort_info.is_some())
        })
    };
    let mut got: Vec<String> = replays::all()
        .iter()
        .filter(|r| r.queries().iter().filter(|q| replays::is_result_query(q)).any(|q| any_sort(q) && !ordered(q)))
        .map(|r| r.suite.clone())
        .collect();
    got.sort();
    assert_eq!(got, COMPARED_UNORDERED, "{got:#?}");
}


/// The M6 story 7 batch 1 replays, as in the fixtures test.
const BATCH1: [&str; 16] = [
    "correctness_p0.test_assert_row_num.test_assert_num_rows",
    "correctness_p0.test_grouping_sets_empty.test_grouping_sets_empty",
    "correctness_p0.test_like_predicate_with_concat.test_like_predicate_with_concat",
    "correctness_p0.test_null_equal.test_null_equal",
    "correctness_p0.test_outer_join_with_window_function.test_outer_join_with_with_window_function",
    "correctness_p0.test_outer_join_with_window_function_datev2.test_outer_join_with_window_function_datev2",
    "nereids_p0.aggregate.agg_distinct_case_when.agg_distinct_case_when",
    "nereids_p0.aggregate.agg_with_distinct_project.agg_with_distinct_project",
    "nereids_p0.aggregate.aggregate_groupby_simplify.aggregate_groupby_simplify",
    "nereids_p0.aggregate.aggregate_stddev_over_range.aggregate_stddev_over_range",
    "nereids_p0.grouping_sets.test_grouping_sets1.test_grouping_sets1",
    "query_p0.having.having_between.test_having_between",
    "query_p0.join.rqg.rqg500079780.rqg500079780.rqg1333347798",
    "query_p0.set.test_user_var.test_user_var",
    "query_p0.sql_functions.string_functions.test_regexp_chinese.test_regexp_chinese",
    "query_p0.sql_functions.string_functions.test_regexp_extended_nonconstant_pattern.test_regexp_extended_nonconstant_pattern",
];

#[test]
fn batch1_matches_the_reference() {
    let mut bad = Vec::new();
    for s in BATCH1 {
        let m = replay(&replays::load(s));
        if !m.is_empty() {
            bad.push(format!("{s}: {} mismatches, first: #{} {}", m.len(), m[0].query, m[0].what));
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

/// M6 story 7 batch 2 (geo, table functions, moments, dates, bits).
const BATCH2: [&str; 39] = [
    "correctness_p0.test_agg_materialize.test_agg_materialize",
    "correctness_p0.test_bit_function.test_bit_functions",
    "correctness_p0.test_bit_shift_lagency.test_bit_shift",
    "correctness_p0.test_bit_shift_nereids.test_bit_shift_nereids",
    "correctness_p0.test_bit_test_function.test_bit_test_function",
    "correctness_p0.test_case_when_date.test_case_when_date",
    "correctness_p0.test_date_diff.test_date_diff",
    "correctness_p0.test_inlineview_with_lateralview.test_inlineview_with_lateralview",
    "correctness_p0.test_nested_lateral_view.test_nested_lateral_view",
    "correctness_p0.test_outer_join_sort.test_outer_join_sort",
    "correctness_p0.test_probe_clean.test_probe_clean",
    "correctness_p0.test_select_constant.test_select_constant",
    "correctness_p0.test_table_function_with_subquery.test_table_function_with_subquery",
    "correctness_p0.test_window_function_with_project.test_window_function_with_project",
    "correctness_p0.test_xor_pred.test_xor_pred",
    "datatype_p0.date.test_weekofyear.test_weekofyear",
    "nereids_p0.expression.operator_precedence.operator_precedence",
    "nereids_p0.legacy.test_clone_legacy_nullable_from_nereids.test_clone_legacy_nullable_from_nereids",
    "nereids_p0.sql_functions.datetime_functions.test_date_or_datetime_computation_negative.test_date_or_datetime_computation_negative",
    "nereids_p0.sql_functions.spatial_functions.test_st_astext.test_st_astext",
    "nereids_p0.sql_functions.string_functions.test_split_by_string.test_split_by_string",
    "nereids_p0.stats.col_stats.estimate_string_as_date.estimate_string_as_date",
    "nereids_p0.test_generate_subquery_output.test_generate_subquery_output",
    "nereids_p0.test_select_constant.test_select_constant",
    "query_p0.aggregate.aggregate_function_kurt.aggregate_function_kurt",
    "query_p0.aggregate.aggregate_function_skew.aggregate_function_skew",
    "query_p0.aggregate.support_type.avg_weighted.avg_weighted.avg_weighted",
    "query_p0.aggregate.support_type.corr.corr.corr",
    "query_p0.aggregate.support_type.corr_welford.corr_welford.corr_welford",
    "query_p0.aggregate.support_type.covar.covar.covar",
    "query_p0.aggregate.support_type.covar_samp.covar_samp.covar_samp",
    "query_p0.aggregate.support_type.regr_intercept.regr_intercept.regr_intercept",
    "query_p0.aggregate.support_type.regr_slope.regr_slope.regr_slope",
    "query_p0.join.left_anti_with_other.left_anti_with_other.left_anti_with_other",
    "query_p0.join.test_ignore.test_ignore.test_ignore",
    "query_p0.lateral_view.test_with_view.lateral_view_with_inline_view",
    "query_p0.sort_spill.sort_spill.sort_spill",
    "query_p0.sql_functions.spatial_functions.test_st_astext.test_st_astext",
    "query_p0.sql_functions.test_years_add.test_years_add",
];

#[test]
fn batch2_matches_the_reference() {
    let mut bad = Vec::new();
    for s in BATCH2 {
        let m = replay(&replays::load(s));
        if !m.is_empty() {
            bad.push(format!("{s}: {} mismatches, first: #{} {}", m.len(), m[0].query, m[0].what));
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}
