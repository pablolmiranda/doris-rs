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
    _fe: Arc<FakeFe>,
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
    Be { agent, load, query, tablets, _fe: fe, _tmp: tmp }
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

fn ordered(plan: &TPipelineFragmentParamsList) -> bool {
    plan.params_list.iter().flatten().filter_map(|p| p.fragment.as_ref()?.plan.as_ref()).flat_map(|p| &p.nodes).any(|n| {
        n.node_type == TPlanNodeType::SORT_NODE || n.exchange_node.as_ref().is_some_and(|e| e.sort_info.is_some())
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
        let m = compare(&r.suite, query, ordered(&plan), &got, &reference);
        self.out.extend(m);
    }
}

/// Runs a replay; every query whose rows differ from the reference's is a mismatch.
pub fn replay(r: &Replay) -> Vec<Mismatch> {
    let be = be();
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
    let mut d = Driver { r, be, plans, want, executed: HashSet::new(), started: HashSet::new(), drained: HashSet::new(), out: Vec::new() };
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
    d.out
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
