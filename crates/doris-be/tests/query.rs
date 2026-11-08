//! SELECT fragments through the query service: results by long-polled fetch_data, failures that
//! reach a parked fetch at once and the FE in reportExecStatus.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use doris_be::master::MasterInfo;
use doris_be::query::QueryService;
use doris_be::testing::{FakeFe, LoadCall};
use doris_fixtures::build::{col, create_tablet, exec_request, scan_query, tcolumn, template, ty, Col};
use doris_proto::doris::{PCacheStatus, PCancelPlanFragmentRequest, PExecPlanFragmentStartRequest, PFetchDataRequest, PFetchDataResult, PUniqueId, RowsetStatePb};
use doris_segment::writer::Datum;
use doris_storage::rowset::write_rowset;
use doris_storage::{DataDir, TabletManager, TabletOptions};
use doris_thrift::gen::data::TResultBatch;
use doris_thrift::gen::palo_internal_service::{TPipelineFragmentParams, TQueryType};
use doris_thrift::gen::plan_nodes::TPlanNodeType;
use doris_thrift::gen::types::{TKeysType, TPrimitiveType as T, TUniqueId};
use thrift::protocol::{TBinaryInputProtocol, TSerializable};

struct Env {
    query: QueryService,
    fe: Arc<FakeFe>,
    tablets: Arc<Mutex<TabletManager>>,
    _tmp: tempfile::TempDir,
}

fn env() -> Env {
    let tmp = tempfile::tempdir().unwrap();
    let tablets = Arc::new(Mutex::new(TabletManager::open(vec![DataDir::open(tmp.path(), "127.0.0.1").unwrap()], TabletOptions::default()).unwrap()));
    let fe = Arc::new(FakeFe::default());
    let query = QueryService::new(tablets.clone(), fe.clone(), MasterInfo::new("127.0.0.1"));
    Env { query, fe, tablets, _tmp: tmp }
}

fn columns() -> Vec<Col> {
    vec![col("k", ty(T::INT), false), col("v", ty(T::VARCHAR), true)]
}

/// A tablet of `columns()`: version 1 empty, version 2 holding `rows` (when there are any).
fn table(env: &Env, tablet_id: i64, rows: Vec<Vec<Datum>>) -> i64 {
    let (_, t) = template();
    let col0 = t.fragment.unwrap().plan.unwrap().nodes[0].olap_scan_node.clone().unwrap().columns_desc.unwrap()[0].clone();
    let tcols = columns().iter().enumerate().map(|(i, c)| tcolumn(&col0, i, c)).collect();
    let mut mgr = env.tablets.lock().unwrap();
    mgr.create_tablet(&create_tablet(tablet_id, tcols, TKeysType::DUP_KEYS, 1)).unwrap();
    if rows.is_empty() {
        return 1;
    }
    let meta = mgr.tablet(tablet_id).unwrap().clone();
    let dir = mgr.tablet_path(tablet_id).unwrap();
    let id = mgr.next_rowset_id();
    let mut rs = write_rowset(&dir, &meta, &id, 7, &PUniqueId { hi: 1, lo: 2 }, rows).unwrap().meta;
    rs.start_version = Some(2);
    rs.end_version = Some(2);
    rs.set_rowset_state(RowsetStatePb::Visible);
    mgr.add_visible_rowset(tablet_id, rs).unwrap();
    2
}

fn kv(n: i128) -> Vec<Vec<Datum>> {
    (0..n).map(|i| vec![Datum::Int(i), if i % 2 == 1 { Datum::Null } else { Datum::Bytes(format!("v{i}").into_bytes()) }]).collect()
}

fn qid(n: i64) -> TUniqueId {
    TUniqueId { hi: 42, lo: n * 100 }
}

fn fetch_req(q: &TUniqueId) -> PFetchDataRequest {
    PFetchDataRequest { finst_id: PUniqueId { hi: q.hi, lo: q.lo }, resp_in_attachment: Some(false) }
}

/// Every packet up to eos (or the first failure).
fn fetch_all(svc: &QueryService, q: &TUniqueId) -> Vec<PFetchDataResult> {
    let mut out = Vec::new();
    loop {
        let r = svc.fetch_data(&fetch_req(q));
        let end = r.eos == Some(true) || r.status.status_code != 0;
        out.push(r);
        if end || out.len() > 100 {
            return out;
        }
    }
}

/// The rows of data packets, each as its cells (None for NULL).
fn rows(packets: &[PFetchDataResult]) -> Vec<Vec<Option<String>>> {
    let mut out = Vec::new();
    for p in packets {
        let Some(b) = &p.row_batch else { continue };
        let mut s = b.as_slice();
        let batch = TResultBatch::read_from_in_protocol(&mut TBinaryInputProtocol::new(&mut s, true)).unwrap();
        for row in batch.rows {
            let mut cells = Vec::new();
            let mut r = row.as_slice();
            while let Some((&n, rest)) = r.split_first() {
                if n == 0xFB {
                    cells.push(None);
                    r = rest;
                } else {
                    // test rows are short: one-byte lengths
                    cells.push(Some(String::from_utf8(rest[..n as usize].to_vec()).unwrap()));
                    r = &rest[n as usize..];
                }
            }
            out.push(cells);
        }
    }
    out
}

fn prepared(mut p: TPipelineFragmentParams) -> TPipelineFragmentParams {
    p.need_wait_execution_trigger = Some(true);
    p
}

fn start(svc: &QueryService, q: &TUniqueId) {
    let r = svc.start(&PExecPlanFragmentStartRequest { query_id: Some(PUniqueId { hi: q.hi, lo: q.lo }) });
    assert_eq!(r.status.status_code, 0, "{:?}", r.status);
}

#[test]
fn select_star_returns_inserted_rows() {
    let env = env();
    let v = table(&env, 1001, kv(3));
    let q = qid(1);
    let r = env.query.exec_plan_fragment(&exec_request(vec![scan_query(q.clone(), 1001, v, &columns(), 4064)]));
    assert_eq!(r.status.status_code, 0, "{:?}", r.status);
    let packets = fetch_all(&env.query, &q);
    let last = packets.last().unwrap();
    assert_eq!(last.eos, Some(true), "{packets:?}");
    assert_eq!(last.query_statistics.as_ref().and_then(|s| s.returned_rows), Some(3));
    let mut got = rows(&packets);
    got.sort();
    let s = |x: &str| Some(x.to_string());
    assert_eq!(got, vec![vec![s("0"), s("v0")], vec![s("1"), None], vec![s("2"), s("v2")]]);
    // a successful query reports nothing
    assert!(env.fe.load_calls().is_empty());
}

#[test]
fn results_span_packets() {
    let env = env();
    let v = table(&env, 1002, kv(10_000));
    let q = qid(2);
    env.query.exec_plan_fragment(&exec_request(vec![scan_query(q.clone(), 1002, v, &columns(), 4064)]));
    let packets = fetch_all(&env.query, &q);
    let seqs: Vec<(i64, bool)> = packets.iter().map(|p| (p.packet_seq.unwrap(), p.eos.unwrap())).collect();
    assert_eq!(seqs, vec![(0, false), (1, false), (2, false), (3, true)]);
    assert_eq!(packets[3].query_statistics.as_ref().and_then(|s| s.returned_rows), Some(10_000));
    assert!(packets[3].row_batch.is_none());
    assert_eq!(rows(&packets).len(), 10_000);
}

#[test]
fn an_empty_result_is_one_eos_packet() {
    let env = env();
    let v = table(&env, 1003, Vec::new());
    let q = qid(3);
    env.query.exec_plan_fragment(&exec_request(vec![scan_query(q.clone(), 1003, v, &columns(), 4064)]));
    let packets = fetch_all(&env.query, &q);
    assert_eq!(packets.len(), 1, "{packets:?}");
    assert_eq!((packets[0].packet_seq, packets[0].eos), (Some(0), Some(true)));
    assert_eq!(packets[0].query_statistics.as_ref().and_then(|s| s.returned_rows), Some(0));
}

#[test]
fn fetch_before_start_waits() {
    let env = Arc::new(env());
    let v = table(&env, 1004, kv(2));
    let q = qid(4);
    let r = env.query.prepare(&exec_request(vec![prepared(scan_query(q.clone(), 1004, v, &columns(), 4064))]));
    assert_eq!(r.status.status_code, 0, "{:?}", r.status);
    let (e2, q2) = (env.clone(), q.clone());
    let fetch = std::thread::spawn(move || fetch_all(&e2.query, &q2));
    std::thread::sleep(Duration::from_millis(200));
    assert!(!fetch.is_finished(), "the fetch answered before start");
    start(&env.query, &q);
    let packets = fetch.join().unwrap();
    assert_eq!(rows(&packets).len(), 2);
}

#[test]
fn a_failing_scan_fails_the_parked_fetch() {
    let env = Arc::new(env());
    table(&env, 1005, kv(2));
    let q = qid(5);
    // version 5 is past the tablet's version 2
    env.query.prepare(&exec_request(vec![prepared(scan_query(q.clone(), 1005, 5, &columns(), 4064))]));
    let (e2, q2) = (env.clone(), q.clone());
    let fetch = std::thread::spawn(move || e2.query.fetch_data(&fetch_req(&q2)));
    std::thread::sleep(Duration::from_millis(100));
    let t = Instant::now();
    start(&env.query, &q);
    let r = fetch.join().unwrap();
    assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
    assert_ne!(r.status.status_code, 0);
    let msg = r.status.error_msgs.join(" ");
    assert!(msg.contains("fail to find path in version_graph"), "{msg}");
    assert!(r.eos.is_none() && r.row_batch.is_none());
    // one report per fragment, done
    let deadline = Instant::now() + Duration::from_secs(2);
    while env.fe.load_calls().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let calls = env.fe.load_calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    let LoadCall::ExecStatus(_, p) = &calls[0] else { panic!("{calls:?}") };
    assert_eq!(p.done, Some(true));
    assert_eq!(p.query_type, Some(TQueryType::SELECT));
    assert_eq!(p.fragment_id, Some(0));
    assert_eq!(p.backend_num, Some(-1));
    assert_eq!(p.query_id.as_ref(), Some(&q));
    assert!(p.status.as_ref().is_some_and(|s| s.status_code != doris_thrift::gen::status::TStatusCode::OK));
}

#[test]
fn unsupported_plans_are_refused_at_prepare() {
    let env = env();
    let v = table(&env, 1006, kv(2));
    let q = qid(6);
    let mut p = prepared(scan_query(q.clone(), 1006, v, &columns(), 4064));
    p.fragment.as_mut().unwrap().plan.as_mut().unwrap().nodes[0].node_type = TPlanNodeType::HASH_JOIN_NODE;
    let r = env.query.prepare(&exec_request(vec![p]));
    assert_ne!(r.status.status_code, 0);
    assert!(r.status.error_msgs.iter().any(|m| m.contains("HASH_JOIN_NODE")), "{:?}", r.status);
    // nothing runs: there is no query to fetch from
    assert_ne!(env.query.fetch_data(&fetch_req(&q)).status.status_code, 0);
}

#[test]
fn a_fetch_for_an_unknown_query_fails() {
    let env = env();
    let r = env.query.fetch_data(&fetch_req(&qid(99)));
    assert_ne!(r.status.status_code, 0);
}

#[test]
fn cancel_and_cache_rpcs_answer() {
    let env = env();
    let v = table(&env, 1007, kv(2));
    let q = qid(7);
    env.query.exec_plan_fragment(&exec_request(vec![scan_query(q.clone(), 1007, v, &columns(), 4064)]));
    fetch_all(&env.query, &q);
    let c = env.query.cancel(&PCancelPlanFragmentRequest {
        finst_id: PUniqueId { hi: 0, lo: 0 },
        query_id: Some(PUniqueId { hi: q.hi, lo: q.lo }),
        ..Default::default()
    });
    assert_eq!(c.status.status_code, 0);
    assert_eq!(env.query.update_cache().status(), PCacheStatus::CacheOk);
    assert_eq!(env.query.fetch_cache().status(), PCacheStatus::NoSqlKey);
}

fn cancel_with(q: &TUniqueId, code: i32) -> PCancelPlanFragmentRequest {
    PCancelPlanFragmentRequest {
        finst_id: PUniqueId { hi: 0, lo: 0 },
        query_id: Some(PUniqueId { hi: q.hi, lo: q.lo }),
        cancel_status: Some(doris_proto::doris::PStatus { status_code: code, error_msgs: vec!["the FE cancelled it".into()] }),
        ..Default::default()
    }
}

#[test]
fn a_finished_query_leaves_no_state() {
    let env = env();
    let v = table(&env, 1008, kv(2));
    let q = qid(8);
    env.query.exec_plan_fragment(&exec_request(vec![scan_query(q.clone(), 1008, v, &columns(), 4064)]));
    assert_eq!(fetch_all(&env.query, &q).last().unwrap().eos, Some(true));
    // a LIMIT_REACH cancel after the query ended (the FE had the rows it needed)
    env.query.cancel(&cancel_with(&q, doris_thrift::gen::status::TStatusCode::LIMIT_REACH.0));
    assert_eq!(env.query.live_state(), (0, 0));
}

#[test]
fn a_failed_query_leaves_no_state() {
    let env = env();
    table(&env, 1009, kv(2));
    let q = qid(9);
    env.query.exec_plan_fragment(&exec_request(vec![scan_query(q.clone(), 1009, 5, &columns(), 4064)]));
    assert_ne!(env.query.fetch_data(&fetch_req(&q)).status.status_code, 0);
    // the FE cancels a failed query with its error
    env.query.cancel(&cancel_with(&q, 6));
    assert_eq!(env.query.live_state(), (0, 0));
}

#[test]
fn a_repeated_prepare_is_refused() {
    let env = env();
    let v = table(&env, 1010, kv(2));
    let q = qid(10);
    let req = exec_request(vec![prepared(scan_query(q.clone(), 1010, v, &columns(), 4064))]);
    assert_eq!(env.query.prepare(&req).status.status_code, 0);
    // as FragmentMgr::exec_plan_fragment: one (query, fragment) per BE
    let again = env.query.prepare(&req);
    assert_ne!(again.status.status_code, 0);
    let msg = again.status.error_msgs.join(" ");
    assert!(msg.contains("input duplicated fragment_id"), "{msg}");
    // the first query is untouched
    start(&env.query, &q);
    assert_eq!(rows(&fetch_all(&env.query, &q)).len(), 2);
}

#[test]
fn exchange_sender_counts_must_match_the_plan() {
    use doris_fixtures::replays::{self, plan_list, shape, Step};
    // a two-fragment corpus plan: SORT(OLAP_SCAN) -> merge | EXCHANGE -> RESULT
    let (req, list) = replays::all()
        .iter()
        .flat_map(|r| r.steps.clone())
        .find_map(|s| match s {
            Step::ExecPlanFragment { request, .. } => plan_list(&request).filter(|p| shape(p) == "SORT(OLAP_SCAN)->DS(merge) | EXCHANGE->RESULT").map(|p| (request, p)),
            _ => None,
        })
        .expect("a shape B plan");
    let encode = |list: &doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList| {
        let mut r = req.clone();
        r.request = exec_request(list.params_list.clone().unwrap()).request;
        r
    };
    for change in [0, 1] {
        let env = env();
        let mut l = list.clone();
        let receiver = l.params_list.as_mut().unwrap().iter_mut().find(|p| !p.per_exch_num_senders.is_empty()).unwrap();
        if change == 0 {
            receiver.per_exch_num_senders.clear();
        } else {
            receiver.per_exch_num_senders.values_mut().for_each(|n| *n += 1);
        }
        let r = env.query.prepare(&encode(&l));
        assert_ne!(r.status.status_code, 0, "change {change} was accepted");
        assert!(r.status.error_msgs.iter().any(|m| m.contains("sender")), "{:?}", r.status);
    }
    // the plan as the FE sent it is accepted
    assert_eq!(env().query.prepare(&encode(&list)).status.status_code, 0);
}
