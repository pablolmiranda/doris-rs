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

#[test]
fn multicast_sender_counts_must_match_the_plan() {
    use doris_fixtures::replays::{self, plan_list, Step};
    use doris_thrift::gen::data_sinks::TDataSinkType;
    let is_multicast = |p: &doris_thrift::gen::palo_internal_service::TPipelineFragmentParams| {
        p.fragment.as_ref().and_then(|f| f.output_sink.as_ref()).is_some_and(|s| s.type_ == TDataSinkType::MULTI_CAST_DATA_STREAM_SINK)
    };
    let (req, list) = replays::load("correctness_p0.test_subquery_grouping.test_subquery_grouping")
        .steps
        .iter()
        .find_map(|s| match s {
            Step::ExecPlanFragment { request, .. } => plan_list(request)
                .filter(|l| l.params_list.iter().flatten().any(is_multicast))
                .map(|l| (request.clone(), l)),
            _ => None,
        })
        .expect("a multicast plan");
    let encode = |list: &doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList| {
        let mut r = req.clone();
        r.request = exec_request(list.params_list.clone().unwrap()).request;
        r
    };
    // the exchanges a consumer feeds
    let fed: Vec<i32> = list
        .params_list
        .iter()
        .flatten()
        .filter(|p| is_multicast(p))
        .flat_map(|p| p.fragment.as_ref().unwrap().output_sink.as_ref().unwrap().multi_cast_stream_sink.as_ref().unwrap().sinks.clone().unwrap_or_default())
        .map(|s| s.dest_node_id)
        .collect();
    assert!(!fed.is_empty());
    let mut l = list.clone();
    let receiver = l.params_list.as_mut().unwrap().iter_mut().find(|p| fed.iter().any(|n| p.per_exch_num_senders.contains_key(n))).expect("a consumer");
    receiver.per_exch_num_senders.retain(|n, _| !fed.contains(n));
    let r = env().query.prepare(&encode(&l));
    assert_ne!(r.status.status_code, 0, "a missing sender count was accepted");
    assert!(r.status.error_msgs.iter().any(|m| m.contains("sender")), "{:?}", r.status);
    let r = env().query.prepare(&encode(&list));
    assert_eq!(r.status.status_code, 0, "{:?}", r.status);
}

#[test]
fn a_multicast_consumer_must_be_runnable() {
    use doris_fixtures::replays::{self, plan_list, Step};
    use doris_thrift::gen::data_sinks::{TDataSinkType, TMultiCastDataStreamSink};
    use doris_thrift::gen::partitions::TPartitionType;
    let (req, list) = replays::load("correctness_p0.test_subquery_grouping.test_subquery_grouping")
        .steps
        .iter()
        .find_map(|s| match s {
            Step::ExecPlanFragment { request, .. } => plan_list(request)
                .filter(|l| {
                    l.params_list.iter().flatten().any(|p| {
                        p.fragment.as_ref().and_then(|f| f.output_sink.as_ref()).is_some_and(|s| s.type_ == TDataSinkType::MULTI_CAST_DATA_STREAM_SINK)
                    })
                })
                .map(|l| (request.clone(), l)),
            _ => None,
        })
        .expect("a multicast plan");
    let refused = |change: &dyn Fn(&mut TMultiCastDataStreamSink), want: &str| {
        let mut l = list.clone();
        let m = l
            .params_list
            .as_mut()
            .unwrap()
            .iter_mut()
            .filter_map(|p| p.fragment.as_mut().and_then(|f| f.output_sink.as_mut()).and_then(|s| s.multi_cast_stream_sink.as_mut()))
            .next()
            .unwrap();
        change(m);
        let mut r = req.clone();
        r.request = exec_request(l.params_list.clone().unwrap()).request;
        let st = env().query.prepare(&r).status;
        assert_ne!(st.status_code, 0, "{want}: accepted");
        assert!(st.error_msgs.iter().any(|e| e.contains(want)), "{want}: {st:?}");
    };
    refused(&|m| m.sinks.as_mut().unwrap()[0].output_partition.type_ = TPartitionType::RANGE_PARTITIONED, "a multicast consumer partitioned as");
    refused(
        &|m| {
            let s = &mut m.sinks.as_mut().unwrap()[0];
            s.output_exprs = Some(vec![doris_fixtures::build::int(1, doris_fixtures::build::ty(doris_thrift::gen::types::TPrimitiveType::INT))]);
            s.output_tuple_id = None;
        },
        "output tuple",
    );
    refused(&|m| m.destinations.as_mut().unwrap().truncate(0), "multicast sink with");
}

/// A set_with_null plan whose set node (`node_type`) runs in 4 instances, with a function that
/// changes the plan (the set node, then the whole list) and prepares it.
fn set_plan_prepare(node_type: TPlanNodeType) -> impl Fn(&dyn Fn(&mut doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList)) -> doris_proto::doris::PStatus {
    use doris_fixtures::replays::{self, plan_list, Step};
    let has = move |l: &doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList| {
        l.params_list.iter().flatten().any(|p| {
            p.local_params.as_ref().is_some_and(|l| l.len() > 1)
                && p.fragment.iter().flat_map(|f| f.plan.iter()).flat_map(|x| &x.nodes).any(|n| n.node_type == node_type)
        })
    };
    let (req, list) = replays::load("query_p0.set_operations.set_with_null.set_with_null.set_with_null")
        .steps
        .iter()
        .find_map(|s| match s {
            Step::ExecPlanFragment { request, .. } => plan_list(request).filter(has).map(|l| (request.clone(), l)),
            _ => None,
        })
        .expect("a set plan in 4 instances");
    move |change| {
        let mut l = list.clone();
        change(&mut l);
        let mut r = req.clone();
        r.request = exec_request(l.params_list.clone().unwrap()).request;
        env().query.prepare(&r).status
    }
}

fn set_node_of(l: &mut doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList, t: TPlanNodeType) -> &mut doris_thrift::gen::plan_nodes::TPlanNode {
    l.params_list.as_mut().unwrap().iter_mut().flat_map(|p| p.fragment.as_mut().unwrap().plan.as_mut().unwrap().nodes.iter_mut()).find(|n| n.node_type == t).unwrap()
}

fn set_lists(n: &mut doris_thrift::gen::plan_nodes::TPlanNode) -> &mut Vec<Vec<doris_thrift::gen::exprs::TExpr>> {
    match (&mut n.intersect_node, &mut n.except_node) {
        (Some(s), _) => &mut s.result_expr_lists,
        (_, Some(s)) => &mut s.result_expr_lists,
        _ => panic!("not a set node"),
    }
}

#[test]
fn a_set_node_must_be_well_formed() {
    use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList as L;
    use doris_thrift::gen::partitions::TPartitionType;
    for t in [TPlanNodeType::INTERSECT_NODE, TPlanNodeType::EXCEPT_NODE] {
        let name = format!("{t:?}");
        let prepare = set_plan_prepare(t);
        let st = prepare(&|_| {});
        assert_eq!(st.status_code, 0, "{name}: {st:?}");
        let refused = |change: &dyn Fn(&mut L), want: &str| {
            let st = prepare(change);
            assert_ne!(st.status_code, 0, "{name}, {want}: accepted");
            assert!(st.error_msgs.iter().any(|e| e.contains(want)), "{name}, {want}: {st:?}");
        };
        let node = |change: &'static dyn Fn(&mut doris_thrift::gen::plan_nodes::TPlanNode)| move |l: &mut L| change(set_node_of(l, t));
        refused(&node(&|n| { set_lists(n).pop(); }), "result expr lists");
        refused(&node(&|n| { set_lists(n)[0].pop(); }), "different lengths");
        refused(
            &node(&|n| {
                n.num_children = 1;
                set_lists(n).truncate(1);
            }),
            "a set node with 1 child; it needs at least 2",
        );
        refused(&node(&|n| set_lists(n)[1][0] = doris_fixtures::build::call("no_such_function", ty(T::INT), vec![])), "no_such_function");
        refused(
            &node(&|n| {
                n.intersect_node = None;
                n.except_node = None;
            }),
            "without its parameters",
        );
        // the lists must fill the node's tuple, with one expr at least
        refused(&node(&|n| set_lists(n).iter_mut().for_each(|l| l.push(l[0].clone()))), "for a tuple of");
        refused(&node(&|n| set_lists(n).iter_mut().for_each(|l| l.clear())), "no result exprs");
        // a child's values must have the slot's type (C++ probes them with child 0's hash method)
        refused(&node(&|n| set_lists(n)[1][0].nodes[0].type_ = doris_fixtures::build::ty(T::LARGEINT)), "for a slot of type");
        // in several instances, equal rows must meet: every child an exchange, child 0's hash partitioned
        refused(
            &node(&|n| match (&mut n.intersect_node, &mut n.except_node) {
                (Some(s), _) => s.is_colocate = Some(true),
                (_, Some(s)) => s.is_colocate = Some(true),
                _ => {}
            }),
            "colocated",
        );
        refused(
            &|l: &mut L| {
                let first_child = {
                    let ps = l.params_list.as_mut().unwrap();
                    let nodes = ps.iter_mut().map(|p| p.fragment.as_mut().unwrap().plan.as_mut().unwrap()).find(|p| p.nodes.iter().any(|n| n.node_type == t)).unwrap();
                    let at = nodes.nodes.iter().position(|n| n.node_type == t).unwrap();
                    nodes.nodes[at + 1].node_id
                };
                for p in l.params_list.as_mut().unwrap() {
                    if let Some(s) = p.fragment.as_mut().unwrap().output_sink.as_mut().and_then(|s| s.stream_sink.as_mut()) {
                        if s.dest_node_id == first_child {
                            s.output_partition.type_ = TPartitionType::RANDOM;
                        }
                    }
                }
            },
            "fed by a RANDOM stream",
        );
        refused(
            &|l: &mut L| {
                let first_child = {
                    let ps = l.params_list.as_mut().unwrap();
                    let nodes = ps.iter_mut().map(|p| p.fragment.as_mut().unwrap().plan.as_mut().unwrap()).find(|p| p.nodes.iter().any(|n| n.node_type == t)).unwrap();
                    let at = nodes.nodes.iter().position(|n| n.node_type == t).unwrap();
                    nodes.nodes[at + 1].node_id
                };
                for p in l.params_list.as_mut().unwrap() {
                    if let Some(s) = p.fragment.as_mut().unwrap().output_sink.as_mut().and_then(|s| s.stream_sink.as_mut()) {
                        if s.dest_node_id == first_child {
                            s.output_partition.partition_exprs = Some(vec![doris_fixtures::build::int(1, ty(T::INT))]);
                        }
                    }
                }
            },
            "hashed on other exprs than its keys",
        );
    }
}

#[test]
fn a_nested_set_node_must_be_keyed_on_its_output() {
    // test_set_operation: INTERSECT(EXCHANGE, EXCEPT(EXCHANGE, EXCHANGE)) in 4 instances; the
    // INTERSECT keys its EXCEPT child on slot 6, the EXCEPT's own; slot 5 is not
    use doris_fixtures::replays::{self, plan_list, Step};
    let (req, list) = replays::load("correctness_p0.test_set_operation.test_set_operation")
        .steps
        .iter()
        .find_map(|s| match s {
            Step::ExecPlanFragment { request, .. } => plan_list(request)
                .filter(|l| {
                    l.params_list.iter().flatten().any(|p| {
                        let nodes = &p.fragment.as_ref().unwrap().plan.as_ref().unwrap().nodes;
                        nodes.iter().any(|n| n.intersect_node.is_some()) && nodes.iter().any(|n| n.except_node.is_some())
                    })
                })
                .map(|l| (request.clone(), l)),
            _ => None,
        })
        .expect("a nested set plan");
    let prepare = |l: &doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList| {
        let mut r = req.clone();
        r.request = exec_request(l.params_list.clone().unwrap()).request;
        env().query.prepare(&r).status
    };
    assert_eq!(prepare(&list).status_code, 0);
    let mut l = list.clone();
    let n = set_node_of(&mut l, TPlanNodeType::INTERSECT_NODE);
    let e = &mut set_lists(n)[1][0].nodes[0];
    assert_eq!(e.slot_ref.as_ref().map(|r| r.slot_id), Some(6));
    e.slot_ref.as_mut().unwrap().slot_id = 5;
    let st = prepare(&l);
    assert_ne!(st.status_code, 0, "accepted");
    assert!(st.error_msgs.iter().any(|m| m.contains("over a set node on other keys in 4 instances")), "{st:?}");
}

#[test]
fn a_set_node_needs_its_children_to_be_exchanges_in_several_instances() {
    // a local child in a 4-instance fragment: each instance would see only its own scan ranges
    let prepare = set_plan_prepare(TPlanNodeType::INTERSECT_NODE);
    let st = prepare(&|l| {
        let ps = l.params_list.as_mut().unwrap();
        let plan = ps.iter_mut().map(|p| p.fragment.as_mut().unwrap().plan.as_mut().unwrap()).find(|p| p.nodes.iter().any(|n| n.node_type == TPlanNodeType::INTERSECT_NODE)).unwrap();
        let at = plan.nodes.iter().position(|n| n.node_type == TPlanNodeType::INTERSECT_NODE).unwrap();
        let child = &mut plan.nodes[at + 1];
        child.node_type = TPlanNodeType::EMPTY_SET_NODE;
        child.exchange_node = None;
    });
    assert_ne!(st.status_code, 0, "accepted");
    assert!(st.error_msgs.iter().any(|e| e.contains("over an EMPTY_SET_NODE in 4 instances")), "{st:?}");
}

/// A check_partitionkey plan whose window node runs in 4 instances (over a sort and a hashed
/// exchange, with no partition sort), with a function that changes the plan and prepares it.
fn window_plan_prepare() -> impl Fn(&dyn Fn(&mut doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList)) -> doris_proto::doris::PStatus {
    window_plan_prepare_in("nereids_p0.partition_topn.check_partitionkey.check_partitionkey", TPlanNodeType::ANALYTIC_EVAL_NODE, 2, true)
}

/// A plan of `suite` with a `node` in at least `instances` instances (and, if `no_partition_sort`,
/// no PARTITION_SORT_NODE), with a function that changes the plan and prepares it.
fn window_plan_prepare_in(
    suite: &str,
    node: TPlanNodeType,
    instances: usize,
    no_partition_sort: bool,
) -> impl Fn(&dyn Fn(&mut doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList)) -> doris_proto::doris::PStatus {
    use doris_fixtures::replays::{self, plan_list, Step};
    let nodes = |l: &doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList| {
        l.params_list.iter().flatten().flat_map(|p| p.fragment.iter().flat_map(|f| f.plan.iter()).flat_map(|x| x.nodes.clone()).map(move |n| (p.local_params.as_ref().map_or(0, |l| l.len()), n))).collect::<Vec<_>>()
    };
    let wanted = move |l: &doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList| {
        let ns = nodes(l);
        ns.iter().any(|(k, n)| *k >= instances && n.node_type == node) && !(no_partition_sort && ns.iter().any(|(_, n)| n.node_type == TPlanNodeType::PARTITION_SORT_NODE))
    };
    let (req, list) = replays::load(suite)
        .steps
        .iter()
        .find_map(|s| match s {
            Step::ExecPlanFragment { request, .. } => plan_list(request).filter(wanted).map(|l| (request.clone(), l)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{suite}: a {node:?} plan in {instances} instances"));
    move |change| {
        let mut l = list.clone();
        change(&mut l);
        let mut r = req.clone();
        r.request = exec_request(l.params_list.clone().unwrap()).request;
        env().query.prepare(&r).status
    }
}

fn window_node_of(l: &mut doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList) -> &mut doris_thrift::gen::plan_nodes::TPlanNode {
    set_node_of(l, TPlanNodeType::ANALYTIC_EVAL_NODE)
}

fn window_of(n: &mut doris_thrift::gen::plan_nodes::TPlanNode) -> &mut doris_thrift::gen::plan_nodes::TAnalyticNode {
    n.analytic_node.as_mut().unwrap()
}

type Prepare = dyn Fn(&dyn Fn(&mut doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList)) -> doris_proto::doris::PStatus;

fn refused_by(prepare: &Prepare, change: &dyn Fn(&mut doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList), want: &str) {
    let st = prepare(change);
    assert_ne!(st.status_code, 0, "{want}: accepted");
    assert!(st.error_msgs.iter().any(|e| e.contains(want)), "{want}: {st:?}");
}

#[test]
fn a_window_node_must_be_well_formed() {
    use doris_thrift::gen::plan_nodes::{TAnalyticWindow, TAnalyticWindowBoundary, TAnalyticWindowBoundaryType as B, TAnalyticWindowType as W};
    let prepare = window_plan_prepare();
    let st = prepare(&|_| {});
    assert_eq!(st.status_code, 0, "{st:?}");
    let name = |n: &mut doris_thrift::gen::plan_nodes::TPlanNode, f: &str| {
        window_of(n).analytic_functions[0].nodes[0].fn_.as_mut().unwrap().name.function_name = f.into();
    };
    refused_by(&prepare, &|l| window_node_of(l).analytic_node = None, "ANALYTIC_EVAL_NODE without its parameters");
    refused_by(&prepare, &|l| name(window_node_of(l), "no_such_function"), "function no_such_function");
    refused_by(
        &prepare,
        &|l| {
            let b = |t, v| TAnalyticWindowBoundary { type_: t, range_offset_predicate: None, rows_offset_value: v };
            window_of(window_node_of(l)).window = Some(TAnalyticWindow { type_: W::RANGE, window_start: Some(b(B::PRECEDING, Some(1))), window_end: Some(b(B::CURRENT_ROW, None)) });
        },
        "a RANGE window with offsets",
    );
    // the FE forces each rank-family function's frame; any other is refused, not guessed
    refused_by(&prepare, &|l| name(window_node_of(l), "rank"), "rank over a ROWS window");
    refused_by(&prepare, &|l| name(window_node_of(l), "lead"), "a lead offset that is not a non-negative literal");
    refused_by(
        &prepare,
        &|l| {
            let w = window_of(window_node_of(l));
            w.analytic_functions.push(w.analytic_functions[0].clone());
        },
        "for a tuple of",
    );
}

#[test]
fn a_window_node_must_be_runnable() {
    use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList as L;
    use doris_thrift::gen::partitions::TPartitionType;
    let prepare = window_plan_prepare();
    refused_by(&prepare, &|l| window_of(window_node_of(l)).partition_exprs.clear(), "an ANALYTIC_EVAL_NODE without partition keys in 4 instances");
    refused_by(
        &prepare,
        &|l| {
            let w = window_of(window_node_of(l));
            w.partition_exprs = w.order_by_exprs.clone();
        },
        "an ANALYTIC_EVAL_NODE hashed on other exprs than its partition keys in 4 instances",
    );
    // the window's sort sits on an exchange; its feeding sinks send at random
    let random = |l: &mut L| {
        let exchange = {
            let plan = l.params_list.as_mut().unwrap().iter_mut().map(|p| p.fragment.as_mut().unwrap().plan.as_mut().unwrap()).find(|p| p.nodes.iter().any(|n| n.node_type == TPlanNodeType::ANALYTIC_EVAL_NODE)).unwrap();
            let at = plan.nodes.iter().position(|n| n.node_type == TPlanNodeType::ANALYTIC_EVAL_NODE).unwrap();
            plan.nodes[at..].iter().find(|n| n.node_type == TPlanNodeType::EXCHANGE_NODE).unwrap().node_id
        };
        for p in l.params_list.as_mut().unwrap() {
            if let Some(s) = p.fragment.as_mut().unwrap().output_sink.as_mut().and_then(|s| s.stream_sink.as_mut()) {
                if s.dest_node_id == exchange {
                    s.output_partition.type_ = TPartitionType::RANDOM;
                }
            }
        }
    };
    refused_by(&prepare, &random, "an ANALYTIC_EVAL_NODE fed by a RANDOM stream in 4 instances");
}

/// A check_partitionkey plan with a PARTITION_SORT_NODE in 4 instances, with a function that
/// changes the plan and prepares it.
fn partition_sort_plan_prepare() -> impl Fn(&dyn Fn(&mut doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList)) -> doris_proto::doris::PStatus {
    use doris_fixtures::replays::{self, plan_list, Step};
    let wanted = |l: &doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList| {
        l.params_list.iter().flatten().any(|p| {
            p.local_params.as_ref().is_some_and(|l| l.len() > 1)
                && p.fragment.iter().flat_map(|f| f.plan.iter()).flat_map(|x| &x.nodes).any(|n| n.node_type == TPlanNodeType::PARTITION_SORT_NODE)
        })
    };
    let (req, list) = replays::load("nereids_p0.partition_topn.check_partitionkey.check_partitionkey")
        .steps
        .iter()
        .find_map(|s| match s {
            Step::ExecPlanFragment { request, .. } => plan_list(request).filter(wanted).map(|l| (request.clone(), l)),
            _ => None,
        })
        .expect("a partition sort plan in 4 instances");
    move |change| {
        let mut l = list.clone();
        change(&mut l);
        let mut r = req.clone();
        r.request = exec_request(l.params_list.clone().unwrap()).request;
        env().query.prepare(&r).status
    }
}

#[test]
fn a_partition_sort_node_must_be_well_formed() {
    use doris_thrift::gen::plan_nodes::TPartTopNPhase;
    let prepare = partition_sort_plan_prepare();
    let st = prepare(&|_| {});
    assert_eq!(st.status_code, 0, "{st:?}");
    fn ps(l: &mut doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList) -> &mut doris_thrift::gen::plan_nodes::TPlanNode {
        set_node_of(l, TPlanNodeType::PARTITION_SORT_NODE)
    }
    refused_by(&prepare, &|l| ps(l).partition_sort_node = None, "PARTITION_SORT_NODE without its parameters");
    refused_by(&prepare, &|l| ps(l).partition_sort_node.as_mut().unwrap().sort_info = None, "a PARTITION_SORT_NODE without its sort");
    refused_by(&prepare, &|l| ps(l).partition_sort_node.as_mut().unwrap().partition_inner_limit = Some(-1), "without a limit of 0 or more");
    refused_by(&prepare, &|l| ps(l).partition_sort_node.as_mut().unwrap().top_n_algorithm = None, "without its algorithm");
    // the global phase's output must be exact and sorted per partition across all instances;
    // no eligible suite has one
    refused_by(
        &prepare,
        &|l| ps(l).partition_sort_node.as_mut().unwrap().ptopn_phase = Some(TPartTopNPhase::TWO_PHASE_GLOBAL),
        "a global PARTITION_SORT_NODE",
    );
    // in one instance too: the window reads it with no sort between, and C++ emits partitions
    // in an order doris-rs does not reproduce
    let one = window_plan_prepare_in("query_p0.sql_functions.window_functions.test_partition_topn.test_partition_topn", TPlanNodeType::PARTITION_SORT_NODE, 1, false);
    assert_eq!(one(&|_| {}).status_code, 0);
    refused_by(&one, &|l| ps(l).partition_sort_node.as_mut().unwrap().ptopn_phase = Some(TPartTopNPhase::TWO_PHASE_GLOBAL), "a global PARTITION_SORT_NODE");
}

#[test]
fn a_colocated_window_node_needs_partition_keys_in_several_instances() {
    // test_min_max_window's window node runs colocated in its 4-instance scan fragment
    let prepare = window_plan_prepare_in("correctness_p0.test_min_max_window.test_min_max_window", TPlanNodeType::ANALYTIC_EVAL_NODE, 2, true);
    assert_eq!(prepare(&|_| {}).status_code, 0);
    refused_by(&prepare, &|l| window_of(window_node_of(l)).partition_exprs.clear(), "an ANALYTIC_EVAL_NODE without partition keys in 4 instances");
}

#[test]
fn a_window_over_an_exchange_without_senders_is_refused() {
    let prepare = window_plan_prepare();
    let orphan = |l: &mut doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList| {
        let exchange = {
            let plan = l.params_list.as_mut().unwrap().iter_mut().map(|p| p.fragment.as_mut().unwrap().plan.as_mut().unwrap()).find(|p| p.nodes.iter().any(|n| n.node_type == TPlanNodeType::ANALYTIC_EVAL_NODE)).unwrap();
            let at = plan.nodes.iter().position(|n| n.node_type == TPlanNodeType::ANALYTIC_EVAL_NODE).unwrap();
            plan.nodes[at..].iter().find(|n| n.node_type == TPlanNodeType::EXCHANGE_NODE).unwrap().node_id
        };
        for p in l.params_list.as_mut().unwrap() {
            if let Some(s) = p.fragment.as_mut().unwrap().output_sink.as_mut().and_then(|s| s.stream_sink.as_mut()) {
                if s.dest_node_id == exchange {
                    s.dest_node_id = 9999;
                }
            }
        }
    };
    // check_senders refuses it before the window's walk would see an exchange fed by nothing
    refused_by(&prepare, &orphan, "senders, the plan has 0");
}

/// A suite's first INSERT … SELECT request (a plan with an OLAP_TABLE_SINK fed by a query).
fn insert_select(suite: &str) -> (doris_fixtures::replays::Rpc, doris_proto::doris::PExecPlanFragmentRequest, doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList) {
    use doris_fixtures::replays::{self, plan_list, Step};
    use doris_thrift::gen::data_sinks::TDataSinkType;
    replays::load(suite)
        .steps
        .iter()
        .find_map(|s| match s {
            Step::ExecPlanFragment { rpc, request, .. } => plan_list(request)
                .filter(|l| {
                    let sinks = || l.params_list.iter().flatten().filter_map(|p| p.fragment.as_ref()?.output_sink.as_ref());
                    sinks().any(|s| s.type_ == TDataSinkType::OLAP_TABLE_SINK)
                        && l.params_list.iter().flatten().flat_map(|p| p.fragment.iter().flat_map(|f| f.plan.iter()).flat_map(|x| &x.nodes)).any(|n| n.node_type == TPlanNodeType::OLAP_SCAN_NODE)
                })
                .map(|l| (*rpc, request.clone(), l)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{suite}: no INSERT … SELECT"))
}

/// An instance that fails (here the scan: its tablet is not on this BE) fails the write: nothing
/// is committed, and every fragment reports the error, as a LOAD, with no commit infos.
#[test]
fn a_failed_instance_commits_nothing() {
    let (_, req, list) = insert_select("insert_p0.test_insert_docs_demo.test_insert_docs_demo");
    let env = env();
    assert_eq!(env.query.exec_plan_fragment(&req).status.status_code, 0);
    assert!(env.query.wait_idle(Duration::from_secs(30)));
    let reports: Vec<_> = env
        .fe
        .load_calls()
        .into_iter()
        .filter_map(|c| match c {
            LoadCall::ExecStatus(_, p) => Some(p),
            _ => None,
        })
        .collect();
    assert_eq!(reports.len(), list.params_list.as_ref().unwrap().len());
    for p in &reports {
        assert_eq!(p.query_type, Some(TQueryType::LOAD));
        assert_ne!(p.status.as_ref().unwrap().status_code, doris_thrift::gen::status::TStatusCode::OK, "{p:?}");
        assert_eq!(p.commit_infos, None);
        assert_eq!(p.done, Some(true));
    }
    // and the write is forgotten: nothing fetches it
    assert_eq!(env.query.live_state(), (0, 0));
}

/// The FE's own statistics DELETE after a TRUNCATE (a partial update with the delete sign into
/// the merge-on-write statistics table) is refused at prepare, naming the partial update.
#[test]
fn the_statistics_delete_is_refused_naming_partial_update() {
    let (_, req, list) = insert_select("query_p0.keyword.test_role_mapping_keyword.test_role_mapping_keyword");
    let mut l = list.clone();
    for p in l.params_list.iter_mut().flatten() {
        if let Some(s) = p.fragment.as_mut().and_then(|f| f.output_sink.as_mut()).and_then(|s| s.olap_table_sink.as_mut()) {
            s.schema.is_partial_update = Some(true);
        }
    }
    let mut r = req.clone();
    r.request = exec_request(l.params_list.clone().unwrap()).request;
    let st = env().query.prepare(&r).status;
    assert_eq!(st.status_code, 3, "{st:?}");
    assert!(st.error_msgs.iter().any(|e| e.contains("partial update")), "{st:?}");
}

/// What the table sink cannot route (a distribution column the sink does not write) is refused at
/// prepare, not found once the write runs.
#[test]
fn a_table_sink_it_cannot_route_is_refused_at_prepare() {
    let (_, req, list) = insert_select("query_p0.keyword.test_role_mapping_keyword.test_role_mapping_keyword");
    let mut l = list.clone();
    for p in l.params_list.iter_mut().flatten() {
        if let Some(s) = p.fragment.as_mut().and_then(|f| f.output_sink.as_mut()).and_then(|s| s.olap_table_sink.as_mut()) {
            s.partition.distributed_columns = Some(vec!["no_such_column".into()]);
        }
    }
    let mut r = req.clone();
    r.request = exec_request(l.params_list.clone().unwrap()).request;
    let st = env().query.prepare(&r).status;
    assert_ne!(st.status_code, 0, "{st:?}");
    assert!(st.error_msgs.iter().any(|e| e.contains("no_such_column")), "{st:?}");
}

/// INSERT … VALUES is the load service's (`exec_plan_fragment`); sent to prepare, the query service
/// refuses it rather than evaluate its constants another way.
#[test]
fn a_values_insert_sent_to_prepare_is_refused() {
    let v = &doris_fixtures::values::values_vectors()[0];
    let st = env().query.prepare(&v.request).status;
    assert_eq!(st.status_code, 3, "{st:?}");
    assert!(st.error_msgs.iter().any(|e| e.contains("INSERT … VALUES")), "{st:?}");
}

/// An ARITHMETIC_EXPR, as the FE sends it: a call node of that type.
fn arith(name: &str, ret: doris_thrift::gen::types::TTypeDesc, args: Vec<doris_thrift::gen::exprs::TExpr>) -> doris_thrift::gen::exprs::TExpr {
    let mut e = doris_fixtures::build::call(name, ret, args);
    e.nodes[0].node_type = doris_thrift::gen::exprs::TExprNodeType::ARITHMETIC_EXPR;
    e
}

/// The scan of `columns()` keeping rows where k + 1 > 0, under `check_overflow_for_decimal`.
fn arithmetic_scan(q: &TUniqueId, tablet: i64, version: i64, check_overflow: bool) -> TPipelineFragmentParams {
    use doris_fixtures::build::{binary, int, slot};
    let mut p = prepared(scan_query(q.clone(), tablet, version, &columns(), 4064));
    p.query_options.as_mut().unwrap().check_overflow_for_decimal = Some(check_overflow);
    let i = ty(T::INT);
    let plus = arith("add", i.clone(), vec![slot(0, i.clone()), int(1, i.clone())]);
    p.fragment.as_mut().unwrap().plan.as_mut().unwrap().nodes[0].conjuncts = Some(vec![binary("gt", plus, int(0, i))]);
    p
}

#[test]
fn arithmetic_with_unchecked_decimal_overflow_is_refused() {
    let env = env();
    let v = table(&env, 1101, kv(2));
    let q = qid(11);
    let r = env.query.prepare(&exec_request(vec![arithmetic_scan(&q, 1101, v, false)]));
    assert_ne!(r.status.status_code, 0);
    assert!(r.status.error_msgs.iter().any(|m| m.contains("arithmetic with check_overflow_for_decimal off")), "{:?}", r.status);
    // checked, as the FE always sends it: the same plan runs
    let q = qid(12);
    let r = env.query.prepare(&exec_request(vec![arithmetic_scan(&q, 1101, v, true)]));
    assert_eq!(r.status.status_code, 0, "{:?}", r.status);
    start(&env.query, &q);
    assert_eq!(rows(&fetch_all(&env.query, &q)).len(), 2);
}

#[test]
fn intermediate_projections_need_their_tuples() {
    use doris_fixtures::build::slot;
    let env = env();
    let v = table(&env, 1102, kv(2));
    let q = qid(13);
    let mut p = prepared(scan_query(q.clone(), 1102, v, &columns(), 4064));
    p.fragment.as_mut().unwrap().plan.as_mut().unwrap().nodes[0].intermediate_projections_list = Some(vec![vec![slot(0, ty(T::INT))]]);
    let r = env.query.prepare(&exec_request(vec![p]));
    assert_ne!(r.status.status_code, 0);
    assert!(r.status.error_msgs.iter().any(|m| m.contains("intermediate projections")), "{:?}", r.status);
}

/// `columns()`'s scan with extra slots: (id, tuple, name, type, nullable); new tuples are added.
fn with_slots(p: &mut TPipelineFragmentParams, slots: &[(i32, i32, &str, doris_thrift::gen::types::TTypeDesc, bool)]) {
    let desc = p.desc_tbl.as_mut().unwrap();
    let slot0 = desc.slot_descriptors.as_ref().unwrap()[0].clone();
    let tuple0 = desc.tuple_descriptors[0].clone();
    for (id, tuple, name, t, nullable) in slots {
        if !desc.tuple_descriptors.iter().any(|d| d.id == *tuple) {
            desc.tuple_descriptors.push(doris_thrift::gen::descriptors::TTupleDescriptor { id: *tuple, ..tuple0.clone() });
        }
        desc.slot_descriptors.as_mut().unwrap().push(doris_thrift::gen::descriptors::TSlotDescriptor {
            id: *id,
            parent: *tuple,
            slot_type: t.clone(),
            col_name: name.to_string(),
            col_unique_id: Some(-1),
            null_indicator_bit: if *nullable { 0 } else { -1 },
            primitive_type: t.types.as_ref().and_then(|x| x[0].scalar_type.as_ref()).map(|s| s.type_),
            ..slot0.clone()
        });
    }
}

#[test]
fn a_two_stage_projection_evaluates() {
    use doris_fixtures::build::{int, slot};
    let env = env();
    let v = table(&env, 1103, kv(3));
    let q = qid(14);
    let i = ty(T::INT);
    let mut p = prepared(scan_query(q.clone(), 1103, v, &columns(), 4064));
    // (k + k) into tuple 1, then that * 2 into tuple 2 (OperatorXBase::do_projections)
    with_slots(&mut p, &[(10, 1, "kk", i.clone(), false), (20, 2, "kk2", i.clone(), false)]);
    let frag = p.fragment.as_mut().unwrap();
    let n = &mut frag.plan.as_mut().unwrap().nodes[0];
    n.intermediate_projections_list = Some(vec![vec![arith("add", i.clone(), vec![slot(0, i.clone()), slot(0, i.clone())])]]);
    n.intermediate_output_tuple_id_list = Some(vec![1]);
    n.projections = Some(vec![arith("multiply", i.clone(), vec![slot(10, i.clone()), int(2, i.clone())])]);
    n.output_tuple_id = Some(2);
    frag.output_exprs = Some(vec![slot(20, i.clone())]);
    let r = env.query.prepare(&exec_request(vec![p]));
    assert_eq!(r.status.status_code, 0, "{:?}", r.status);
    start(&env.query, &q);
    let mut got = rows(&fetch_all(&env.query, &q));
    got.sort();
    let s = |x: &str| vec![Some(x.to_string())];
    assert_eq!(got, vec![s("0"), s("4"), s("8")]);
}

#[test]
fn a_virtual_slot_is_computed_from_its_row() {
    use doris_fixtures::build::{call, slot, string};
    let env = env();
    let v = table(&env, 1104, kv(3));
    let q = qid(15);
    let vc = ty(T::VARCHAR);
    let mut p = prepared(scan_query(q.clone(), 1104, v, &columns(), 4064));
    // slot 2: concat(v, '!'), a virtual column of the scan's tuple (col_unique_id INT32_MAX - 1)
    with_slots(&mut p, &[(2, 0, "__DORIS_VIRTUAL_COL__1", vc.clone(), true)]);
    let desc = p.desc_tbl.as_mut().unwrap();
    let s2 = desc.slot_descriptors.as_mut().unwrap().iter_mut().find(|s| s.id == 2).unwrap();
    s2.col_unique_id = Some(i32::MAX - 1);
    s2.virtual_column_expr = Some(call("concat", vc.clone(), vec![slot(1, vc.clone()), string("!")]));
    p.fragment.as_mut().unwrap().output_exprs = Some(vec![slot(0, ty(T::INT)), slot(2, vc.clone())]);
    let r = env.query.prepare(&exec_request(vec![p]));
    assert_eq!(r.status.status_code, 0, "{:?}", r.status);
    start(&env.query, &q);
    let mut got = rows(&fetch_all(&env.query, &q));
    got.sort();
    let s = |x: &str| Some(x.to_string());
    assert_eq!(got, vec![vec![s("0"), s("v0!")], vec![s("1"), None], vec![s("2"), s("v2!")]]);
}

#[test]
fn a_constant_overflow_fails_exec_plan_fragment() {
    use doris_fixtures::build::{binary, call, date, int};
    let env = env();
    let v = table(&env, 1030, kv(2));
    let q = qid(30);
    let mut p = prepared(scan_query(q.clone(), 1030, v, &columns(), 4064));
    let d = ty(T::DATEV2);
    let over = call("years_add", d.clone(), vec![date("9999-12-31", d.clone()), int(1, ty(T::INT))]);
    p.fragment.as_mut().unwrap().plan.as_mut().unwrap().nodes[0].conjuncts = Some(vec![binary("eq", over, date("2024-01-01", d))]);
    let r = env.query.exec_plan_fragment(&exec_request(vec![p]));
    assert_eq!(r.status.status_code, -218, "{:?}", r.status);
    assert_eq!(r.status.error_msgs, vec!["(127.0.0.1)[E-218]Operation year_add of 9999-12-31, 1 out of range".to_string()]);
    // nothing runs
    assert_ne!(env.query.fetch_data(&fetch_req(&q)).status.status_code, 0);
}

/// test_window_fn's `rank() over (partition by … order by sum(…))` on a GROUP BY: the window
/// sits on a sort over the merging aggregation, in 4 instances, fed by an exchange hashed on one
/// grouping key. A window partition's rows are in one instance when the exchange's hash keys are
/// among the partition keys, mapped through the sort and the grouping exprs.
fn window_over_agg_prepare(projected: bool) -> impl Fn(&dyn Fn(&mut doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList)) -> doris_proto::doris::PStatus {
    use doris_proto::doris::PExecPlanFragmentRequest;
    use prost::Message;
    let suite = "nereids_p0.sql_functions.window_functions.test_window_fn.test_window_fn";
    let recs = doris_wire::suites::read_suite(&doris_wire::suites::corpus_dir().join(format!("{suite}.bin.gz"))).expect("the suite's traffic");
    let (req, list) = recs
        .iter()
        .filter(|r| r.header.get("method").and_then(|v| v.as_str()) == Some("exec_plan_fragment_prepare") && r.header.get("kind").and_then(|v| v.as_str()) == Some("request"))
        .filter_map(|r| PExecPlanFragmentRequest::decode(&r.data[..]).ok())
        .find_map(|req| {
            let l = doris_fixtures::replays::plan_list(&req)?;
            let fits = l.params_list.iter().flatten().any(|p| {
                let nodes = p.fragment.as_ref().and_then(|f| f.plan.as_ref()).map(|x| x.nodes.as_slice()).unwrap_or_default();
                p.local_params.as_ref().is_some_and(|l| l.len() >= 4)
                    && nodes.iter().position(|n| n.node_type == TPlanNodeType::ANALYTIC_EVAL_NODE).is_some_and(|a| nodes[a..].iter().any(|n| n.node_type == TPlanNodeType::AGGREGATION_NODE))
                    && !nodes.iter().any(|n| n.node_type == TPlanNodeType::PARTITION_SORT_NODE)
                    && nodes.iter().any(|n| n.node_type == TPlanNodeType::AGGREGATION_NODE && n.projections.is_some() == projected)
            });
            fits.then_some((req, l))
        })
        .expect("a window over an aggregation in 4 instances");
    move |change| {
        let mut l = list.clone();
        change(&mut l);
        let mut r = req.clone();
        r.request = exec_request(l.params_list.clone().unwrap()).request;
        env().query.prepare(&r).status
    }
}

#[test]
fn a_window_over_an_aggregation_runs_in_several_instances() {
    let prepare = window_over_agg_prepare(false);
    let st = prepare(&|_| {});
    assert_eq!(st.status_code, 0, "{st:?}");
    // the aggregation projects (`partition by k order by sum(a) + sum(b)`): the partition key is
    // a projected grouping slot
    let st = window_over_agg_prepare(true)(&|_| {});
    assert_eq!(st.status_code, 0, "{st:?}");
    // the aggregation's exchange hashed on another grouping key: a partition may span instances
    let rehash = |l: &mut doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList| {
        let agg_exchange = {
            let plan = l.params_list.as_mut().unwrap().iter_mut().map(|p| p.fragment.as_mut().unwrap().plan.as_mut().unwrap()).find(|p| p.nodes.iter().any(|n| n.node_type == TPlanNodeType::ANALYTIC_EVAL_NODE)).unwrap();
            let at = plan.nodes.iter().position(|n| n.node_type == TPlanNodeType::ANALYTIC_EVAL_NODE).unwrap();
            plan.nodes[at..].iter().find(|n| n.node_type == TPlanNodeType::EXCHANGE_NODE).unwrap().node_id
        };
        let group = l
            .params_list
            .as_ref()
            .unwrap()
            .iter()
            .flat_map(|p| p.fragment.as_ref().unwrap().plan.as_ref().unwrap().nodes.iter())
            .find_map(|n| n.agg_node.as_ref().filter(|_| n.node_type == TPlanNodeType::AGGREGATION_NODE).and_then(|a| a.grouping_exprs.clone()))
            .unwrap();
        for p in l.params_list.as_mut().unwrap() {
            if let Some(s) = p.fragment.as_mut().unwrap().output_sink.as_mut().and_then(|s| s.stream_sink.as_mut()) {
                if s.dest_node_id == agg_exchange {
                    // every grouping key: more than the window's partition key
                    s.output_partition.partition_exprs = Some(group.clone());
                }
            }
        }
    };
    refused_by(&prepare, &rehash, "an ANALYTIC_EVAL_NODE hashed on other exprs than its partition keys in 4 instances");
}
