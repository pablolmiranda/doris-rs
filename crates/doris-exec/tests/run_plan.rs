//! Join plans run end to end over hand-built fragments: every instance on its own thread, rows
//! fed and collected through the in-process exchanges, as doris-be runs them.
use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc;
use std::time::Duration;

use doris_exec::block::Block;
use doris_exec::exchange::{Chunk, ExchangeKey, Exchanges};
use doris_exec::pipeline::{run_instance, ExecCtx};
use doris_exec::query::{plan_query, DescTable, FragmentPlan, InstancePlan, PlanTree, QueryPlan, RunMode, SlotInfo};
use doris_exec::value::ColType;
use doris_exec::ExecError;
use doris_fixtures::build::{slot, ty};
use doris_fixtures::replays;
use doris_segment::writer::Datum;
use doris_thrift::gen::data_sinks::{TDataSink, TDataSinkType, TPlanFragmentDestination};
use doris_thrift::gen::opcodes::TExprOpcode;
use doris_thrift::gen::partitions::{TDataPartition, TPartitionType};
use doris_thrift::gen::plan_nodes::{
    TAssertNumRowsNode, TAssertion, TEqJoinCondition, TExchangeNode, THashJoinNode, TJoinDistributionType, TJoinOp, TPlanNode, TPlanNodeType as P,
};
use doris_thrift::gen::types::{TNetworkAddress, TPrimitiveType as T, TUniqueId};

const QUERY: (i64, i64) = (77, 77);
/// The exchange every test's last fragment sends to; nothing runs it, the test reads it.
const OUT: i32 = 99;
const OUT_INSTANCE: (i64, i64) = (5, 5);

fn int(nullable: bool) -> ColType {
    ColType {
        name: "k".into(),
        ptype: T::INT,
        len: -1,
        precision: 0,
        scale: 0,
        nullable,
    }
}

/// Tuple 0: slot 1 (probe k); tuple 1: slot 2 (build k); tuple 2: slots 3, 4 (a join's
/// intermediate); tuple 3: slot 5 (a semi join's intermediate). All INT, not nullable.
fn desc() -> DescTable {
    let s = |id: i32, tuple: i32| {
        (
            id,
            SlotInfo {
                id,
                tuple,
                ty: int(false),
                unique_id: 0,
                name: "k".into(),
                virtual_expr: None,
            },
        )
    };
    DescTable {
        slots: HashMap::from([s(1, 0), s(2, 1), s(3, 2), s(4, 2), s(5, 3)]),
        tuples: HashMap::from([(0, vec![1]), (1, vec![2]), (2, vec![3, 4]), (3, vec![5])]),
    }
}

/// A plan node and a stream sink from a corpus plan, to clone: Thrift's structs have no Default.
fn templates() -> (TPlanNode, TPlanNode, TDataSink) {
    let r = replays::load("correctness_p0.test_join_with_projection.test_join_with_projection");
    for q in r.queries().into_iter().filter(replays::is_result_query) {
        let Ok(p) = plan_query(&q) else { continue };
        let scan = p
            .fragments
            .iter()
            .flat_map(|f| f.root.nodes())
            .find(|n| n.node_type == P::OLAP_SCAN_NODE)
            .cloned();
        let sink = p
            .fragments
            .iter()
            .find(|f| f.sink.type_ == TDataSinkType::DATA_STREAM_SINK)
            .map(|f| f.sink.clone());
        if let (Some(scan), Some(sink)) = (scan, sink) {
            let mut blank = scan.clone();
            blank.olap_scan_node = None;
            return (node(blank, P::EMPTY_SET_NODE, 0, 0, vec![0]), scan, sink);
        }
    }
    panic!("no template plan")
}

/// `template` as a node of another type: no children's fields, conjuncts, projections or limit.
fn node(mut n: TPlanNode, t: P, id: i32, children: i32, tuples: Vec<i32>) -> TPlanNode {
    n.node_id = id;
    n.node_type = t;
    n.num_children = children;
    n.limit = -1;
    n.nullable_tuples = vec![false; tuples.len()];
    n.row_tuples = tuples;
    n.conjuncts = None;
    n.projections = None;
    n.output_tuple_id = None;
    n.intermediate_projections_list = None;
    n.intermediate_output_tuple_id_list = None;
    n.runtime_filters = None;
    n.hash_join_node = None;
    n.nested_loop_join_node = None;
    n.exchange_node = None;
    n.assert_num_rows_node = None;
    n
}

/// A sender fragment's root (the test sends its rows itself).
fn empty(blank: &TPlanNode, id: i32, tuple: i32) -> PlanTree {
    PlanTree {
        node: node(blank.clone(), P::EMPTY_SET_NODE, id, 0, vec![tuple]),
        children: vec![],
    }
}

fn exchange(blank: &TPlanNode, id: i32, tuple: i32) -> PlanTree {
    let mut n = node(blank.clone(), P::EXCHANGE_NODE, id, 0, vec![tuple]);
    n.exchange_node = Some(TExchangeNode::new(vec![tuple], None, None, None));
    PlanTree { node: n, children: vec![] }
}

/// An INNER (or other op) hash join of probe slot 1 = build slot 2 into tuple 2 (or 3).
fn hash_join(blank: &TPlanNode, op: TJoinOp, broadcast: bool, probe: PlanTree, build: PlanTree) -> PlanTree {
    let semi = op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;
    let inter = if semi { 3 } else { 2 };
    let mut n = node(blank.clone(), P::HASH_JOIN_NODE, 10, 2, vec![0, 1]);
    n.hash_join_node = Some(THashJoinNode::new(
        op,
        vec![TEqJoinCondition::new(slot(1, ty(T::INT)), slot(2, ty(T::INT)), Some(TExprOpcode::EQ))],
        None,
        None,
        None,
        None,
        None,
        None,
        Some(vec![inter]),
        Some(broadcast),
        Some(false),
        Some(if broadcast {
            TJoinDistributionType::BROADCAST
        } else {
            TJoinDistributionType::PARTITIONED
        }),
        None,
        None,
        None,
    ));
    PlanTree {
        node: n,
        children: vec![probe, build],
    }
}

fn id(i: i64) -> TUniqueId {
    TUniqueId { hi: 1, lo: i }
}

fn dest(u: TUniqueId) -> TPlanFragmentDestination {
    TPlanFragmentDestination::new(
        u,
        TNetworkAddress {
            hostname: "127.0.0.1".into(),
            port: 8060,
        },
        None,
    )
}

fn stream_sink(template: &TDataSink, node: i32) -> TDataSink {
    let mut s = template.clone();
    let st = s.stream_sink.as_mut().expect("a stream sink");
    st.dest_node_id = node;
    st.output_partition = TDataPartition::new(TPartitionType::UNPARTITIONED, None, None, None, None);
    st.output_exprs = None;
    st.output_tuple_id = None;
    st.conjuncts = None;
    st.runtime_filters = None;
    s
}

fn fragment(
    root: PlanTree,
    sink: TDataSink,
    destinations: Vec<TUniqueId>,
    instances: Vec<InstancePlan>,
    senders: &[(i32, i32)],
    mode: RunMode,
) -> FragmentPlan {
    FragmentPlan {
        fragment_id: 0,
        root,
        sink,
        output_exprs: vec![],
        per_exch_num_senders: senders.iter().copied().collect(),
        destinations: destinations.into_iter().map(dest).collect(),
        instances,
        write: None,
        mode,
    }
}

fn instance(i: i64, ranges: &[(i32, Vec<(i64, i64)>)]) -> InstancePlan {
    InstancePlan {
        data_gen: Default::default(),
        id: id(i),
        sender_id: i as i32,
        scan_ranges: ranges.iter().cloned().collect::<BTreeMap<_, _>>(),
    }
}

fn query(fragments: Vec<FragmentPlan>) -> QueryPlan {
    QueryPlan {
        query_id: TUniqueId { hi: QUERY.0, lo: QUERY.1 },
        fragments,
        desc: desc(),
        batch_size: 4064,
        timeout_s: 30,
        coord: None,
        backend_id: None,
        need_wait: false,
        parallel_result_sink: true,
        binary_rows: false,
        dry_run: false,
        unchecked_arithmetic: false,
        clock: doris_exec::clock::QueryClock::from_optional(None),
        share_broadcast_build: true,
        regex: Default::default(),
    }
}

struct NoFe;
impl doris_exec::schema_scan::SchemaFe for NoFe {
    fn get_db_names(
        &self,
        _: &TNetworkAddress,
        _: doris_thrift::gen::frontend_service::TGetDbsParams,
    ) -> Result<doris_thrift::gen::frontend_service::TGetDbsResult, String> {
        Err("no FE".into())
    }
    fn get_table_names(
        &self,
        _: &TNetworkAddress,
        _: doris_thrift::gen::frontend_service::TGetTablesParams,
    ) -> Result<doris_thrift::gen::frontend_service::TGetTablesResult, String> {
        Err("no FE".into())
    }
    fn describe_tables(
        &self,
        _: &TNetworkAddress,
        _: doris_thrift::gen::frontend_service::TDescribeTablesParams,
    ) -> Result<doris_thrift::gen::frontend_service::TDescribeTablesResult, String> {
        Err("no FE".into())
    }
    fn list_table_status(
        &self,
        _: &TNetworkAddress,
        _: doris_thrift::gen::frontend_service::TGetTablesParams,
    ) -> Result<doris_thrift::gen::frontend_service::TListTableStatusResult, String> {
        Err("no FE".into())
    }
    fn show_variables(
        &self,
        _: &TNetworkAddress,
        _: doris_thrift::gen::frontend_service::TShowVariableRequest,
    ) -> Result<doris_thrift::gen::frontend_service::TShowVariableResult, String> {
        Err("no FE".into())
    }
}

/// Runs every instance of fragment `f` on its own thread; each tablet scans as one row of its id.
fn run(plan: &QueryPlan, f: usize, ex: &Exchanges) -> Vec<Result<(), ExecError>> {
    let (tx, rx) = mpsc::channel();
    std::thread::scope(|s| {
        for i in 0..plan.fragments[f].instances.len() {
            let tx = tx.clone();
            s.spawn(move || {
                let scan = |tablet: i64, _: i64, _: &[doris_storage::reader::ScanColumn]| Ok(vec![vec![vec![Datum::Int(i128::from(tablet))]]]);
                let result = |_: Block| Ok(());
                let fetch = |_: &[Vec<u8>], _: &[doris_storage::reader::ScanColumn]| Ok(vec![]);
                let table = |_: usize, _: usize, _: Block| Ok(());
                let ctx = ExecCtx {
                    scan: &scan,
                    exchanges: ex,
                    result: &result,
                    fetch: &fetch,
                    fe: &NoFe,
                    table: &table,
                };
                tx.send(run_instance(plan, f, i, &ctx)).unwrap();
            });
        }
        drop(tx);
        let n = plan.fragments[f].instances.len();
        let out: Vec<_> = (0..n)
            .map(|_| rx.recv_timeout(Duration::from_secs(10)).expect("an instance hung"))
            .collect();
        if out.len() < n {
            ex.cancel(QUERY, "test timed out".into());
        }
        out
    })
}

fn out_key() -> ExchangeKey {
    ExchangeKey {
        query: QUERY,
        instance: OUT_INSTANCE,
        node: OUT,
    }
}

fn ints(rows: &[Vec<Datum>]) -> Vec<Vec<i128>> {
    let mut v: Vec<Vec<i128>> = rows
        .iter()
        .map(|r| {
            r.iter()
                .map(|d| match d {
                    Datum::Int(i) => *i,
                    other => panic!("{other:?}"),
                })
                .collect()
        })
        .collect();
    v.sort();
    v
}

fn chunk(slot_id: i32, values: &[i128]) -> Chunk {
    Chunk {
        block: Block {
            types: vec![int(false)],
            rows: values.iter().map(|v| vec![Datum::Int(*v)]).collect(),
        },
        slots: vec![slot_id],
    }
}

#[test]
fn shared_build_feeds_four_instances() {
    let (blank, _, sink) = templates();
    // fragment 0 (not run) sends the build to instance 1 only, fragment 1 (not run) a probe
    // row to each instance; fragment 2 joins them in four instances
    let receivers: Vec<TUniqueId> = (1..=4).map(id).collect();
    let probe_sender = fragment(
        empty(&blank, 50, 0),
        stream_sink(&sink, 11),
        receivers.clone(),
        vec![instance(20, &[])],
        &[],
        RunMode::Each,
    );
    let build_sender = fragment(
        empty(&blank, 51, 1),
        stream_sink(&sink, 12),
        vec![id(1)],
        vec![instance(21, &[])],
        &[],
        RunMode::Each,
    );
    let join = hash_join(&blank, TJoinOp::INNER_JOIN, true, exchange(&blank, 11, 0), exchange(&blank, 12, 1));
    let joining = fragment(
        join,
        stream_sink(&sink, OUT),
        vec![TUniqueId {
            hi: OUT_INSTANCE.0,
            lo: OUT_INSTANCE.1,
        }],
        (1..=4).map(|i| instance(i, &[])).collect(),
        &[(11, 1), (12, 1)],
        RunMode::Each,
    );
    let plan = query(vec![probe_sender, build_sender, joining]);
    doris_exec::query::check_plan(&plan).unwrap();
    let ex = Exchanges::default();
    let key = |i: i64, node: i32| ExchangeKey {
        query: QUERY,
        instance: (1, i),
        node,
    };
    ex.send(&key(1, 12), chunk(2, &[1, 2, 3, 4]));
    ex.end(&key(1, 12));
    for i in 1..=4 {
        ex.send(&key(i, 11), chunk(1, &[i as i128]));
        ex.end(&key(i, 11));
    }
    for r in run(&plan, 2, &ex) {
        r.unwrap();
    }
    // every instance joined its probe row against the whole build: four rows, four ends
    assert_eq!(ex.pending(&out_key()), Some((4, 4)));
    let rows: Vec<Vec<Datum>> = ex.receive(&out_key(), 4).unwrap().into_iter().flat_map(|c| c.block.rows).collect();
    assert_eq!(ints(&rows), [[1, 1], [2, 2], [3, 3], [4, 4]]);
}

#[test]
fn merged_fragment_scans_every_range_once() {
    let (_, scan, sink) = templates();
    let mut scan = node(scan.clone(), P::OLAP_SCAN_NODE, 1, 0, vec![0]);
    scan.olap_scan_node.as_mut().unwrap().tuple_id = 0;
    let root = PlanTree {
        node: scan,
        children: vec![],
    };
    // three instances, the second and third holding ranges of the same scan node
    let instances = vec![
        instance(1, &[(1, vec![(100, 1)])]),
        instance(2, &[(1, vec![(101, 1), (102, 1)])]),
        instance(3, &[(1, vec![(103, 1)])]),
    ];
    let merged = fragment(
        root,
        stream_sink(&sink, OUT),
        vec![TUniqueId {
            hi: OUT_INSTANCE.0,
            lo: OUT_INSTANCE.1,
        }],
        instances,
        &[],
        RunMode::Merged,
    );
    let plan = query(vec![merged]);
    doris_exec::query::check_plan(&plan).unwrap();
    let ex = Exchanges::default();
    for r in run(&plan, 0, &ex) {
        r.unwrap();
    }
    // the first instance sends every range's row once; each instance ends once
    assert_eq!(ex.pending(&out_key()), Some((1, 3)));
    let rows: Vec<Vec<Datum>> = ex.receive(&out_key(), 3).unwrap().into_iter().flat_map(|c| c.block.rows).collect();
    assert_eq!(ints(&rows), [[100], [101], [102], [103]]);
}

/// A probe child that fails when it runs: ASSERT_NUM_ROWS EQ 5 over an empty set.
fn failing_probe(blank: &TPlanNode) -> PlanTree {
    let mut a = node(blank.clone(), P::ASSERT_NUM_ROWS_NODE, 30, 1, vec![0]);
    a.assert_num_rows_node = Some(TAssertNumRowsNode {
        desired_num_rows: Some(5),
        subquery_string: Some("probe".into()),
        assertion: Some(TAssertion::EQ),
        should_convert_output_to_nullable: Some(false),
    });
    PlanTree {
        node: a,
        children: vec![PlanTree {
            node: node(blank.clone(), P::EMPTY_SET_NODE, 31, 0, vec![0]),
            children: vec![],
        }],
    }
}

/// A one-instance join over a failing probe child and the given build rows.
fn short_circuit(op: TJoinOp, build: &[Option<i128>]) -> Result<(), ExecError> {
    let (blank, _, sink) = templates();
    let build_sender = fragment(
        empty(&blank, 51, 1),
        stream_sink(&sink, 12),
        vec![id(1)],
        vec![instance(21, &[])],
        &[],
        RunMode::Each,
    );
    let join = hash_join(&blank, op, false, failing_probe(&blank), exchange(&blank, 12, 1));
    let joining = fragment(
        join,
        stream_sink(&sink, OUT),
        vec![TUniqueId {
            hi: OUT_INSTANCE.0,
            lo: OUT_INSTANCE.1,
        }],
        vec![instance(1, &[])],
        &[(12, 1)],
        RunMode::Each,
    );
    let plan = query(vec![build_sender, joining]);
    doris_exec::query::check_plan(&plan).unwrap();
    let ex = Exchanges::default();
    let key = ExchangeKey {
        query: QUERY,
        instance: (1, 1),
        node: 12,
    };
    let rows = build.iter().map(|v| vec![v.map_or(Datum::Null, Datum::Int)]).collect();
    ex.send(
        &key,
        Chunk {
            block: Block {
                types: vec![int(true)],
                rows,
            },
            slots: vec![2],
        },
    );
    ex.end(&key);
    run(&plan, 1, &ex).remove(0)
}

#[test]
fn an_empty_build_side_skips_the_probe_child() {
    // INNER over an empty build: the probe child never runs, as in C++
    short_circuit(TJoinOp::INNER_JOIN, &[]).unwrap();
    // LEFT OUTER needs its probe rows: the failing child runs
    let err = short_circuit(TJoinOp::LEFT_OUTER_JOIN, &[]).unwrap_err().to_string();
    assert!(err.contains("Expected EQ 5"), "{err}");
}

#[test]
fn null_aware_anti_with_a_null_build_key_skips_the_probe_child() {
    // `_short_circuit_for_null_in_build_side`: the result is empty, the probe child not run
    short_circuit(TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN, &[Some(1), None]).unwrap();
    let err = short_circuit(TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN, &[Some(1)]).unwrap_err().to_string();
    assert!(err.contains("Expected EQ 5"), "{err}");
}

#[test]
fn assert_rows_over_no_rows_keeps_its_types() {
    // C++ makes the columns nullable only for a block with rows, or the EQ-1 NULL row
    let (blank, _, sink) = templates();
    let mut a = node(blank.clone(), P::ASSERT_NUM_ROWS_NODE, 30, 1, vec![0]);
    a.assert_num_rows_node = Some(TAssertNumRowsNode {
        desired_num_rows: Some(5),
        subquery_string: Some("s".into()),
        assertion: Some(TAssertion::LE),
        should_convert_output_to_nullable: Some(true),
    });
    let root = PlanTree {
        node: a,
        children: vec![PlanTree {
            node: node(blank.clone(), P::EMPTY_SET_NODE, 31, 0, vec![0]),
            children: vec![],
        }],
    };
    let f = fragment(
        root,
        stream_sink(&sink, OUT),
        vec![TUniqueId {
            hi: OUT_INSTANCE.0,
            lo: OUT_INSTANCE.1,
        }],
        vec![instance(1, &[])],
        &[],
        RunMode::Each,
    );
    let plan = query(vec![f]);
    let ex = Exchanges::default();
    run(&plan, 0, &ex).remove(0).unwrap();
    let chunks = ex.receive(&out_key(), 1).unwrap();
    assert!(chunks.iter().all(|c| c.block.types.iter().all(|t| !t.nullable)), "{chunks:?}");
}
