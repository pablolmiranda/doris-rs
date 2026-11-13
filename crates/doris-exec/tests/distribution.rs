//! How join plans are distributed over a query's instances: the bucket-shuffle sink, fragments
//! the FE runs as one task with a local shuffle (`parallel_instances`), and broadcast builds
//! shared by every instance of a fragment.
use std::collections::BTreeSet;

use doris_exec::block::Block;
use doris_exec::exchange::{Chunk, ExchangeKey, Exchanges};
use doris_exec::pipeline::send_stream;
use doris_exec::query::{check_plan, plan_query, shared_build, RunMode};
use doris_exec::value::ColType;
use doris_fixtures::build::slot;
use doris_fixtures::replays;
use doris_segment::writer::Datum;
use doris_thrift::gen::data_sinks::{TDataStreamSink, TPlanFragmentDestination};
use doris_thrift::gen::partitions::{TDataPartition, TPartitionType};
use doris_thrift::gen::plan_nodes::TPlanNodeType as P;
use doris_thrift::gen::types::{TNetworkAddress, TPrimitiveType as T, TUniqueId};

fn int_col() -> ColType {
    ColType {
        name: "k".into(),
        ptype: T::INT,
        len: -1,
        precision: 0,
        scale: 0,
        nullable: true,
        ..ColType::default()
    }
}

fn dest(hi: i64, lo: i64) -> TPlanFragmentDestination {
    TPlanFragmentDestination::new(
        TUniqueId { hi, lo },
        TNetworkAddress {
            hostname: "127.0.0.1".into(),
            port: 8060,
        },
        None,
    )
}

/// zlib crc32 of an INT's four little-endian bytes, as `RawValue::zlib_crc32` hashes it.
fn crc_int(k: i32) -> u32 {
    crc32fast::hash(&k.to_le_bytes())
}

#[test]
fn bucket_shuffle_routes_by_crc_and_dedups() {
    // a key for each bucket of four, by the crc alone
    let keys: Vec<i32> = (0..4).map(|b| (0..).find(|k| crc_int(*k) % 4 == b).unwrap()).collect();
    let sink = TDataStreamSink::new(
        7,
        TDataPartition::new(
            TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED,
            Some(vec![slot(1, doris_fixtures::build::ty(T::INT))]),
            None,
            None,
            None,
        ),
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
    // buckets 0 and 1 in instance A, bucket 2 in B, bucket 3 unscanned
    let destinations = vec![dest(1, 1), dest(1, 1), dest(1, 2), dest(-1, -1)];
    let chunk = Chunk {
        block: Block {
            types: vec![int_col()],
            rows: keys.iter().map(|k| vec![Datum::Int(i128::from(*k))]).collect(),
        },
        slots: vec![1],
    };
    let ex = Exchanges::default();
    send_stream(&ex, (9, 9), &sink, &destinations, chunk, 0).unwrap();
    let key = |hi, lo| ExchangeKey {
        query: (9, 9),
        instance: (hi, lo),
        node: 7,
    };
    assert_eq!(ex.pending(&key(1, 1)), Some((1, 1)), "A: one send, one end");
    assert_eq!(ex.pending(&key(1, 2)), Some((1, 1)), "B: one send, one end");
    assert_eq!(ex.pending(&key(-1, -1)), None, "nothing to an unscanned bucket");
    let rows = |hi, lo| -> Vec<i128> {
        ex.receive(&key(hi, lo), 1)
            .unwrap()
            .into_iter()
            .flat_map(|c| c.block.rows)
            .map(|r| match r[0] {
                Datum::Int(i) => i,
                _ => panic!("{r:?}"),
            })
            .collect()
    };
    assert_eq!(rows(1, 1), [i128::from(keys[0]), i128::from(keys[1])]);
    assert_eq!(rows(1, 2), [i128::from(keys[2])]);
}

/// The first result query of a replay whose plan has a fragment matching `want`.
fn find_plan(suite: &str, want: impl Fn(&doris_exec::query::QueryPlan) -> bool) -> doris_exec::query::QueryPlan {
    for q in replays::load(suite).queries() {
        if let Ok(p) = plan_query(&q) {
            if want(&p) {
                return p;
            }
        }
    }
    panic!("{suite}: no such plan")
}

#[test]
fn merged_fragment_runs_once_over_all_ranges() {
    let p = find_plan("correctness_p0.test_join_with_projection.test_join_with_projection", |p| {
        p.fragments
            .iter()
            .any(|f| f.mode == RunMode::Merged && f.root.nodes().iter().any(|n| n.node_type == P::EXCHANGE_NODE))
    });
    for f in &p.fragments {
        if f.mode == RunMode::Merged {
            assert!(f.instances.len() > 1);
        }
    }
    check_plan(&p).unwrap();
}

#[test]
fn shared_build_reaches_every_instance() {
    let suite = "nereids_p0.join.test_join2.test_join2";
    let p = find_plan(suite, |p| {
        p.fragments.iter().enumerate().any(|(i, f)| {
            f.mode == RunMode::Each
                && f.instances.len() > 1
                && f.root
                    .nodes()
                    .iter()
                    .any(|n| n.node_type == P::EXCHANGE_NODE && shared_build(p, i, n.node_id))
        })
    });
    check_plan(&p).unwrap();
    // every broadcast build exchange is fed only to the fragment's first instance
    for (i, f) in p.fragments.iter().enumerate() {
        for n in f
            .root
            .nodes()
            .into_iter()
            .filter(|n| n.node_type == P::EXCHANGE_NODE && shared_build(&p, i, n.node_id))
        {
            let fed: BTreeSet<usize> = (0..f.instances.len())
                .filter(|ii| doris_exec::query::plan_senders(&p, n.node_id, &f.instances[*ii].id) > 0)
                .collect();
            assert_eq!(fed, BTreeSet::from([0]), "{suite}: node {}", n.node_id);
        }
    }
}

#[test]
fn check_senders_accepts_local_shuffle_plans() {
    let suite = "correctness_p0.test_outer_join_with_cross_join.test_outer_join_with_cross_join";
    let mut merged = 0;
    for q in replays::load(suite).queries().into_iter().filter(replays::is_result_query) {
        let p = plan_query(&q).unwrap();
        check_plan(&p).unwrap();
        merged += p.fragments.iter().filter(|f| f.mode == RunMode::Merged).count();
    }
    assert!(merged > 0, "{suite}: no merged fragment");
}

#[test]
fn a_sender_naming_a_merged_fragments_idle_instance_is_refused() {
    let mut p = find_plan("correctness_p0.test_join_with_projection.test_join_with_projection", |p| {
        p.fragments
            .iter()
            .any(|f| f.mode == RunMode::Merged && f.root.nodes().iter().any(|n| n.node_type == P::EXCHANGE_NODE))
    });
    let merged = p.fragments.iter().position(|f| f.mode == RunMode::Merged).unwrap();
    let exchange = p.fragments[merged]
        .root
        .nodes()
        .into_iter()
        .find(|n| n.node_type == P::EXCHANGE_NODE)
        .unwrap()
        .node_id;
    let second = p.fragments[merged].instances[1].id.clone();
    // the fragment that feeds that exchange sends to the merged fragment's second instance
    let sender = p
        .fragments
        .iter()
        .position(|f| f.sink.stream_sink.as_ref().is_some_and(|s| s.dest_node_id == exchange))
        .expect("a sender");
    // instance 0 keeps its senders; instance 1 is named too, and would never read its rows
    let mut extra = p.fragments[sender].destinations[0].clone();
    extra.fragment_instance_id = second;
    p.fragments[sender].destinations.push(extra);
    let err = check_plan(&p).unwrap_err().to_string();
    assert!(err.contains("merged"), "{err}");
}

/// The first plan in `suites` with a multi-instance fragment (run one instance at a time) holding
/// a hash join of distribution `dist`, and that fragment's index.
fn join_plan(suites: &[&str], dist: doris_thrift::gen::plan_nodes::TJoinDistributionType) -> (doris_exec::query::QueryPlan, usize) {
    for s in suites {
        for q in replays::load(s).queries().into_iter().filter(replays::is_result_query) {
            let Ok(p) = plan_query(&q) else { continue };
            let at = p.fragments.iter().position(|f| {
                f.mode == RunMode::Each
                    && f.instances.len() > 1
                    && f.root
                        .nodes()
                        .iter()
                        .any(|n| n.hash_join_node.as_ref().is_some_and(|h| h.dist_type == Some(dist)))
            });
            if let Some(at) = at {
                return (p, at);
            }
        }
    }
    panic!("no {dist:?} join in {suites:?}")
}

/// Every sink that feeds an exchange of fragment `at` now partitions as `to`.
fn repartition(p: &mut doris_exec::query::QueryPlan, at: usize, to: TPartitionType) {
    let exchanges: Vec<i32> = p.fragments[at]
        .root
        .nodes()
        .iter()
        .filter(|n| n.node_type == P::EXCHANGE_NODE)
        .map(|n| n.node_id)
        .collect();
    for f in &mut p.fragments {
        if let Some(s) = f.sink.stream_sink.as_mut().filter(|s| exchanges.contains(&s.dest_node_id)) {
            s.output_partition.type_ = to;
        }
    }
}

#[test]
fn a_shuffle_join_whose_inputs_are_not_hashed_is_refused() {
    use doris_thrift::gen::plan_nodes::TJoinDistributionType as D;
    let (mut p, at) = join_plan(&["query_p0.join.test_runtimefilter_2.test_runtimefilter_2"], D::PARTITIONED);
    check_plan(&p).unwrap();
    repartition(&mut p, at, TPartitionType::RANDOM);
    let err = check_plan(&p).unwrap_err().to_string();
    assert!(err.contains("PARTITIONED join"), "{err}");
}

#[test]
fn a_bucket_shuffle_join_fed_by_a_hash_shuffle_is_refused() {
    use doris_thrift::gen::plan_nodes::TJoinDistributionType as D;
    let (mut p, at) = join_plan(&["nereids_p0.join.test_join_local_shuffle.test_join_local_shuffle"], D::BUCKET_SHUFFLE);
    check_plan(&p).unwrap();
    repartition(&mut p, at, TPartitionType::HASH_PARTITIONED);
    let err = check_plan(&p).unwrap_err().to_string();
    assert!(err.contains("BUCKET_SHUFFLE join"), "{err}");
}

#[test]
fn a_broadcast_join_whose_build_is_split_is_refused() {
    use doris_thrift::gen::plan_nodes::TJoinDistributionType as D;
    let (mut p, at) = join_plan(
        &["nereids_p0.join.test_join2.test_join2", "query_p0.join.test_join4.test_join4"],
        D::BROADCAST,
    );
    check_plan(&p).unwrap();
    repartition(&mut p, at, TPartitionType::RANDOM);
    let err = check_plan(&p).unwrap_err().to_string();
    assert!(err.contains("BROADCAST join"), "{err}");
}

#[test]
fn bucket_shuffle_skips_constant_partition_exprs() {
    // `partitioner.cpp:44-46`: a constant column is not hashed
    let keys: Vec<i32> = (0..2).map(|b| (0..).find(|k| crc_int(*k) % 2 == b).unwrap()).collect();
    let exprs = vec![
        slot(1, doris_fixtures::build::ty(T::INT)),
        doris_fixtures::build::int(5, doris_fixtures::build::ty(T::INT)),
    ];
    let sink = TDataStreamSink::new(
        7,
        TDataPartition::new(TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED, Some(exprs), None, None, None),
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
    let chunk = Chunk {
        block: Block {
            types: vec![int_col()],
            rows: keys.iter().map(|k| vec![Datum::Int(i128::from(*k))]).collect(),
        },
        slots: vec![1],
    };
    let ex = Exchanges::default();
    send_stream(&ex, (9, 9), &sink, &[dest(1, 1), dest(1, 2)], chunk, 0).unwrap();
    let rows = |lo| -> Vec<Vec<Datum>> {
        ex.receive(
            &ExchangeKey {
                query: (9, 9),
                instance: (1, lo),
                node: 7,
            },
            1,
        )
        .unwrap()
        .into_iter()
        .flat_map(|c| c.block.rows)
        .collect()
    };
    assert_eq!(rows(1), [vec![Datum::Int(i128::from(keys[0]))]]);
    assert_eq!(rows(2), [vec![Datum::Int(i128::from(keys[1]))]]);
}

/// A mark join's plan with its op set to `op`.
fn mark_join_as(op: doris_thrift::gen::plan_nodes::TJoinOp) -> doris_exec::query::QueryPlan {
    let is_mark = |n: &doris_thrift::gen::plan_nodes::TPlanNode| n.hash_join_node.as_ref().is_some_and(|h| h.is_mark == Some(true));
    let mut p = find_plan("nereids_p0.join.test_mark_join.test_mark_join", |p| p.fragments.iter().any(|f| f.root.nodes().iter().any(|n| is_mark(n))));
    fn set(t: &mut doris_exec::query::PlanTree, op: doris_thrift::gen::plan_nodes::TJoinOp) {
        if let Some(h) = t.node.hash_join_node.as_mut().filter(|h| h.is_mark == Some(true)) {
            h.join_op = op;
        }
        t.children.iter_mut().for_each(|c| set(c, op));
    }
    p.fragments.iter_mut().for_each(|f| set(&mut f.root, op));
    p
}

#[test]
fn a_right_semi_mark_join_is_run_and_a_right_anti_one_refused() {
    use doris_thrift::gen::plan_nodes::TJoinOp;
    check_plan(&mark_join_as(TJoinOp::RIGHT_SEMI_JOIN)).unwrap();
    let err = check_plan(&mark_join_as(TJoinOp::RIGHT_ANTI_JOIN)).unwrap_err().to_string();
    assert!(err.contains("a mark join with join op RIGHT_ANTI_JOIN"), "{err}");
}
