//! The multicast sink (a CTE used more than once): each consumer gets its own copy of the
//! fragment's output, filtered by its conjuncts and then projected onto its tuple, as
//! MultiCastDataStreamerSourceOperatorX does.
use std::collections::HashMap;

use doris_exec::block::Block;
use doris_exec::exchange::Chunk;
use doris_exec::exchange::{ExchangeKey, Exchanges};
use doris_exec::pipeline::{multicast, send_stream};
use doris_exec::query::{DescTable, SlotInfo};
use doris_exec::value::ColType;
use doris_exec::ExecError;
use doris_fixtures::build::{binary, int, scalar, slot, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::data_sinks::{TDataStreamSink, TMultiCastDataStreamSink, TPlanFragmentDestination};
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::partitions::{TDataPartition, TPartitionType};
use doris_thrift::gen::types::{TNetworkAddress, TPrimitiveType as T, TUniqueId};

fn col(name: &str, ptype: T) -> ColType {
    ColType {
        name: name.into(),
        ptype,
        len: -1,
        precision: 0,
        scale: 0,
        nullable: true,
    }
}

/// Tuple 0: slots 1 (INT k) and 2 (VARCHAR v); tuple 5: slot 50 (VARCHAR).
fn desc() -> DescTable {
    let slot = |id: i32, tuple: i32, name: &str, ptype: T| {
        (
            id,
            SlotInfo {
                id,
                tuple,
                ty: col(name, ptype),
                unique_id: -1,
                name: name.into(),
            },
        )
    };
    DescTable {
        slots: HashMap::from([slot(1, 0, "k", T::INT), slot(2, 0, "v", T::VARCHAR), slot(50, 5, "v2", T::VARCHAR)]),
        tuples: HashMap::from([(0, vec![1, 2]), (5, vec![50])]),
    }
}

fn input() -> Chunk {
    let row = |k: i128, v: &str| vec![Datum::Int(k), Datum::Bytes(v.as_bytes().to_vec())];
    Chunk {
        block: Block {
            types: vec![col("k", T::INT), col("v", T::VARCHAR)],
            rows: vec![row(1, "a"), row(2, "b"), row(3, "c")],
        },
        slots: vec![1, 2],
    }
}

fn consumer(node: i32, conjuncts: Option<Vec<TExpr>>, project: Option<(Vec<TExpr>, i32)>) -> TDataStreamSink {
    let (exprs, tuple) = project.map_or((None, None), |(e, t)| (Some(e), Some(t)));
    TDataStreamSink::new(
        node,
        TDataPartition::new(TPartitionType::UNPARTITIONED, None, None, None, None),
        None,
        exprs,
        tuple,
        conjuncts,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
}

fn dest(lo: i64) -> Vec<TPlanFragmentDestination> {
    vec![TPlanFragmentDestination::new(
        TUniqueId { hi: 1, lo },
        TNetworkAddress {
            hostname: "127.0.0.1".into(),
            port: 8060,
        },
        None,
    )]
}

fn varchar() -> doris_thrift::gen::types::TTypeDesc {
    scalar(T::VARCHAR, 10, 0, 0)
}

fn texts(c: &Chunk) -> Vec<String> {
    c.block
        .rows
        .iter()
        .map(|r| {
            r.iter()
                .map(|d| match d {
                    Datum::Int(v) => v.to_string(),
                    Datum::Bytes(b) => String::from_utf8(b.clone()).unwrap(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect()
}

#[test]
fn each_consumer_gets_its_own_copy() {
    let sink = TMultiCastDataStreamSink::new(
        vec![
            consumer(10, Some(vec![binary("gt", slot(1, ty(T::INT)), int(1, ty(T::INT)))]), None),
            consumer(20, Some(vec![]), Some((vec![slot(2, varchar())], 5))),
        ],
        vec![dest(1), dest(2)],
    );
    let out = input();
    let copies = multicast(&out, &sink, &desc()).unwrap();
    assert_eq!(copies.len(), 2);
    assert_eq!(copies[0].0.dest_node_id, 10);
    assert_eq!(copies[0].1[0].fragment_instance_id.lo, 1);
    assert_eq!(texts(&copies[0].2), vec!["2,b", "3,c"]);
    assert_eq!(copies[0].2.slots, vec![1, 2]);
    // an empty conjunct list filters nothing; the projection lands on tuple 5
    assert_eq!(copies[1].0.dest_node_id, 20);
    assert_eq!(texts(&copies[1].2), vec!["a", "b", "c"]);
    assert_eq!(copies[1].2.slots, vec![50]);
    assert_eq!(texts(&out), vec!["1,a", "2,b", "3,c"]);
}

#[test]
fn filter_comes_before_projection() {
    // the conjunct reads slot 1, which the projection drops
    let sink = TMultiCastDataStreamSink::new(
        vec![consumer(
            10,
            Some(vec![binary("eq", slot(1, ty(T::INT)), int(3, ty(T::INT)))]),
            Some((vec![slot(2, varchar())], 5)),
        )],
        vec![dest(1)],
    );
    let copies = multicast(&input(), &sink, &desc()).unwrap();
    assert_eq!(texts(&copies[0].2), vec!["c"]);
}

#[test]
fn sinks_and_destinations_must_pair() {
    let sink = TMultiCastDataStreamSink::new(vec![consumer(10, None, None), consumer(20, None, None)], vec![dest(1)]);
    match multicast(&input(), &sink, &desc()) {
        Err(ExecError::BadRequest(m)) => assert!(m.contains("multicast"), "{m}"),
        other => panic!("expected a BadRequest, got {other:?}"),
    }
}

#[test]
fn empty_output_exprs_send_rows_unchanged() {
    // C++ projects only when output_exprs is non-empty
    let sink = TMultiCastDataStreamSink::new(vec![consumer(10, None, Some((vec![], 5)))], vec![dest(1)]);
    let copies = multicast(&input(), &sink, &desc()).unwrap();
    assert_eq!(texts(&copies[0].2), vec!["1,a", "2,b", "3,c"]);
    assert_eq!(copies[0].2.slots, vec![1, 2]);
}

#[test]
fn output_exprs_need_a_tuple() {
    let mut c = consumer(10, None, None);
    c.output_exprs = Some(vec![slot(2, varchar())]);
    let sink = TMultiCastDataStreamSink::new(vec![c], vec![dest(1)]);
    match multicast(&input(), &sink, &desc()) {
        Err(ExecError::BadRequest(m)) => assert!(m.contains("output tuple"), "{m}"),
        other => panic!("expected a BadRequest, got {other:?}"),
    }
}

#[test]
fn two_consumers_into_one_instance_stay_apart() {
    // both consumers feed instance 1, through exchange nodes 10 and 20
    let sink = TMultiCastDataStreamSink::new(
        vec![
            consumer(10, Some(vec![binary("eq", slot(1, ty(T::INT)), int(1, ty(T::INT)))]), None),
            consumer(20, Some(vec![binary("gt", slot(1, ty(T::INT)), int(1, ty(T::INT)))]), None),
        ],
        vec![dest(1), dest(1)],
    );
    let exchanges = Exchanges::default();
    let query = (7, 7);
    for (s, d, copy) in multicast(&input(), &sink, &desc()).unwrap() {
        send_stream(&exchanges, query, &s, &d, copy, 0).unwrap();
    }
    let got = |node: i32| {
        let chunks = exchanges
            .receive(
                &ExchangeKey {
                    query,
                    instance: (1, 1),
                    node,
                },
                1,
            )
            .unwrap();
        chunks.iter().flat_map(texts).collect::<Vec<_>>()
    };
    assert_eq!(got(10), vec!["1,a"]);
    assert_eq!(got(20), vec!["2,b", "3,c"]);
}
