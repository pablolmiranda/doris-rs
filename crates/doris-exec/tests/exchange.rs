//! In-process exchanges: a receiver ends once every sender has ended, with every row once.
use std::sync::Arc;

use doris_exec::block::Block;
use doris_exec::exchange::{Chunk, ExchangeKey, Exchanges};
use doris_exec::value::ColType;
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType;

fn chunk(values: &[i128]) -> Chunk {
    let t = ColType {
        name: "k".into(),
        ptype: TPrimitiveType::INT,
        len: -1,
        precision: 0,
        scale: 0,
        nullable: false,
        ..ColType::default()
    };
    Chunk {
        block: Block {
            types: vec![t],
            rows: values.iter().map(|v| vec![Datum::Int(*v)]).collect(),
        },
        slots: vec![7],
    }
}

#[test]
fn exchange_waits_for_every_sender() {
    let ex = Arc::new(Exchanges::default());
    let key = ExchangeKey {
        query: (1, 2),
        instance: (1, 3),
        node: 4,
    };
    let senders: Vec<_> = (0..3i128)
        .map(|s| {
            let ex = ex.clone();
            let key = key.clone();
            std::thread::spawn(move || {
                for b in 0..2 {
                    std::thread::sleep(std::time::Duration::from_millis(20 * (3 - s as u64)));
                    ex.send(&key, chunk(&[s * 100 + b * 10, s * 100 + b * 10 + 1]));
                }
                ex.end(&key);
            })
        })
        .collect();
    let got = ex.receive(&key, 3).expect("all senders end");
    for s in senders {
        s.join().unwrap();
    }
    let mut values: Vec<i128> = got
        .iter()
        .flat_map(|c| {
            c.block.rows.iter().map(|r| match r[0] {
                Datum::Int(v) => v,
                _ => panic!(),
            })
        })
        .collect();
    values.sort();
    assert_eq!(values, vec![0, 1, 10, 11, 100, 101, 110, 111, 200, 201, 210, 211]);
    assert!(got.iter().all(|c| c.slots == vec![7]));
}

#[test]
fn a_cancelled_query_ends_its_receivers() {
    let ex = Arc::new(Exchanges::default());
    let key = ExchangeKey {
        query: (1, 2),
        instance: (1, 3),
        node: 4,
    };
    let ex2 = ex.clone();
    let k2 = key.clone();
    let waiting = std::thread::spawn(move || ex2.receive(&k2, 2));
    std::thread::sleep(std::time::Duration::from_millis(50));
    ex.cancel((1, 2), "the scan failed".into());
    let err = waiting.join().unwrap().expect_err("cancelled");
    assert!(err.to_string().contains("the scan failed"), "{err}");
    // a receiver that comes after the cancel does not wait either
    assert!(ex
        .receive(
            &ExchangeKey {
                query: (1, 2),
                instance: (1, 9),
                node: 4
            },
            1
        )
        .is_err());
}
