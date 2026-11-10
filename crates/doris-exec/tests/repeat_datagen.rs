//! REPEAT_NODE (grouping sets) and DATA_GEN_SCAN_NODE (`numbers`), as `repeat_operator.cpp` and
//! `vnumbers_tvf.cpp` produce them.
use std::collections::BTreeSet;

use doris_exec::datagen::{check_numbers_tuple, numbers, Numbers, MAX_NUMBERS};
use doris_exec::repeat::{check_repeat, repeat_rows};
use doris_segment::writer::Datum;
use doris_thrift::gen::plan_nodes::TRepeatNode;

fn i(v: i128) -> Datum {
    Datum::Int(v)
}

/// A repeat node over output slots 10, 11 (exprs) with `all_slot_ids` {10, 11}.
fn node(kept: Vec<Vec<i32>>, grouping: Vec<Vec<i64>>, all: &[i32]) -> TRepeatNode {
    let r = kept.len();
    TRepeatNode::new(
        1,
        kept.into_iter().map(|k| k.into_iter().collect::<BTreeSet<_>>()).collect(),
        (0..r as i64).collect(),
        grouping,
        all.iter().copied().collect(),
        vec![],
    )
}

#[test]
fn repeat_emits_the_input_once_per_repeat() {
    let n = node(vec![vec![10, 11], vec![10], vec![]], vec![vec![0, 1, 3]], &[10, 11]);
    let values = vec![vec![i(1), i(2)], vec![i(5), i(6)]];
    let got = repeat_rows(&n, &[10, 11], &values, 2).unwrap();
    assert_eq!(
        got,
        vec![
            vec![i(1), i(2), i(0)],
            vec![i(5), i(6), i(0)],
            vec![i(1), Datum::Null, i(1)],
            vec![i(5), Datum::Null, i(1)],
            vec![Datum::Null, Datum::Null, i(3)],
            vec![Datum::Null, Datum::Null, i(3)],
        ]
    );
}

#[test]
fn repeat_keeps_slots_outside_the_grouping_slots() {
    // slot 11 is not a grouping slot: copied in every repeat
    let n = node(vec![vec![], vec![10]], vec![vec![1, 0], vec![1, 1]], &[10]);
    let got = repeat_rows(&n, &[10, 11], &[vec![i(7), i(8)]], 1).unwrap();
    assert_eq!(got, vec![vec![Datum::Null, i(8), i(1), i(1)], vec![i(7), i(8), i(0), i(1)]]);
}

#[test]
fn repeat_with_no_exprs_outputs_grouping_columns_only() {
    let n = node(vec![vec![], vec![], vec![]], vec![vec![0, 1, 2]], &[]);
    let got = repeat_rows(&n, &[], &[vec![], vec![]], 2).unwrap();
    assert_eq!(got, vec![vec![i(0)], vec![i(0)], vec![i(1)], vec![i(1)], vec![i(2)], vec![i(2)]]);
}

#[test]
fn repeat_refuses_a_short_grouping_list() {
    let n = node(vec![vec![], vec![]], vec![vec![0]], &[]);
    let e = check_repeat(&n).unwrap_err().to_string();
    assert_eq!(e, "grouping_list size 1 is less than repeat_id_list size 2");
}

#[test]
fn numbers_counts_from_zero() {
    assert_eq!(
        numbers(&Numbers {
            total: 4,
            use_const: false,
            const_value: 0
        })
        .unwrap(),
        vec![i(0), i(1), i(2), i(3)]
    );
    assert_eq!(
        numbers(&Numbers {
            total: 0,
            use_const: false,
            const_value: 0
        })
        .unwrap(),
        vec![]
    );
}

#[test]
fn numbers_with_const_value_repeats_it() {
    assert_eq!(
        numbers(&Numbers {
            total: 3,
            use_const: true,
            const_value: 9
        })
        .unwrap(),
        vec![i(9), i(9), i(9)]
    );
}

#[test]
fn repeat_refuses_fewer_kept_sets_than_repeats() {
    let mut n = node(vec![vec![], vec![]], vec![vec![0, 1]], &[]);
    n.slot_id_set_list.pop();
    assert!(check_repeat(&n).is_err());
}

#[test]
fn numbers_past_what_one_chunk_holds_is_refused() {
    assert!(numbers(&Numbers { total: MAX_NUMBERS + 1, use_const: false, const_value: 0 }).is_err());
}

#[test]
fn numbers_needs_one_non_null_bigint_slot() {
    use doris_exec::value::ColType;
    use doris_thrift::gen::types::TPrimitiveType as T;
    let col = |ptype, nullable| ColType { name: "number".into(), ptype, len: -1, precision: 0, scale: 0, nullable };
    assert!(check_numbers_tuple(&[col(T::BIGINT, false)]).is_ok());
    assert!(check_numbers_tuple(&[col(T::BIGINT, true)]).is_err());
    assert!(check_numbers_tuple(&[col(T::INT, false)]).is_err());
    assert!(check_numbers_tuple(&[col(T::BIGINT, false), col(T::BIGINT, false)]).is_err());
}
