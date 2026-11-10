//! TABLE_FUNCTION_NODE (lateral view) as `table_function_operator.cpp` builds its rows, with
//! `explode` and `explode_numbers`: see `docs/superpowers/research/2026-11-10-m6-s7b2-tablefn.md`.
use doris_exec::table_function::{check, expand, values};
use doris_fixtures::build::{array_literal, array_of, call, int, slot, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::plan_nodes::TTableFunctionNode;
use doris_thrift::gen::types::TPrimitiveType as T;

fn i(v: i128) -> Datum {
    Datum::Int(v)
}

#[test]
fn explode_numbers_counts_from_zero() {
    assert_eq!(values("explode_numbers", &i(3)), vec![i(0), i(1), i(2)]);
    assert_eq!(values("explode_numbers", &i(0)), vec![]);
    assert_eq!(values("explode_numbers", &i(-2)), vec![]);
}

#[test]
fn empty_or_null_input_gives_no_rows() {
    assert_eq!(values("explode_numbers", &Datum::Null), vec![]);
    assert_eq!(values("explode", &Datum::Null), vec![]);
    assert_eq!(values("explode", &Datum::Array(vec![])), vec![]);
    // NULL elements are rows
    assert_eq!(values("explode", &Datum::Array(vec![i(1), Datum::Null])), vec![i(1), Datum::Null]);
    // a child row whose function gives nothing is dropped
    let rows = vec![vec![i(10)], vec![i(20)]];
    let lists = vec![vec![vec![], vec![i(7)]]];
    assert_eq!(expand(&rows, &lists, &[true], &[Datum::Null]), vec![vec![i(20), i(7)]]);
}

#[test]
fn two_functions_nest_last_fastest() {
    let rows = vec![vec![i(10)]];
    let lists = vec![vec![vec![i(1), i(2)]], vec![vec![i(7), i(8)]]];
    assert_eq!(
        expand(&rows, &lists, &[true], &[Datum::Null]),
        vec![
            vec![i(10), i(1), i(7)],
            vec![i(10), i(1), i(8)],
            vec![i(10), i(2), i(7)],
            vec![i(10), i(2), i(8)]
        ]
    );
}

#[test]
fn unlisted_child_slots_are_their_default() {
    // child slot 0 is not in outputSlotIds: NULL if nullable, else the type's zero
    let rows = vec![vec![i(10), i(11)]];
    let lists = vec![vec![vec![i(1)]]];
    assert_eq!(
        expand(&rows, &lists, &[false, false], &[Datum::Null, i(0)]),
        vec![vec![Datum::Null, i(0), i(1)]]
    );
}

fn node(fns: Vec<doris_thrift::gen::exprs::TExpr>) -> TTableFunctionNode {
    TTableFunctionNode::new(fns, vec![1], vec![])
}

#[test]
fn outer_is_refused() {
    let e = |name: &str| call(name, ty(T::INT), vec![int(3, ty(T::INT))]);
    check(&node(vec![e("explode_numbers")])).unwrap();
    let err = check(&node(vec![e("explode_numbers_outer")])).unwrap_err().to_string();
    assert!(err.contains("explode_numbers_outer"), "{err}");
    let t = ty(T::TINYINT);
    check(&node(vec![call(
        "explode",
        t.clone(),
        vec![array_literal(t.clone(), vec![int(1, t.clone())])],
    )]))
    .unwrap();
    // a multi-argument explode zips into a STRUCT
    let arr = || slot(2, array_of(ty(T::INT)));
    assert!(check(&node(vec![call("explode", ty(T::INT), vec![arr(), arr()])])).is_err());
    // expand_conjuncts (LEFT JOIN LATERAL) are refused
    let mut n = node(vec![e("explode_numbers")]);
    n.expand_conjuncts = Some(vec![doris_fixtures::build::boolean(true)]);
    assert!(check(&n).is_err());
}
