//! Read-side expressions: predicates over blocks, with SQL's three-valued logic.
use std::collections::HashMap;

use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, filter, SlotMap};
use doris_exec::value::{parse_datetimev2, parse_datev2, ColType};
use doris_fixtures::build::{
    binary, boolean, call, compound, date, decimal, float, in_list, int, null, scalar, slot,
    string, ty,
};
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

fn col(name: &str, ptype: T, precision: i32, scale: i32) -> ColType {
    ColType {
        name: name.into(),
        ptype,
        len: -1,
        precision,
        scale,
        nullable: true,
    }
}

/// Slots 1..=6: INT, DECIMAL32(9,2), DATEV2, DATETIMEV2(3), VARCHAR, DOUBLE; the last row is all NULL.
fn typed_block() -> (Block, SlotMap) {
    let types = vec![
        col("i", T::INT, 0, 0),
        col("d", T::DECIMAL32, 9, 2),
        col("dt", T::DATEV2, 0, 0),
        col("ts", T::DATETIMEV2, 0, 3),
        col("s", T::VARCHAR, 0, 0),
        col("f", T::DOUBLE, 0, 0),
    ];
    let row = |i: i128, d: i128, dt: &str, ts: &str, s: &str, f: f64| {
        vec![
            Datum::Int(i),
            Datum::Int(d),
            Datum::Int(parse_datev2(dt).unwrap().into()),
            Datum::Int(parse_datetimev2(ts, 3).unwrap().into()),
            Datum::Bytes(s.as_bytes().to_vec()),
            Datum::Double(f),
        ]
    };
    let rows = vec![
        row(1, 150, "2020-01-01", "2020-01-01 00:00:00.123", "a", 1.5),
        row(2, 225, "2021-06-15", "2021-06-15 10:00:00.000", "b", 2.5),
        vec![Datum::Null; 6],
    ];
    let slots = SlotMap(
        (1..=6)
            .map(|id| (id, id as usize - 1))
            .collect::<HashMap<_, _>>(),
    );
    (Block { types, rows }, slots)
}

fn kept(conjunct: doris_thrift::gen::exprs::TExpr) -> Vec<usize> {
    let (block, slots) = typed_block();
    check_read(&conjunct).expect("supported");
    let firsts: Vec<Datum> = block.rows.iter().map(|r| r[0].clone()).collect();
    let out = filter(&[conjunct], block, &slots).expect("filter");
    out.rows
        .iter()
        .map(|r| firsts.iter().position(|f| *f == r[0]).unwrap())
        .collect()
}

#[test]
fn comparisons_per_type() {
    let dec = scalar(T::DECIMAL32, -1, 9, 2);
    assert_eq!(
        kept(binary("eq", slot(1, ty(T::INT)), int(2, ty(T::INT)))),
        vec![1]
    );
    assert_eq!(
        kept(binary("ne", slot(1, ty(T::INT)), int(2, ty(T::INT)))),
        vec![0]
    );
    assert_eq!(
        kept(binary(
            "lt",
            slot(2, dec.clone()),
            decimal("2", dec.clone())
        )),
        vec![0]
    );
    // decimals compare at their common scale: 2.25 < 2.3
    assert_eq!(
        kept(binary(
            "lt",
            slot(2, dec),
            decimal("2.3", scalar(T::DECIMAL32, -1, 9, 1))
        )),
        vec![0, 1]
    );
    assert_eq!(
        kept(binary(
            "ge",
            slot(3, ty(T::DATEV2)),
            date("2021-01-01", ty(T::DATEV2))
        )),
        vec![1]
    );
    let ts = scalar(T::DATETIMEV2, -1, 0, 3);
    assert_eq!(
        kept(binary(
            "ge",
            slot(4, ts.clone()),
            date("2020-01-01 00:00:00.123", ts)
        )),
        vec![0, 1]
    );
    assert_eq!(
        kept(binary("eq", slot(5, ty(T::VARCHAR)), string("a"))),
        vec![0]
    );
    assert_eq!(
        kept(binary(
            "lt",
            slot(6, ty(T::DOUBLE)),
            float(2.0, ty(T::DOUBLE))
        )),
        vec![0]
    );
    assert_eq!(
        kept(binary("le", slot(1, ty(T::INT)), int(2, ty(T::INT)))),
        vec![0, 1]
    );
    assert_eq!(
        kept(binary("gt", slot(1, ty(T::INT)), int(1, ty(T::INT)))),
        vec![1]
    );
}

fn one_row(e: doris_thrift::gen::exprs::TExpr) -> Datum {
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    check_read(&e).expect("supported");
    let (_, v) = eval_column(&e, &block, &SlotMap::default()).expect("eval");
    v.into_iter().next().unwrap()
}

#[test]
fn three_valued_logic() {
    let b = ty(T::BOOLEAN);
    assert_eq!(
        one_row(compound("and", vec![null(b.clone()), boolean(false)])),
        Datum::Bool(false)
    );
    assert_eq!(
        one_row(compound("and", vec![null(b.clone()), boolean(true)])),
        Datum::Null
    );
    assert_eq!(
        one_row(compound("or", vec![null(b.clone()), boolean(true)])),
        Datum::Bool(true)
    );
    assert_eq!(
        one_row(compound("or", vec![null(b.clone()), boolean(false)])),
        Datum::Null
    );
    assert_eq!(one_row(compound("not", vec![null(b)])), Datum::Null);
    assert_eq!(
        one_row(compound("not", vec![boolean(false)])),
        Datum::Bool(true)
    );
}

#[test]
fn in_list_with_null_is_null_on_no_match() {
    let block = Block {
        types: vec![col("x", T::INT, 0, 0)],
        rows: vec![vec![Datum::Int(1)], vec![Datum::Int(2)], vec![Datum::Null]],
    };
    let slots = SlotMap(HashMap::from([(7, 0)]));
    let e = in_list(
        slot(7, ty(T::INT)),
        vec![int(1, ty(T::INT)), null(ty(T::INT))],
        false,
    );
    check_read(&e).expect("supported");
    let (t, v) = eval_column(&e, &block, &slots).expect("eval");
    assert_eq!(t.ptype, T::BOOLEAN);
    assert_eq!(v, vec![Datum::Bool(true), Datum::Null, Datum::Null]);
    let not_in = in_list(
        slot(7, ty(T::INT)),
        vec![int(1, ty(T::INT)), int(3, ty(T::INT))],
        true,
    );
    assert_eq!(
        eval_column(&not_in, &block, &slots).unwrap().1,
        vec![Datum::Bool(false), Datum::Bool(true), Datum::Null]
    );
    let out = filter(&[e], block, &slots).expect("filter");
    assert_eq!(out.rows, vec![vec![Datum::Int(1)]]);
}

#[test]
fn null_predicates() {
    let block = Block {
        types: vec![col("x", T::INT, 0, 0)],
        rows: vec![vec![Datum::Int(1)], vec![Datum::Null]],
    };
    let slots = SlotMap(HashMap::from([(7, 0)]));
    let is_null = call("is_null_pred", ty(T::BOOLEAN), vec![slot(7, ty(T::INT))]);
    let is_not_null = call(
        "is_not_null_pred",
        ty(T::BOOLEAN),
        vec![slot(7, ty(T::INT))],
    );
    assert_eq!(
        eval_column(&is_null, &block, &slots).unwrap().1,
        vec![Datum::Bool(false), Datum::Bool(true)]
    );
    assert_eq!(
        eval_column(&is_not_null, &block, &slots).unwrap().1,
        vec![Datum::Bool(true), Datum::Bool(false)]
    );
}

#[test]
fn substring_counts_characters() {
    let e = call(
        "substring",
        ty(T::VARCHAR),
        vec![string("héllo"), int(2, ty(T::INT)), int(3, ty(T::INT))],
    );
    assert_eq!(one_row(e), Datum::Bytes("éll".as_bytes().to_vec()));
}

#[test]
fn unknown_function_is_refused() {
    let e = call("like", ty(T::BOOLEAN), vec![string("a"), string("%")]);
    let err = check_read(&e)
        .expect_err("like is not supported")
        .to_string();
    assert!(err.contains("function like"), "{err}");
}

#[test]
fn datetime_casts_round_to_the_lower_scale() {
    // cast_to_datetimev2_impl.hpp transform_date_scale: half up, carrying into the second
    let from = scalar(T::DATETIMEV2, -1, 0, 6);
    let to = scalar(T::DATETIMEV2, -1, 0, 3);
    let ts = |s: &str| Datum::Int(parse_datetimev2(s, 6).unwrap().into());
    let block = Block {
        types: vec![col("t", T::DATETIMEV2, 0, 6)],
        rows: vec![
            vec![ts("2022-12-01 22:23:24.999999")],
            vec![ts("2022-12-31 23:59:59.999500")],
            vec![ts("2022-12-01 22:23:24.123449")],
            vec![ts("9999-12-31 23:59:59.999999")],
        ],
    };
    let slots = SlotMap(HashMap::from([(1, 0)]));
    let mut cast = doris_fixtures::build::blank(doris_thrift::gen::exprs::TExprNodeType::CAST_EXPR, to, 1);
    cast.opcode = Some(doris_thrift::gen::opcodes::TExprOpcode::CAST);
    let e = doris_fixtures::build::over(cast, vec![slot(1, from)]);
    let (t, v) = eval_column(&e, &block, &slots).unwrap();
    let text: Vec<Option<String>> = v.iter().map(|d| doris_exec::row_text::cell_text(d, &t).map(|b| String::from_utf8(b).unwrap())).collect();
    assert_eq!(
        text,
        vec![Some("2022-12-01 22:23:25.000".into()), Some("2023-01-01 00:00:00.000".into()), Some("2022-12-01 22:23:24.123".into()), None]
    );
}

#[test]
fn text_against_a_number_is_refused() {
    // the FE casts both sides to one type; a comparison it left mixed has no order doris-rs knows
    let (block, slots) = typed_block();
    let vs = scalar(T::VARCHAR, 10, 0, 0);
    for e in [
        binary("eq", slot(5, vs.clone()), int(1, ty(T::INT))),
        in_list(slot(1, ty(T::INT)), vec![string("1")], false),
    ] {
        let err = filter(&[e], block.clone(), &slots).expect_err("text against a number").to_string();
        assert!(err.contains("text against"), "{err}");
    }
}

/// An ARITHMETIC_EXPR, as the FE sends it: a call node of that type (decimal ones carry no opcode).
fn arith(name: &str, ret: doris_thrift::gen::types::TTypeDesc, args: Vec<doris_thrift::gen::exprs::TExpr>) -> doris_thrift::gen::exprs::TExpr {
    let mut e = call(name, ret, args);
    e.nodes[0].node_type = doris_thrift::gen::exprs::TExprNodeType::ARITHMETIC_EXPR;
    e
}

#[test]
fn arithmetic_and_abs_evaluate() {
    let big = ty(T::BIGINT);
    assert_eq!(one_row(arith("add", big.clone(), vec![int(2, big.clone()), int(3, big.clone())])), Datum::Int(5));
    let d = scalar(T::DECIMAL128I, -1, 38, 12);
    let e = arith(
        "multiply",
        d,
        vec![decimal("0.000000001", scalar(T::DECIMAL128I, -1, 27, 9)), decimal("0.00050000", scalar(T::DECIMAL64, -1, 16, 8))],
    );
    assert_eq!(one_row(e), Datum::Int(1));
    let e = call("abs", ty(T::LARGEINT), vec![int(i64::MIN, big.clone())]);
    assert_eq!(one_row(e), Datum::Int(9_223_372_036_854_775_808));
}

#[test]
fn unlisted_arithmetic_is_refused() {
    let d = scalar(T::DECIMAL128I, -1, 38, 2);
    let e = arith("add", d.clone(), vec![decimal("1.00", d.clone()), decimal("2.00", d)]);
    let err = check_read(&e).expect_err("decimal add is not supported").to_string();
    assert!(err.contains("function add"), "{err}");
}

#[test]
fn conditionals_evaluate() {
    let i = ty(T::INT);
    let e = call("if", i.clone(), vec![null(ty(T::BOOLEAN)), int(1, i.clone()), int(2, i.clone())]);
    assert_eq!(one_row(e), Datum::Int(2));
    let d = ty(T::DOUBLE);
    assert_eq!(one_row(call("ifnull", d.clone(), vec![null(d.clone()), float(1.5, d.clone())])), Datum::Double(1.5));
    let e = call("coalesce", i.clone(), vec![null(i.clone()), null(i.clone()), int(3, i.clone())]);
    assert_eq!(one_row(e), Datum::Int(3));
}
