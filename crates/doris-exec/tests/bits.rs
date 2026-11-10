//! Bit functions as the C++ BE computes them (`be/src/exprs/function/function_bit*.cpp`,
//! `functions_logical.h`): see `docs/superpowers/research/2026-11-10-m6-s7b2-dates-bits.md`.
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_fixtures::build::{boolean, call, int, null, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::{TExpr, TExprNodeType};
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn one_row(e: TExpr) -> Datum {
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    check_read(&e).expect("supported");
    let (_, v) = eval_column(&e, &block, &SlotMap::default()).expect("eval");
    v.into_iter().next().unwrap()
}

/// An ARITHMETIC_EXPR, as the FE sends `bitand`, `bitor`, `bitxor` and `bitnot`.
fn arith(name: &str, ret: TTypeDesc, args: Vec<TExpr>) -> TExpr {
    let mut e = call(name, ret, args);
    e.nodes[0].node_type = TExprNodeType::ARITHMETIC_EXPR;
    e
}

fn i(v: i128) -> Datum {
    Datum::Int(v)
}

#[test]
fn bit_ops_keep_the_operand_type() {
    let t = ty(T::TINYINT);
    assert_eq!(one_row(arith("bitand", t.clone(), vec![int(-1, t.clone()), int(5, t.clone())])), i(5));
    assert_eq!(one_row(arith("bitor", t.clone(), vec![int(3, t.clone()), int(4, t.clone())])), i(7));
    // tinyint 0x0f ^ -1 = -16
    assert_eq!(one_row(arith("bitxor", t.clone(), vec![int(15, t.clone()), int(-1, t.clone())])), i(-16));
    let b = ty(T::BIGINT);
    assert_eq!(one_row(arith("bitnot", b.clone(), vec![int(0, b.clone())])), i(-1));
    let l = ty(T::LARGEINT);
    assert_eq!(
        one_row(arith("bitand", l.clone(), vec![int(-1, l.clone()), int(i64::MAX, l.clone())])),
        i(i64::MAX as i128)
    );
    assert_eq!(one_row(arith("bitand", b.clone(), vec![null(b.clone()), int(1, b.clone())])), Datum::Null);
}

#[test]
fn bit_count_of_negative_counts_twos_complement() {
    let b = ty(T::BIGINT);
    assert_eq!(one_row(call("bit_count", ty(T::TINYINT), vec![int(-1, b.clone())])), i(64));
    assert_eq!(one_row(call("bit_count", ty(T::TINYINT), vec![int(10, b.clone())])), i(2));
    assert_eq!(one_row(call("bit_count", ty(T::TINYINT), vec![int(-1, ty(T::INT))])), i(32));
}

fn shift(name: &str, v: i64, by: i64) -> Datum {
    one_row(call(name, ty(T::BIGINT), vec![int(v, ty(T::BIGINT)), int(by, ty(T::TINYINT))]))
}

#[test]
fn shift_by_64_or_negative_is_zero() {
    assert_eq!(shift("bit_shift_left", 1, 64), i(0));
    assert_eq!(shift("bit_shift_left", 1, 127), i(0));
    assert_eq!(shift("bit_shift_left", 1, -128), i(0));
    assert_eq!(shift("bit_shift_right", 1, -128), i(0));
    assert_eq!(shift("bit_shift_left", 1, 63), i(i64::MIN as i128));
    assert_eq!(shift("bit_shift_left", i64::MAX, 1), i(-2));
}

#[test]
fn shift_right_is_logical() {
    assert_eq!(shift("bit_shift_right", -1, 1), i(i64::MAX as i128));
    assert_eq!(shift("bit_shift_right", i64::MIN, 63), i(1));
    assert_eq!(shift("bit_shift_right", i64::MIN, 64), i(0));
}

fn bit_test(t: T, args: &[i64]) -> Datum {
    one_row(call("bit_test", ty(T::TINYINT), args.iter().map(|a| int(*a, ty(t))).collect()))
}

#[test]
fn bit_test_needs_every_bit() {
    // 6 = 0b110
    assert_eq!(bit_test(T::INT, &[6, 1]), i(1));
    assert_eq!(bit_test(T::INT, &[6, 0]), i(0));
    assert_eq!(bit_test(T::INT, &[6, 1, 2]), i(1));
    assert_eq!(bit_test(T::INT, &[6, 0, 1, 2]), i(0));
    // an arithmetic shift: -1 has every bit set
    assert_eq!(bit_test(T::LARGEINT, &[-1, 127]), i(1));
}

#[test]
fn bit_test_past_the_width_is_zero() {
    assert_eq!(bit_test(T::TINYINT, &[-1, 8]), i(0));
    assert_eq!(bit_test(T::INT, &[-1, 32]), i(0));
    assert_eq!(bit_test(T::INT, &[-1, -1]), i(0));
    let e = call("bit_test", ty(T::TINYINT), vec![int(6, ty(T::INT)), null(ty(T::INT))]);
    assert_eq!(one_row(e), Datum::Null);
}

#[test]
fn xor_with_null_is_null() {
    let b = ty(T::BOOLEAN);
    let x = |l: TExpr, r: TExpr| one_row(call("xor", b.clone(), vec![l, r]));
    assert_eq!(x(boolean(true), boolean(false)), Datum::Bool(true));
    assert_eq!(x(boolean(true), boolean(true)), Datum::Bool(false));
    assert_eq!(x(null(b.clone()), boolean(false)), Datum::Null);
    assert_eq!(x(boolean(false), null(b.clone())), Datum::Null);
}
