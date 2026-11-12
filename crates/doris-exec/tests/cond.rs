//! `if`, `ifnull`, `coalesce` as the C++ BE selects (be/src/exprs/vcondition_expr.cpp).
use doris_exec::cond::{coalesce, if_};
use doris_segment::writer::Datum;

fn ints(v: &[Option<i128>]) -> Vec<Datum> {
    v.iter().map(|x| x.map_or(Datum::Null, Datum::Int)).collect()
}

#[test]
fn if_null_condition_takes_else() {
    let c = vec![Datum::Bool(true), Datum::Null, Datum::Bool(false)];
    let out = if_(&c, &ints(&[Some(1), Some(2), Some(3)]), &ints(&[Some(7), Some(8), Some(9)]));
    assert_eq!(out, ints(&[Some(1), Some(8), Some(9)]));
}

#[test]
fn coalesce_takes_first_non_null() {
    let a = ints(&[None, None, Some(3)]);
    let b = ints(&[Some(1), None, Some(4)]);
    assert_eq!(coalesce(&[&a, &b]), ints(&[Some(1), None, Some(3)]));
}

#[test]
fn ifnull_keeps_nan() {
    let a = vec![Datum::Double(f64::NAN), Datum::Null];
    let b = vec![Datum::Double(1.0), Datum::Double(2.0)];
    let out = coalesce(&[&a, &b]);
    assert!(matches!(out[0], Datum::Double(x) if x.is_nan()));
    assert_eq!(out[1], Datum::Double(2.0));
}

#[test]
fn coalesce_three_args() {
    let a = ints(&[None, None, None]);
    let b = ints(&[None, Some(5), None]);
    let c = ints(&[Some(9), Some(6), None]);
    assert_eq!(coalesce(&[&a, &b, &c]), ints(&[Some(9), Some(5), None]));
}
