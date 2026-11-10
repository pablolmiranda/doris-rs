//! `if`, `ifnull`, `coalesce` as the C++ BE selects (be/src/exprs/vcondition_expr.cpp).
use doris_exec::cond::{case_, coalesce, if_};
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

#[test]
fn case_first_true_when_wins() {
    let w1 = vec![Datum::Null, Datum::Bool(true), Datum::Bool(false)];
    let w2 = vec![Datum::Bool(true), Datum::Bool(true), Datum::Bool(false)];
    let t1 = ints(&[Some(1), Some(2), Some(3)]);
    let t2 = ints(&[Some(4), Some(5), Some(6)]);
    // a NULL WHEN is not a match; no match and no ELSE is NULL
    assert_eq!(case_(&[&w1, &w2], &[&t1, &t2], None, false), ints(&[Some(4), Some(2), None]));
    let e = ints(&[Some(7), Some(8), Some(9)]);
    assert_eq!(case_(&[&w1, &w2], &[&t1, &t2], Some(&e), false), ints(&[Some(4), Some(2), Some(9)]));
}

#[test]
fn case_non_nullable_double_sums() {
    let w = vec![Datum::Bool(false), Datum::Bool(true)];
    let then = vec![Datum::Double(f64::NAN), Datum::Double(-0.0)];
    let else_ = vec![Datum::Double(1.0), Datum::Double(2.0)];
    // a non-nullable DOUBLE CASE sums selected × value over every branch: an unselected NaN
    // poisons the row, and a selected -0.0 comes out +0.0
    let summed = case_(&[&w], &[&then], Some(&else_), true);
    assert!(matches!(summed[0], Datum::Double(x) if x.is_nan()), "{summed:?}");
    assert!(matches!(summed[1], Datum::Double(x) if x == 0.0 && x.is_sign_positive()), "{summed:?}");
    // nullable: the selected value as it is
    let copied = case_(&[&w], &[&then], Some(&else_), false);
    assert_eq!(copied[0], Datum::Double(1.0));
    assert!(matches!(copied[1], Datum::Double(x) if x == 0.0 && x.is_sign_negative()), "{copied:?}");
}
