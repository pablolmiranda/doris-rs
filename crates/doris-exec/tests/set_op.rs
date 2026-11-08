//! INTERSECT and EXCEPT over rows, as the C++ BE's set operators give them: DISTINCT, NULL equal
//! to NULL, floats keyed by their bits, a one-column NULL row first.
use doris_exec::set_op::{set_rows, SetKind};
use doris_segment::writer::Datum;

const N: Datum = Datum::Null;

fn i(v: i128) -> Datum {
    Datum::Int(v)
}

fn col(values: &[Datum]) -> Vec<Vec<Datum>> {
    values.iter().map(|v| vec![v.clone()]).collect()
}

#[test]
fn intersect_keeps_distinct_rows_found_in_every_child() {
    let got = set_rows(
        SetKind::Intersect,
        vec![col(&[i(3), i(1), i(1), i(2)]), col(&[i(1), i(3), i(4)]), col(&[i(3), i(1), i(5)])],
    );
    assert_eq!(got, col(&[i(3), i(1)]));
}

#[test]
fn except_keeps_rows_found_in_no_other_child() {
    let got = set_rows(SetKind::Except, vec![col(&[i(3), i(1), i(1), i(2), i(5)]), col(&[i(1)]), col(&[i(5)])]);
    assert_eq!(got, col(&[i(3), i(2)]));
}

#[test]
fn null_equals_null_and_comes_first() {
    // test_pullup_predicate_null #13: {2,6,NULL} INTERSECT {4,NULL,2} is NULL, 2
    let got = set_rows(SetKind::Intersect, vec![col(&[i(2), i(6), N]), col(&[i(4), N, i(2)])]);
    assert_eq!(got, col(&[N, i(2)]));
}

#[test]
fn nulls_in_wider_rows_keep_first_seen_order() {
    let a = vec![i(1), N];
    let b = vec![N, i(1)];
    let got = set_rows(
        SetKind::Intersect,
        vec![vec![a.clone(), b.clone(), a.clone()], vec![b.clone(), a.clone()]],
    );
    assert_eq!(got, vec![a.clone(), b.clone()]);
    let got = set_rows(SetKind::Except, vec![vec![a.clone(), b.clone()], vec![b]]);
    assert_eq!(got, vec![a]);
}

#[test]
fn negative_zero_is_its_own_row() {
    let children = || vec![col(&[Datum::Double(-0.0), Datum::Double(0.0)]), col(&[Datum::Double(0.0)])];
    let except = set_rows(SetKind::Except, children());
    assert_eq!(except.len(), 1);
    match except[0][0] {
        Datum::Double(d) => assert!(d == 0.0 && d.is_sign_negative(), "{d}"),
        ref other => panic!("{other:?}"),
    }
    let intersect = set_rows(SetKind::Intersect, children());
    assert_eq!(intersect.len(), 1);
    match intersect[0][0] {
        Datum::Double(d) => assert!(d == 0.0 && d.is_sign_positive(), "{d}"),
        ref other => panic!("{other:?}"),
    }
}

#[test]
fn an_empty_child_zero_gives_nothing() {
    for kind in [SetKind::Intersect, SetKind::Except] {
        assert!(set_rows(kind, vec![vec![], col(&[i(1)])]).is_empty());
    }
}

#[test]
fn nan_is_one_row_and_float_keeps_its_sign() {
    let d = Datum::Double;
    let got = set_rows(SetKind::Except, vec![col(&[d(f64::NAN), d(1.0)]), col(&[d(f64::NAN)])]);
    assert_eq!(got, col(&[d(1.0)]));
    let got = set_rows(SetKind::Except, vec![col(&[d(f64::NAN)]), col(&[d(0.0), d(-0.0)])]);
    assert!(matches!(got.as_slice(), [r] if matches!(r[0], Datum::Double(x) if x.is_nan())), "{got:?}");
    let f = Datum::Float;
    let got = set_rows(SetKind::Except, vec![col(&[f(-0.0), f(0.0)]), col(&[f(0.0)])]);
    assert!(
        matches!(got.as_slice(), [r] if matches!(r[0], Datum::Float(x) if x == 0.0 && x.is_sign_negative())),
        "{got:?}"
    );
}

#[test]
fn a_wide_null_row_keeps_its_place() {
    // only a one-column NULL row goes first
    let one = vec![i(1), i(1)];
    let nulls = vec![N, N];
    let got = set_rows(
        SetKind::Intersect,
        vec![vec![one.clone(), nulls.clone()], vec![nulls.clone(), one.clone()]],
    );
    assert_eq!(got, vec![one, nulls]);
}

#[test]
fn keys_do_not_run_together() {
    let b = |s: &str| Datum::Bytes(s.as_bytes().to_vec());
    assert!(set_rows(SetKind::Intersect, vec![vec![vec![b("ab"), b("c")]], vec![vec![b("a"), b("bc")]]]).is_empty());
    assert!(set_rows(SetKind::Intersect, vec![col(&[Datum::Bool(true)]), col(&[i(1)])]).is_empty());
    assert_eq!(
        set_rows(SetKind::Intersect, vec![col(&[Datum::Bool(true)]), col(&[Datum::Bool(true)])]),
        col(&[Datum::Bool(true)])
    );
}
