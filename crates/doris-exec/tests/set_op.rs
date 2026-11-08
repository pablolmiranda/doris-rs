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
