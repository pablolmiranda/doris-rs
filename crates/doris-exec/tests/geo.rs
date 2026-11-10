//! `st_point` and `st_astext` as the C++ BE computes them (`exprs/function/geo/`): see
//! `docs/superpowers/research/2026-11-10-m6-s7b2-geo.md`.
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::geo::g15;
use doris_fixtures::build::{call, float, null, string, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::TPrimitiveType as T;

fn one_row(e: TExpr) -> Datum {
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    check_read(&e).expect("supported");
    let (_, v) = eval_column(&e, &block, &SlotMap::default()).expect("eval");
    v.into_iter().next().unwrap()
}

fn point(x: f64, y: f64) -> TExpr {
    let d = ty(T::DOUBLE);
    call("st_point", ty(T::VARCHAR), vec![float(x, d.clone()), float(y, d)])
}

fn astext(e: TExpr) -> Datum {
    one_row(call("st_astext", ty(T::VARCHAR), vec![e]))
}

fn text(s: &str) -> Datum {
    Datum::Bytes(s.as_bytes().to_vec())
}

#[test]
fn astext_of_point_prints_15_digits() {
    // the two rows of the suites' .out files
    assert_eq!(astext(point(24.7, 56.7)), text("POINT (24.7 56.7)"));
    assert_eq!(
        astext(point(123.123_456_789_012_35, 89.123_456_789)),
        text("POINT (123.123456789012 89.123456789)")
    );
    // the S2 round trip gives 0.9999999999999998, printed at 15 digits
    assert_eq!(astext(point(1.0, 1.0)), text("POINT (1 1)"));
    assert_eq!(astext(point(-180.0, 90.0)), text("POINT (-180 90)"));
}

#[test]
fn g15_formats() {
    assert_eq!(g15(-0.0), "-0");
    assert_eq!(g15(0.0), "0");
    assert_eq!(g15(1e-7), "1e-07");
    assert_eq!(g15(1e15), "1e+15");
    assert_eq!(g15(999_999_999_999_999.9), "1e+15");
    assert_eq!(g15(123_456.0), "123456");
    assert_eq!(g15(0.0001), "0.0001");
    assert_eq!(g15(0.000_012_34), "1.234e-05");
}

#[test]
fn out_of_range_point_is_null() {
    assert_eq!(one_row(point(180.5, 0.0)), Datum::Null);
    assert_eq!(one_row(point(0.0, -90.5)), Datum::Null);
    assert_eq!(one_row(point(f64::NAN, 0.0)), Datum::Null);
    let d = ty(T::DOUBLE);
    assert_eq!(
        one_row(call("st_point", ty(T::VARCHAR), vec![null(d.clone()), float(1.0, d)])),
        Datum::Null
    );
}

#[test]
fn a_point_is_26_bytes() {
    let Datum::Bytes(b) = one_row(point(0.0, 0.0)) else { panic!() };
    assert_eq!(b.len(), 26);
    assert_eq!(&b[..2], &[0, 1]);
    // (lng 0, lat 0) is the unit vector (1, 0, 0)
    assert_eq!(f64::from_le_bytes(b[2..10].try_into().unwrap()), 1.0);
}

#[test]
fn bad_bytes_are_null() {
    assert_eq!(astext(string("POINT (1 1)")), Datum::Null);
    assert_eq!(astext(null(ty(T::VARCHAR))), Datum::Null);
    assert_eq!(astext(string("")), Datum::Null);
    // a known header with an unknown shape type (2)
    let mut bad = vec![0u8, 2];
    bad.extend([0u8; 24]);
    assert_eq!(astext(string(&String::from_utf8(bad).unwrap())), Datum::Null);
}
