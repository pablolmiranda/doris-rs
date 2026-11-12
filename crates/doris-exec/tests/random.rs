//! `random`/`rand` and `sleep` as the C++ BE computes them (`exprs/function/random.cpp`,
//! `function_utility.cpp`): see `docs/superpowers/research/2026-11-12-m6-s7b6-random.md`.
//! Seeded values are the recorded rows (mt19937_64, `(double)g() / 2^64`).
use doris_exec::block::Block;
use doris_exec::eval::{check_constants, check_read, eval_column, SlotMap};
use doris_exec::value::ColType;
use doris_exec::ExecError;
use doris_fixtures::build::{call, int, null, slot, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::TPrimitiveType as T;

fn rows(n: usize) -> (Block, SlotMap) {
    let c = ColType::from_type_desc("", &ty(T::INT), true).unwrap();
    let block = Block {
        types: vec![c],
        rows: (0..n).map(|i| vec![Datum::Int(i as i128)]).collect(),
    };
    (block, SlotMap([(1, 0)].into_iter().collect()))
}

fn eval_rows(e: &TExpr, n: usize) -> Result<Vec<Datum>, ExecError> {
    check_read(e)?;
    let (block, slots) = rows(n);
    Ok(eval_column(e, &block, &slots)?.1)
}

fn seeded(seed: i64) -> TExpr {
    call("random", ty(T::DOUBLE), vec![int(seed, ty(T::BIGINT))])
}

#[test]
fn seeded_random_draws_as_mt19937_64() {
    let got = doris_exec::random::with_scope(|| eval_rows(&seeded(100), 2)).unwrap();
    assert_eq!(got, vec![Datum::Double(0.9616644308453555), Datum::Double(0.32358402105146866)]);
    let got = doris_exec::random::with_scope(|| eval_rows(&call("rand", ty(T::DOUBLE), vec![int(20, ty(T::BIGINT))]), 1)).unwrap();
    assert_eq!(got, vec![Datum::Double(0.715657937525516)]);
    // the seed is truncated to 32 bits
    let wide = doris_exec::random::with_scope(|| eval_rows(&seeded((1 << 32) + 100), 1)).unwrap();
    assert_eq!(wide, vec![Datum::Double(0.9616644308453555)]);
    // a NULL seed is NULL on every row
    let e = call("random", ty(T::DOUBLE), vec![null(ty(T::BIGINT))]);
    assert_eq!(
        doris_exec::random::with_scope(|| eval_rows(&e, 2)).unwrap(),
        vec![Datum::Null, Datum::Null]
    );
}

/// One generator per expression per instance: a second evaluation in the same instance goes on
/// drawing; a new instance starts over.
#[test]
fn each_instance_has_its_own_generator() {
    let e = seeded(100);
    let (a, b) = doris_exec::random::with_scope(|| (eval_rows(&e, 1).unwrap(), eval_rows(&e, 1).unwrap()));
    assert_eq!(a, vec![Datum::Double(0.9616644308453555)]);
    assert_eq!(b, vec![Datum::Double(0.32358402105146866)]);
    let c = doris_exec::random::with_scope(|| eval_rows(&e, 1).unwrap());
    assert_eq!(c, vec![Datum::Double(0.9616644308453555)]);
    // two expressions are two generators
    let (x, y) = (seeded(100), seeded(100));
    let (gx, gy) = doris_exec::random::with_scope(|| (eval_rows(&x, 1).unwrap(), eval_rows(&y, 1).unwrap()));
    assert_eq!(gx, gy);
}

#[test]
fn unseeded_random_is_in_range() {
    let got = doris_exec::random::with_scope(|| eval_rows(&call("random", ty(T::DOUBLE), vec![]), 50)).unwrap();
    assert!(got.iter().all(|d| matches!(d, Datum::Double(v) if (0.0..1.0).contains(v))), "{got:?}");
}

#[test]
fn random_range_bounds() {
    let range = |a: i64, b: i64| call("random", ty(T::BIGINT), vec![int(a, ty(T::BIGINT)), int(b, ty(T::BIGINT))]);
    let got = doris_exec::random::with_scope(|| eval_rows(&range(-5, -3), 200)).unwrap();
    assert!(got.iter().all(|d| matches!(d, Datum::Int(v) if (-5..=-3).contains(v))), "{got:?}");
    // the upper bound is reachable
    assert!(got.contains(&Datum::Int(-3)));
    for (a, b) in [(10, 1), (3, 3)] {
        match doris_exec::random::with_scope(|| eval_rows(&range(a, b), 1)) {
            Err(ExecError::Coded { name, msg, .. }) => {
                assert_eq!(name, "INVALID_ARGUMENT");
                assert_eq!(msg, format!("random's lower bound should less than upper bound, but got [{a}, {b})"));
            }
            other => panic!("{other:?}"),
        }
    }
    // raised when the rows run, not at prepare
    check_constants(&range(10, 1)).unwrap();
}

#[test]
fn sleep_returns_zero() {
    let s = |e: TExpr| eval_rows(&call("sleep", ty(T::BOOLEAN), vec![e]), 1).unwrap();
    let start = std::time::Instant::now();
    assert_eq!(s(int(0, ty(T::INT))), vec![Datum::Bool(false)]);
    assert_eq!(s(int(-3, ty(T::INT))), vec![Datum::Bool(false)]);
    assert_eq!(s(null(ty(T::INT))), vec![Datum::Null]);
    assert!(start.elapsed().as_secs() < 1);
    // a row's value sleeps that many seconds
    let start = std::time::Instant::now();
    assert_eq!(
        eval_rows(&call("sleep", ty(T::BOOLEAN), vec![slot(1, ty(T::INT))]), 2).unwrap(),
        vec![Datum::Bool(false); 2]
    );
    assert!(start.elapsed().as_millis() >= 1000, "rows 0 and 1 sleep 0 s and 1 s");
    // not at prepare
    let start = std::time::Instant::now();
    check_constants(&call("sleep", ty(T::BOOLEAN), vec![int(2, ty(T::INT))])).unwrap();
    assert!(start.elapsed().as_secs() < 1);
}
