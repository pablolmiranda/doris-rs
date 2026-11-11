//! Math functions as the C++ BE computes them (`be/src/exprs/function/math.cpp`, `modulo.cpp`,
//! `least_greast.cpp`, `function_width_bucket.cpp`): see
//! `docs/superpowers/research/2026-11-10-m6-s7b3-math.md`. Expected doubles are the corpus's
//! (glibc, correctly rounded on every recorded input).
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::value::{parse_datetimev2, parse_datev2};
use doris_exec::ExecError;
use doris_fixtures::build::{call, date, decimal, float, int, null, scalar, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn eval(e: TExpr) -> Result<Datum, ExecError> {
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    check_read(&e)?;
    let (_, v) = eval_column(&e, &block, &SlotMap::default())?;
    Ok(v.into_iter().next().unwrap())
}

fn one_row(e: TExpr) -> Datum {
    eval(e).expect("eval")
}

fn d() -> TTypeDesc {
    ty(T::DOUBLE)
}

fn dbl(v: f64) -> TExpr {
    float(v, d())
}

fn f1(name: &str, x: f64) -> Datum {
    one_row(call(name, d(), vec![dbl(x)]))
}

fn big(v: i64) -> TExpr {
    int(v, ty(T::BIGINT))
}

/// The recorded values, not approximations of constants.
#[allow(clippy::approx_constant)]
#[test]
fn trig_matches_glibc() {
    assert_eq!(f1("cot", 1.0), Datum::Double(0.6420926159343306));
    assert_eq!(f1("cot", 10.0), Datum::Double(1.5423510453569202));
    assert_eq!(f1("cot", 1e7), Datum::Double(-2.1573538153608958));
    assert_eq!(f1("cot", 0.0), Datum::Double(f64::INFINITY));
    assert_eq!(f1("asin", 0.5), Datum::Double(0.5235987755982989));
    assert_eq!(f1("acosh", 1.1), Datum::Double(0.4435682543851154));
    assert_eq!(f1("ln", 3.0), Datum::Double(1.0986122886681098));
    let atan2 = |y: f64, x: f64| one_row(call("atan2", d(), vec![dbl(y), dbl(x)]));
    assert_eq!(atan2(1.0, 0.1), Datum::Double(1.4711276743037345));
    assert_eq!(atan2(0.1, 10.0), Datum::Double(0.00999966668666524));
    assert_eq!(atan2(123.0, -1.0), Datum::Double(1.5789262289748405));
    let pow = |x: f64, y: f64| one_row(call("pow", d(), vec![dbl(x), dbl(y)]));
    assert_eq!(pow(2.0, 0.5), Datum::Double(std::f64::consts::SQRT_2));
    assert_eq!(pow(1.0, 0.2), Datum::Double(1.0));
    assert_eq!(pow(3.0, 2.0), Datum::Double(9.0));
    let Datum::Double(v) = pow(-8.0, 0.2) else { panic!() };
    assert!(v.is_nan());
}

#[test]
fn out_of_domain_is_null() {
    assert_eq!(f1("asin", 2.0), Datum::Null);
    assert_eq!(f1("acos", -1.5), Datum::Null);
    assert_eq!(f1("acosh", 0.5), Datum::Null);
    assert_eq!(f1("atanh", 1.0), Datum::Null);
    assert_eq!(f1("sqrt", -1.0), Datum::Null);
    assert_eq!(f1("dsqrt", -1.0), Datum::Null);
    assert_eq!(f1("ln", 0.0), Datum::Null);
    assert_eq!(f1("sqrt", 4.0), Datum::Double(2.0));
    let Datum::Double(z) = f1("sqrt", -0.0) else { panic!() };
    assert!(z == 0.0 && z.is_sign_negative());
    // NaN in gives NaN, not NULL
    let Datum::Double(v) = f1("asin", f64::NAN) else { panic!() };
    assert!(v.is_nan());
    // a NULL argument is NULL
    assert_eq!(one_row(call("asin", d(), vec![null(d())])), Datum::Null);
}

#[test]
fn factorial_past_20_is_null() {
    let f = |n: i64| one_row(call("factorial", ty(T::BIGINT), vec![big(n)]));
    assert_eq!(f(20), Datum::Int(2432902008176640000));
    assert_eq!(f(0), Datum::Int(1));
    assert_eq!(f(21), Datum::Null);
    assert_eq!(f(-1), Datum::Null);
}

#[test]
fn pmod_takes_the_divisor_sign() {
    let p = |a: i64, b: i64| one_row(call("pmod", ty(T::BIGINT), vec![big(a), big(b)]));
    assert_eq!(p(-7, 3), Datum::Int(2));
    assert_eq!(p(7, -3), Datum::Int(-2));
    assert_eq!(p(7, 3), Datum::Int(1));
    assert_eq!(p(1, 0), Datum::Null);
}

#[test]
fn pmod_min_by_minus_one_errors() {
    match eval(call("pmod", ty(T::BIGINT), vec![big(i64::MIN), big(-1)])) {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "INVALID_ARGUMENT");
            assert_eq!(
                msg,
                "Division of minimal signed number by minus one is an undefined behavior, -9223372036854775808 % -1. "
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn least_with_null_is_null() {
    let i = |v: i64| int(v, ty(T::INT));
    assert_eq!(one_row(call("least", ty(T::INT), vec![i(3), i(5), i(1)])), Datum::Int(1));
    assert_eq!(one_row(call("greatest", ty(T::INT), vec![i(3), i(5), i(1)])), Datum::Int(5));
    assert_eq!(one_row(call("least", ty(T::INT), vec![i(3), null(ty(T::INT)), i(1)])), Datum::Null);
}

#[test]
fn bin_of_negative_is_64_bits() {
    let b = |v: i64| one_row(call("bin", scalar(T::VARCHAR, 65533, 0, 0), vec![big(v)]));
    assert_eq!(b(2), Datum::Bytes(b"10".to_vec()));
    assert_eq!(b(0), Datum::Bytes(b"0".to_vec()));
    assert_eq!(b(-1), Datum::Bytes(vec![b'1'; 64]));
}

#[test]
fn width_bucket_buckets() {
    let t = || ty(T::TINYINT);
    let w = |x: i64, lo: i64, hi: i64, n: i64| {
        one_row(call(
            "width_bucket",
            ty(T::BIGINT),
            vec![int(x, t()), int(lo, t()), int(hi, t()), int(n, t())],
        ))
    };
    assert_eq!(w(1, 2, 3, 2), Datum::Int(0));
    assert_eq!(w(6, 2, 6, 4), Datum::Int(5));
    assert_eq!(w(3, 2, 6, 4), Datum::Int(2));
    // (max - min) / n is an integer division for integer types: a range below n gives 0
    assert_eq!(w(2, 1, 3, 4), Datum::Int(0));
    let wd = |x: f64, lo: f64, hi: f64| one_row(call("width_bucket", ty(T::BIGINT), vec![dbl(x), dbl(lo), dbl(hi), int(2, t())]));
    assert_eq!(wd(20221118.0, 20221117.0, 20221119.0), Datum::Int(2));
    match eval(call(
        "width_bucket",
        ty(T::BIGINT),
        vec![int(1, t()), int(0, t()), int(5, t()), int(0, t())],
    )) {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "INTERNAL_ERROR");
            assert_eq!(msg, "The desired number(0) of buckets must be a positive integer value.");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn date_casts_to_yyyymmdd() {
    let cast = |e: TExpr, to: TTypeDesc| {
        let mut c = call("cast", to.clone(), vec![e]);
        c.nodes[0].node_type = doris_thrift::gen::exprs::TExprNodeType::CAST_EXPR;
        c.nodes[0].fn_ = None;
        one_row(c)
    };
    assert!(parse_datev2("2022-11-18").is_some() && parse_datetimev2("2022-11-18 01:02:03", 0).is_some());
    assert_eq!(cast(date("2022-11-18", ty(T::DATEV2)), d()), Datum::Double(20221118.0));
    let dt = scalar(T::DATETIMEV2, -1, 18, 0);
    assert_eq!(cast(date("2022-11-18 01:02:03", dt), ty(T::BIGINT)), Datum::Int(20221118010203));
}

#[test]
fn abs_of_decimal_keeps_its_type() {
    let t = scalar(T::DECIMAL128I, -1, 38, 6);
    assert_eq!(
        one_row(call("abs", t.clone(), vec![decimal("-1.500000", t.clone())])),
        Datum::Int(1500000)
    );
}
