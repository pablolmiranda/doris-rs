//! The math tail as the C++ BE computes it (`be/src/exprs/function/math.cpp`,
//! `function_math_log.h`, `round.h`, `function_conv.cpp`, `modulo.cpp`): see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-math.md` ("Per function"). Expected values are
//! the rows `fold_constant_numeric_arithmatic` recorded on the reference (glibc 2.35).
#![allow(clippy::approx_constant)]
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::ExecError;
use doris_fixtures::build::{blank, call, decimal, float, int, null, over, scalar, string, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::{TExpr, TExprNodeType, TLargeIntLiteral};
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn eval(e: TExpr) -> Result<Datum, ExecError> {
    check_read(&e)?;
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    Ok(eval_column(&e, &block, &SlotMap::default())?.1.remove(0))
}

fn d() -> TTypeDesc {
    ty(T::DOUBLE)
}

fn dbl(v: f64) -> TExpr {
    float(v, d())
}

fn f1(name: &str, x: f64) -> Datum {
    eval(call(name, d(), vec![dbl(x)])).unwrap()
}

fn pi() -> TExpr {
    call("pi", d(), vec![])
}

fn of(name: &str, arg: TExpr) -> Datum {
    eval(call(name, d(), vec![arg])).unwrap()
}

fn dd(v: f64) -> Datum {
    Datum::Double(v)
}

fn dec(p: i32, s: i32) -> TTypeDesc {
    let t = match p {
        0..=9 => T::DECIMAL32,
        10..=18 => T::DECIMAL64,
        _ => T::DECIMAL128I,
    };
    scalar(t, -1, p, s)
}

fn large(v: i128) -> TExpr {
    let mut e = int(0, ty(T::LARGEINT));
    e.nodes[0].node_type = TExprNodeType::LARGE_INT_LITERAL;
    e.nodes[0].int_literal = None;
    e.nodes[0].large_int_literal = Some(TLargeIntLiteral::new(v.to_string()));
    e
}

#[test]
fn pi_and_e() {
    assert_eq!(eval(pi()).unwrap(), dd(3.141592653589793));
    assert_eq!(eval(call("e", d(), vec![])).unwrap(), dd(2.718281828459045));
}

#[test]
fn trig_matches_glibc() {
    assert_eq!(f1("atan", 1.0), dd(0.7853981633974483));
    assert_eq!(f1("atan", 1.5), dd(0.982793723247329));
    assert_eq!(f1("atan", -1e308), dd(-1.5707963267948966));
    assert_eq!(f1("atan", 1e-308), dd(1e-308));
    assert_eq!(of("atan", pi()), dd(1.2626272556789118));
    assert_eq!(of("cos", pi()), dd(-1.0));
    assert_eq!(f1("cos", 1e308), dd(-0.8913089376870335));
    assert_eq!(f1("cos", 1e-308), dd(1.0));
    assert_eq!(f1("sin", 0.0), dd(0.0));
    assert_eq!(of("sin", pi()), dd(1.2246467991473532e-16));
    assert_eq!(f1("sin", 1e308), dd(0.4533964905016491));
    assert_eq!(of("tan", pi()), dd(-1.2246467991473532e-16));
    assert_eq!(f1("tan", 0.0), dd(0.0));
}

#[test]
fn hyperbolic_and_roots_match_glibc() {
    // glibc is not correctly rounded on these: the ports must agree with it
    assert_eq!(f1("cbrt", 27.0), dd(3.0000000000000004));
    assert_eq!(f1("cbrt", -27.0), dd(-3.0000000000000004));
    assert_eq!(f1("cbrt", 8.0), dd(2.0));
    assert_eq!(f1("cbrt", 1e308), dd(4.64158883361278e+102));
    assert_eq!(f1("sinh", 2.0), dd(3.626860407847019));
    assert_eq!(f1("sinh", -1.0), dd(-1.1752011936438014));
    assert_eq!(f1("tanh", 1e-7), dd(9.999999999999965e-08));
    assert_eq!(f1("tanh", -0.5), dd(-0.46211715726000974));
    assert_eq!(f1("tanh", 10.0), dd(0.9999999958776927));
    assert_eq!(f1("tanh", 20.0), dd(1.0));
    assert_eq!(f1("cosh", 1.0), dd(1.5430806348152437));
    assert_eq!(of("cosh", pi()), dd(11.591953275521519));
    assert_eq!(f1("cosh", 709.782712893384), dd(8.988465674311366e+307));
    assert_eq!(f1("asinh", 1.0), dd(0.881373587019543));
    assert_eq!(f1("asinh", -0.5), dd(-0.48121182505960347));
    assert_eq!(f1("exp", 1.0), dd(2.718281828459045));
    assert_eq!(f1("dexp", -1.0), dd(0.36787944117144233));
    assert_eq!(f1("exp", 709.782712893384), dd(1.7976931348622732e+308));
    assert_eq!(f1("exp", -709.782712893384), dd(5.562684646268137e-309));
    assert_eq!(f1("exp", 1959859681.0), dd(f64::INFINITY));
    assert_eq!(of("exp", pi()), dd(23.140692632779267));
}

#[test]
fn degrees_radians_even_sign_signbit() {
    assert_eq!(of("degrees", pi()), dd(180.0));
    assert_eq!(f1("degrees", std::f64::consts::PI / 6.0), dd(29.999999999999996));
    assert_eq!(f1("degrees", std::f64::consts::PI / 3.0), dd(59.99999999999999));
    assert_eq!(f1("radians", 180.0), dd(3.141592653589793));
    assert_eq!(f1("radians", 1e308), dd(1.7453292519943295e+306));
    assert_eq!(f1("even", -2.5), dd(-4.0));
    assert_eq!(f1("even", 2.5), dd(4.0));
    assert_eq!(f1("even", 0.0), dd(0.0));
    let sign = |x: f64| eval(call("sign", ty(T::TINYINT), vec![dbl(x)])).unwrap();
    assert_eq!(sign(-3.14), Datum::Int(-1));
    assert_eq!(sign(0.0), Datum::Int(0));
    assert_eq!(sign(10.0), Datum::Int(1));
    assert_eq!(sign(f64::NAN), Datum::Int(1));
    let signbit = |x: f64| eval(call("signbit", ty(T::BOOLEAN), vec![dbl(x)])).unwrap();
    assert_eq!(signbit(-10.0), Datum::Bool(true));
    assert_eq!(signbit(0.0), Datum::Bool(false));
    assert_eq!(signbit(-0.0), Datum::Bool(true));
    assert_eq!(signbit(3.14), Datum::Bool(false));
    assert_eq!(eval(call("even", d(), vec![null(d())])).unwrap(), Datum::Null);
}

#[test]
fn logs_are_null_at_or_below_zero() {
    for name in ["log10", "dlog10"] {
        assert_eq!(f1(name, 100.0), dd(2.0), "{name}");
        assert_eq!(f1(name, 1e-308), dd(-308.0), "{name}");
        assert_eq!(f1(name, 0.1), dd(-1.0), "{name}");
        assert_eq!(f1(name, 0.0), Datum::Null, "{name}");
        assert_eq!(f1(name, -1.0), Datum::Null, "{name}");
        assert!(matches!(f1(name, f64::NAN), Datum::Double(v) if v.is_nan()), "{name}");
    }
    assert_eq!(f1("log2", 8.0), dd(3.0));
    assert_eq!(f1("log2", 1e308), dd(1023.1538532253076));
    assert_eq!(f1("log2", 0.0), Datum::Null);
    let log = |a: f64, b: f64| eval(call("log", d(), vec![dbl(a), dbl(b)])).unwrap();
    assert_eq!(log(100.0, 10.0), dd(0.5));
    assert_eq!(log(8.0, 2.0), dd(0.33333333333333337));
    assert_eq!(log(100.0, 1.0), dd(0.0));
    assert_eq!(log(1e308, 10.0), dd(0.003246753246753247));
    assert_eq!(log(100.0, 1e-308), dd(-154.0));
    assert_eq!(log(0.0, 10.0), Datum::Null);
    assert_eq!(log(100.0, -1.0), Datum::Null);
    assert_eq!(log(1.0, 10.0), Datum::Null);
    assert!(matches!(log(2.0, f64::NAN), Datum::Double(v) if v.is_nan()));
}

#[test]
fn fmod_by_zero_is_null() {
    let fmod = |a: f64, b: f64| eval(call("fmod", d(), vec![dbl(a), dbl(b)])).unwrap();
    assert_eq!(fmod(10.5, 3.0), dd(1.5));
    assert_eq!(fmod(-10.5, 3.0), dd(-1.5));
    assert_eq!(fmod(10.5, -3.0), dd(1.5));
    assert_eq!(fmod(10.5, 0.0), Datum::Null);
}

#[test]
fn negative_and_positive_wrap() {
    let big = |name: &str, v: i64| eval(call(name, ty(T::BIGINT), vec![int(v, ty(T::BIGINT))])).unwrap();
    assert_eq!(big("negative", 5), Datum::Int(-5));
    assert_eq!(big("negative", i64::MAX), Datum::Int(-i128::from(i64::MAX)));
    assert_eq!(big("negative", i64::MIN), Datum::Int(i128::from(i64::MIN)));
    assert_eq!(big("negative", -2147483648), Datum::Int(2147483648));
    assert_eq!(big("positive", -5), Datum::Int(-5));
    assert_eq!(f1("negative", 1e308), dd(-1e308));
    assert_eq!(f1("negative", -1e-308), dd(1e-308));
    assert_eq!(f1("positive", -1e308), dd(-1e308));
    // a decimal keeps its type
    let neg = eval(call("negative", dec(3, 2), vec![decimal("3.14", dec(3, 2))])).unwrap();
    assert_eq!(neg, Datum::Int(-314));
    let pos = eval(call("positive", dec(3, 2), vec![decimal("-3.14", dec(3, 2))])).unwrap();
    assert_eq!(pos, Datum::Int(-314));
}

#[test]
fn gcd_lcm_over_each_width() {
    let g = |name: &str, t: T, a: i64, b: i64| eval(call(name, ty(t), vec![int(a, ty(t)), int(b, ty(t))])).unwrap();
    assert_eq!(g("gcd", T::TINYINT, -2, -4), Datum::Int(2));
    assert_eq!(g("gcd", T::TINYINT, 9, 11), Datum::Int(1));
    assert_eq!(g("gcd", T::SMALLINT, -32767, 16384), Datum::Int(1));
    assert_eq!(g("gcd", T::INT, -1000000000, 500000000), Datum::Int(500000000));
    assert_eq!(g("gcd", T::BIGINT, -i64::MAX, 2), Datum::Int(1));
    assert_eq!(g("lcm", T::SMALLINT, -128, 64), Datum::Int(128));
    assert_eq!(g("lcm", T::SMALLINT, 0, 2), Datum::Int(0));
    assert_eq!(g("lcm", T::INT, -32767, 16384), Datum::Int(536854528));
    assert_eq!(g("lcm", T::INT, 127, 32767), Datum::Int(4161409));
    assert_eq!(g("lcm", T::BIGINT, -2147483647, 1073741824), Datum::Int(2305843008139952128));
    assert_eq!(g("lcm", T::BIGINT, 32767, 2147483647), Datum::Int(70366596661249));
    let l = |name: &str, a: i128, b: i128| eval(call(name, ty(T::LARGEINT), vec![large(a), large(b)])).unwrap();
    assert_eq!(l("gcd", -i128::MAX, 85070591730234615865843651857942052864), Datum::Int(1));
    assert_eq!(l("gcd", -i128::MAX, -i128::MAX), Datum::Int(i128::MAX));
    assert_eq!(l("lcm", i128::from(i64::MAX), 200000000), Datum::Int(1844674407370955161400000000));
    assert_eq!(l("lcm", 32767, i128::from(i64::MAX)), Datum::Int(43174604504517205552567));
    // overflow wraps in the type's width
    assert_eq!(g("lcm", T::SMALLINT, 32767, 32766), Datum::Int(i128::from((32767i32 * 32766) as i16)));
    assert_eq!(
        eval(call("gcd", ty(T::BIGINT), vec![int(4, ty(T::BIGINT)), null(ty(T::BIGINT))])).unwrap(),
        Datum::Null
    );
}

#[test]
fn round_bankers_ties_to_even() {
    let r = |v: &str| eval(call("round_bankers", dec(2, 0), vec![decimal(v, dec(2, 1))])).unwrap();
    assert_eq!(r("1.5"), Datum::Int(2));
    assert_eq!(r("2.5"), Datum::Int(2));
    assert_eq!(r("-1.5"), Datum::Int(-2));
    assert_eq!(r("-2.5"), Datum::Int(-2));
    assert_eq!(r("2.6"), Datum::Int(3));
    assert_eq!(f1("round_bankers", 2.5), dd(2.0));
    assert_eq!(f1("round_bankers", 3.5), dd(4.0));
    assert_eq!(f1("round_bankers", -0.5), dd(-0.0));
    let r2 = |x: f64, p: i64| eval(call("round_bankers", d(), vec![dbl(x), int(p, ty(T::INT))])).unwrap();
    assert_eq!(r2(1.25, 1), dd(1.2));
}

#[test]
fn conv_as_cpp() {
    let tiny = |v: i64| int(v, ty(T::TINYINT));
    let vc = || scalar(T::VARCHAR, 65533, 0, 0);
    let text = |d: Datum| match d {
        Datum::Bytes(b) => Some(String::from_utf8(b).unwrap()),
        Datum::Null => None,
        other => panic!("{other:?}"),
    };
    let ci = |v: i64, a: i64, b: i64| text(eval(call("conv", vc(), vec![int(v, ty(T::BIGINT)), tiny(a), tiny(b)])).unwrap());
    let cs = |v: &str, a: i64, b: i64| text(eval(call("conv", vc(), vec![string(v), tiny(a), tiny(b)])).unwrap());
    assert_eq!(ci(15, 10, 2).as_deref(), Some("1111"));
    assert_eq!(ci(1111, 2, 10).as_deref(), Some("15"));
    assert_eq!(ci(255, 10, 16).as_deref(), Some("FF"));
    assert_eq!(
        ci(-10, 10, 2).as_deref(),
        Some("1111111111111111111111111111111111111111111111111111111111110110")
    );
    // a decimal digit at or above the base ends the number
    assert_eq!(ci(1291, 2, 10).as_deref(), Some("1"));
    assert_eq!(cs("1e+308", 10, 2).as_deref(), Some("1"));
    assert_eq!(cs("-1e+308", 10, 16).as_deref(), Some("FFFFFFFFFFFFFFFF"));
    assert_eq!(cs("FF", 16, 10).as_deref(), Some("255"));
    assert_eq!(cs("777", 8, 10).as_deref(), Some("511"));
    assert_eq!(cs("Z", 36, 10).as_deref(), Some("35"));
    assert_eq!(cs("9223372036854775807", 10, 16).as_deref(), Some("7FFFFFFFFFFFFFFF"));
    assert_eq!(cs("  12.9", 10, 10).as_deref(), Some("12"));
    // `-` makes the limit 0: a 0 digit after a nonzero value overflows to -1
    assert_eq!(cs("-10", 10, 10).as_deref(), Some("18446744073709551615"));
    assert_eq!(cs("-12", 10, -10).as_deref(), Some("-12"));
    // nothing parses → "0"; a bad base → NULL
    assert_eq!(cs("", 10, 2).as_deref(), Some("0"));
    assert_eq!(cs("xyz", 10, 2).as_deref(), Some("0"));
    assert_eq!(ci(15, 1, 2), None);
    assert_eq!(ci(15, 10, 37), None);
    assert_eq!(ci(15, -10, 2), None);
}

#[test]
fn null_arguments_give_null() {
    for name in [
        "asinh", "atan", "cbrt", "cos", "cosh", "degrees", "exp", "log10", "log2", "radians", "sin", "sinh", "tan", "tanh",
    ] {
        assert_eq!(eval(call(name, d(), vec![null(d())])).unwrap(), Datum::Null, "{name}");
    }
}

/// The fold suite sends `abs(DOUBLE)` and `mod` as FUNCTION_CALLs of DOUBLEs and same-scale
/// DECIMAL32s (modulo.cpp: C's fmod, the remainder of the raw decimals; a zero divisor is NULL).
#[test]
fn abs_and_mod_of_doubles_and_decimals() {
    assert_eq!(f1("abs", -1.5), dd(1.5));
    assert_eq!(f1("abs", -0.0), dd(0.0));
    let m = |a: f64, b: f64| eval(call("mod", d(), vec![dbl(a), dbl(b)])).unwrap();
    assert_eq!(m(10.5, 3.0), dd(1.5));
    assert_eq!(m(-10.5, 3.0), dd(-1.5));
    assert_eq!(m(10.5, 0.0), Datum::Null);
    let md = |a: &str, b: &str| eval(call("mod", dec(3, 1), vec![decimal(a, dec(3, 1)), decimal(b, dec(3, 1))])).unwrap();
    assert_eq!(md("10.5", "3.0"), Datum::Int(15));
    assert_eq!(md("-10.5", "3.0"), Datum::Int(-15));
    assert_eq!(md("10.5", "-3.0"), Datum::Int(15));
    assert_eq!(md("10.5", "0.0"), Datum::Null);
}

/// The fold suite's hidden needs: `conv` reads `CAST(1e308 AS VARCHAR)` as `1e+308`, and
/// `lcm(CAST(LARGEINT AS SMALLINT), …)` gets NULL from the overflowing cast.
#[test]
fn the_fold_suites_casts() {
    let vc = scalar(T::VARCHAR, 65533, 0, 0);
    let cast = |e: TExpr, to: TTypeDesc| over(blank(TExprNodeType::CAST_EXPR, to, 0), vec![e]);
    assert_eq!(eval(cast(dbl(1e308), vc)).unwrap(), Datum::Bytes(b"1e+308".to_vec()));
    let c = cast(large(170141183460469231731687303715884105726), ty(T::SMALLINT));
    assert_eq!(eval(c).unwrap(), Datum::Null);
}

/// glibc's `e_acosh.c` and `e_atanh.c` round these off by one ulp from the correctly rounded
/// value; the reference rows are glibc's (batch 6 run 1).
#[test]
fn acosh_and_atanh_round_as_glibc() {
    assert_eq!(f1("acosh", 1.0), dd(0.0));
    assert_eq!(f1("acosh", 2.0), dd(1.3169578969248166));
    assert_eq!(f1("acosh", 10.0), dd(2.993222846126381));
    assert_eq!(f1("atanh", 0.0), dd(0.0));
    assert_eq!(f1("atanh", 0.5), dd(0.5493061443340548));
    assert_eq!(f1("atanh", -0.5), dd(-0.5493061443340548));
    // the out-of-domain inputs stay NULL
    assert_eq!(f1("acosh", 0.5), Datum::Null);
    assert_eq!(f1("atanh", 1.0), Datum::Null);
}
