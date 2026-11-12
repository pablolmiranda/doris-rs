//! Arithmetic as the C++ BE computes it (F/ = be/src/exprs/function/): integers wrap, zero
//! divisors give NULL, decimals work on raw scaled integers.
use doris_exec::arith::{abs, arithmetic, arithmetic_supported};
use doris_exec::value::ColType;
use doris_exec::ExecError;
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

fn ty(ptype: T, precision: i32, scale: i32) -> ColType {
    ColType {
        name: String::new(),
        ptype,
        len: -1,
        precision,
        scale,
        nullable: true,
    }
}

fn ints(v: &[i128]) -> Vec<Datum> {
    v.iter().map(|x| Datum::Int(*x)).collect()
}

fn run(op: &str, ta: &ColType, a: &[Datum], tb: &ColType, b: &[Datum], to: &ColType) -> Vec<Datum> {
    arithmetic(op, (ta, a), (tb, b), to).expect("evaluates")
}

#[test]
fn integer_ops_wrap() {
    let big = ty(T::BIGINT, 0, 0);
    let max = i64::MAX as i128;
    let min = i64::MIN as i128;
    assert_eq!(run("add", &big, &ints(&[max]), &big, &ints(&[1]), &big), ints(&[min]));
    assert_eq!(run("subtract", &big, &ints(&[min]), &big, &ints(&[1]), &big), ints(&[max]));
    assert_eq!(run("multiply", &big, &ints(&[min]), &big, &ints(&[-1]), &big), ints(&[min]));
    assert_eq!(run("add", &big, &[Datum::Null], &big, &ints(&[1]), &big), vec![Datum::Null]);
}

#[test]
fn mod_truncates_and_zero_is_null() {
    let big = ty(T::BIGINT, 0, 0);
    let out = run("mod", &big, &ints(&[-7, 7, 5]), &big, &ints(&[3, -3, 0]), &big);
    assert_eq!(out, vec![Datum::Int(-1), Datum::Int(1), Datum::Null]);
}

#[test]
fn mod_of_min_by_minus_one_errors() {
    let big = ty(T::BIGINT, 0, 0);
    let err = arithmetic("mod", (&big, &ints(&[i64::MIN as i128])), (&big, &ints(&[-1])), &big).unwrap_err();
    assert_eq!(
        err.to_string(),
        "Division of minimal signed number by minus one is an undefined behavior, -9223372036854775808 % -1. "
    );
    assert!(
        matches!(
            err,
            ExecError::Coded {
                name: "INVALID_ARGUMENT",
                code: 33,
                ..
            }
        ),
        "{err:?}"
    );
}

#[test]
fn double_ops_and_divide_by_zero_is_null() {
    let d = ty(T::DOUBLE, 0, 0);
    let a = vec![
        Datum::Double(1.0),
        Datum::Double(1.0),
        Datum::Double(0.0),
        Datum::Double(1.0),
        Datum::Double(3.0),
    ];
    let b = vec![
        Datum::Double(0.0),
        Datum::Double(-0.0),
        Datum::Double(0.0),
        Datum::Double(f64::NAN),
        Datum::Double(2.0),
    ];
    let out = run("divide", &d, &a, &d, &b, &d);
    assert_eq!(out[..3], [Datum::Null, Datum::Null, Datum::Null]);
    assert!(matches!(out[3], Datum::Double(x) if x.is_nan()));
    assert_eq!(out[4], Datum::Double(1.5));
    assert_eq!(
        run("add", &d, &[Datum::Double(0.1)], &d, &[Datum::Double(0.2)], &d),
        vec![Datum::Double(0.1 + 0.2)]
    );
    assert_eq!(
        run("multiply", &d, &[Datum::Double(1.5)], &d, &[Datum::Double(-2.0)], &d),
        vec![Datum::Double(-3.0)]
    );
}

#[test]
fn decimal_multiply_rounds_half_away() {
    // F/multiply.cpp:428-496: the raw product is scaled down by 10^(9+8-12), half away from zero
    let a = ty(T::DECIMAL128I, 27, 9);
    let b = ty(T::DECIMAL64, 16, 8);
    let to = ty(T::DECIMAL128I, 38, 12);
    assert_eq!(
        run("multiply", &a, &ints(&[1, -1, 1_000_000_001]), &b, &ints(&[50000, 50000, 5]), &to),
        ints(&[1, -1, 50000])
    );
    assert_eq!(run("multiply", &a, &ints(&[1, 1]), &b, &ints(&[149_999, 49_999]), &to), ints(&[1, 0]));
    // (22,14) x (3,0) -> (25,14): no rescale
    let a = ty(T::DECIMAL128I, 22, 14);
    let b = ty(T::DECIMAL32, 3, 0);
    let to = ty(T::DECIMAL128I, 25, 14);
    assert_eq!(run("multiply", &a, &ints(&[12_345]), &b, &ints(&[-3]), &to), ints(&[-37_035]));
}

#[test]
fn decimal_multiply_past_i128_is_exact() {
    // both raws 10^19 + 7 at scale 10: the product needs 256 bits; scaled down by 10^10 it fits 38 digits
    let t = ty(T::DECIMAL128I, 38, 10);
    let x = 10i128.pow(19) + 7;
    // (10^19+7)^2 = 10^38 + 14*10^19 + 49; / 10^10 rounded = 10^28 + 14*10^9 (the 49 rounds away)
    let want = 10i128.pow(28) + 14 * 10i128.pow(9);
    assert_eq!(run("multiply", &t, &ints(&[x]), &t, &ints(&[x]), &t), ints(&[want]));
    let neg = run("multiply", &t, &ints(&[-x]), &t, &ints(&[x]), &t);
    assert_eq!(neg, ints(&[-want]));
}

#[test]
fn decimal_multiply_overflow_errors() {
    // past the result's precision, within its native width
    let t = ty(T::DECIMAL32, 5, 0);
    let err = arithmetic("multiply", (&t, &ints(&[1000])), (&t, &ints(&[1000])), &t).unwrap_err();
    assert_eq!(
        err.to_string(),
        "Arithmetic overflow: 1000 multiply 1000 = 1000000, result type: Decimal(5, 0)"
    );
    assert!(
        matches!(
            err,
            ExecError::Coded {
                name: "ARITHMETIC_OVERFLOW_ERRROR",
                ..
            }
        ),
        "{err:?}"
    );
    // past i128: redone in 256 bits, then past 38 digits
    let t = ty(T::DECIMAL128I, 38, 0);
    let e20 = 10i128.pow(20);
    let err = arithmetic("multiply", (&t, &ints(&[e20])), (&t, &ints(&[e20])), &t).unwrap_err();
    assert_eq!(
        err.to_string(),
        "Arithmetic overflow: 100000000000000000000 multiply 100000000000000000000 = 10000000000000000000000000000000000000000, result type: Decimal(38, 0)"
    );
    // a NULL row is not computed (need_replace_null_data_to_default)
    let ok = arithmetic("multiply", (&t, &[Datum::Null]), (&t, &ints(&[e20])), &t).unwrap();
    assert_eq!(ok, vec![Datum::Null]);
}

#[test]
fn decimal_divide_truncates() {
    let a = ty(T::DECIMAL128I, 22, 14);
    let b = ty(T::DECIMAL32, 3, 0);
    let one = 10i128.pow(14);
    let out = run("divide", &a, &ints(&[one, -2 * one, one]), &b, &ints(&[3, 3, 0]), &a);
    assert_eq!(out, vec![Datum::Int(33_333_333_333_333), Datum::Int(-66_666_666_666_666), Datum::Null]);
    let a = ty(T::DECIMAL128I, 38, 10);
    let b = ty(T::DECIMAL128I, 38, 2);
    let to = ty(T::DECIMAL128I, 38, 8);
    assert_eq!(run("divide", &a, &ints(&[10i128.pow(10)]), &b, &ints(&[300]), &to), ints(&[33_333_333]));
    let a = ty(T::DECIMAL32, 6, 5);
    let b = ty(T::DECIMAL128I, 20, 0);
    assert_eq!(run("divide", &a, &ints(&[200_000]), &b, &ints(&[3]), &a), ints(&[66_666]));
}

#[test]
fn abs_widens() {
    let out = abs((&ty(T::INT, 0, 0), &ints(&[i32::MIN as i128, -5, 7])), &ty(T::BIGINT, 0, 0)).unwrap();
    assert_eq!(out, ints(&[2_147_483_648, 5, 7]));
    let out = abs(
        (&ty(T::BIGINT, 0, 0), &[Datum::Int(i64::MIN as i128), Datum::Null]),
        &ty(T::LARGEINT, 0, 0),
    )
    .unwrap();
    assert_eq!(out, vec![Datum::Int(9_223_372_036_854_775_808), Datum::Null]);
}

#[test]
fn refuses_unlisted_signatures() {
    let big = ty(T::BIGINT, 0, 0);
    let dbl = ty(T::DOUBLE, 0, 0);
    let d256 = ty(T::DECIMAL256, 76, 10);
    let d128 = ty(T::DECIMAL128I, 38, 10);
    assert!(arithmetic_supported("add", &[big.clone(), big.clone()], &big));
    assert!(arithmetic_supported("multiply", &[d128.clone(), d128.clone()], &d128));
    assert!(!arithmetic_supported("multiply", &[d256.clone(), d256.clone()], &d256));
    assert!(!arithmetic_supported("add", &[big.clone(), dbl.clone()], &dbl));
    assert!(!arithmetic_supported("divide", &[big.clone(), big.clone()], &big));
    // decimal add/subtract/mod are not in this story's suites
    assert!(!arithmetic_supported("add", &[d128.clone(), d128.clone()], &d128));
    assert!(!arithmetic_supported("mod", &[dbl.clone(), dbl.clone()], &dbl));
}

#[test]
fn narrow_decimal_rounding_wraps_as_cpp_does() {
    // F/multiply.cpp:477-484: `res + m/2` is computed in the result's native int64, so a product
    // at i64::MAX wraps negative before the division, and passes the precision check
    let t = ty(T::DECIMAL64, 18, 1);
    let a = 188_232_082_384_791_343; // x 49 = i64::MAX
    let out = run("multiply", &t, &ints(&[a]), &t, &ints(&[49]), &t);
    assert_eq!(out, ints(&[-922_337_203_685_477_580]));
}

#[test]
fn decimal_scales_past_38_digits_are_refused() {
    let d = |p, s| ty(T::DECIMAL128I, p, s);
    // 10^(38+38-0) does not fit i128: refused at prepare, not panicking at run time
    assert!(!arithmetic_supported("multiply", &[d(38, 38), d(38, 38)], &d(38, 0)));
    assert!(!arithmetic_supported("multiply", &[d(38, 2), d(38, 2)], &d(39, 4)));
    assert!(arithmetic_supported("multiply", &[d(38, 20), d(38, 18)], &d(38, 0)));
}
