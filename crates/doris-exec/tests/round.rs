//! `round` as the C++ BE computes it (be/src/exprs/function/round.h).
use doris_exec::round::{round_decimal, round_double};

#[test]
fn round_double_matches_cpp() {
    assert_eq!(round_double(2.675, 2), 2.68);
    assert_eq!(round_double(1.005, 2), 1.0);
    assert_eq!(round_double(0.285, 2), 0.28);
    assert_eq!(round_double(1.15, 1), 1.2);
    assert_eq!(round_double(-0.5, 0), -1.0);
    assert_eq!(round_double(-2.5, 0), -3.0);
    assert_eq!(round_double(12345.6789, -2), 12300.0);
    assert_eq!(round_double(1e300, 20), f64::INFINITY);
    // 10^d saturates at u64::MAX past 19: x / 1.8446744073709552e19 rounds to 0
    assert_eq!(round_double(12345.0, -20), 0.0);
}

#[test]
fn round_decimal_half_away() {
    // (38,3) to 2 digits: result scale 2
    assert_eq!(round_decimal(-1235, 3, 2, 2), -124);
    assert_eq!(round_decimal(1234, 3, 2, 2), 123);
    assert_eq!(round_decimal(1235, 3, 2, 2), 124);
    // (33,25) to 0 digits: 0.5 -> 1
    assert_eq!(round_decimal(5 * 10i128.pow(24), 25, 0, 0), 1);
    // negative digits: result scale 0, rounded to hundreds
    assert_eq!(round_decimal(12_350, 0, -2, 0), 12_400);
    assert_eq!(round_decimal(-12_349, 0, -2, 0), -12_300);
    // more digits than the scale: unchanged
    assert_eq!(round_decimal(1234, 3, 5, 3), 1234);
    // a divisor past i128 rounds everything to 0
    assert_eq!(round_decimal(10i128.pow(37), 0, -39, 0), 0);
}
