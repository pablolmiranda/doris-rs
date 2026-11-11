//! `money_format` and `format_round` (`function_string_format.h`): see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-math.md`. The integer and decimal paths copy
//! C++'s `int` truncations, which the recorded rows depend on.
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::value::ColType;
use crate::ExecError;

/// Types the two functions format.
pub fn formats(t: &ColType) -> bool {
    matches!(
        t.ptype,
        T::DOUBLE | T::BIGINT | T::LARGEINT | T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I
    )
}

/// `my_double_round` (`exprs/math_functions.cpp:67-99`) for `dec ≥ 0`.
fn double_round(value: f64, dec: i64) -> f64 {
    let tmp = if dec < 309 {
        format!("1e{dec}").parse::<f64>().unwrap_or(f64::INFINITY)
    } else {
        10f64.powi(dec as i32)
    };
    let mul = value * tmp;
    if !mul.is_finite() {
        return value;
    }
    mul.round() / tmp
}

/// `SimpleItoaWithCommas` of a signed integer's digits.
fn with_commas(digits: &str) -> String {
    let (neg, d) = match digits.strip_prefix('-') {
        Some(d) => (true, d),
        None => (false, digits),
    };
    let mut out = String::new();
    for (i, c) in d.chars().enumerate() {
        if i > 0 && (d.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if neg {
        format!("-{out}")
    } else {
        out
    }
}

/// A double formatted with `places` decimals (fmt's `{:.Nf}`), commas in its integer part.
fn double_text(v: f64, places: usize) -> String {
    if v.is_nan() {
        return if v.is_sign_negative() { "-nan" } else { "nan" }.into();
    }
    if v.is_infinite() {
        return if v < 0.0 { "-inf" } else { "inf" }.into();
    }
    let s = format!("{v:.places$}");
    match s.split_once('.') {
        Some((i, f)) => format!("{}.{f}", with_commas(i)),
        None => with_commas(&s),
    }
}

/// A value as (scale, integer part, fractional part), both truncated toward zero.
fn split(t: &ColType, raw: i128) -> (u32, i128, i128) {
    match t.ptype {
        T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I => {
            let p = 10i128.pow(t.scale as u32);
            (t.scale as u32, raw / p, raw % p)
        }
        _ => (0, raw, 0),
    }
}

/// The integer part with commas, `-` before a zero integer part of a negative value.
fn integer_text(int: i128, neg: bool) -> String {
    let digits = if int < 0 { format!("-{}", int.unsigned_abs()) } else { int.to_string() };
    let s = with_commas(&digits);
    if neg && int == 0 {
        format!("-{s}")
    } else {
        s
    }
}

/// `do_money_format`.
fn money(t: &ColType, raw: i128) -> String {
    let (scale, mut int, mut frac) = split(t, raw);
    let neg = int < 0 || frac < 0;
    if scale > 2 {
        // `(int)(frac / 10^(scale-3))`
        let third = (frac / 10i128.pow(scale - 3)) as i32;
        frac = i128::from(third.wrapping_abs()) + 5;
        frac /= 10;
    } else if scale < 2 {
        frac *= 10i128.pow(2 - scale);
    }
    if frac == 100 {
        int += if neg { -1 } else { 1 };
        frac = 0;
    }
    format!("{}.{}{}", integer_text(int, neg), (frac / 10).abs(), (frac % 10).abs())
}

/// `exp10_i32`: 10^n, saturated at `i32::MAX` past 10^9.
fn exp10_i32(n: i64) -> i128 {
    if n <= 9 {
        10i128.pow(n as u32)
    } else {
        i128::from(i32::MAX)
    }
}

/// Wraps `v` to the width of the type C++ computes in (`T`: Int64 for DECIMAL32/64 and BIGINT,
/// Int128 otherwise).
fn wrap(t: &ColType, v: i128) -> i128 {
    match t.ptype {
        T::DECIMAL32 | T::DECIMAL64 | T::BIGINT => i128::from(v as i64),
        _ => v,
    }
}

/// `do_format_round`.
fn round_fixed(t: &ColType, raw: i128, d: i64) -> String {
    let (scale, mut int, mut frac) = split(t, raw);
    let scale = i64::from(scale);
    let neg = int < 0 || frac < 0;
    if scale > d {
        let m = 10i128.pow((scale - (d + 1)) as u32);
        let kept = ((frac / m) as i32).wrapping_abs().wrapping_add(5);
        frac = wrap(t, i128::from(kept)) / 10;
    } else if scale < d && d > 0 {
        frac = wrap(t, frac.wrapping_mul(exp10_i32(d - scale)));
    }
    if frac == exp10_i32(d) {
        int += if neg { -1 } else { 1 };
        frac = 0;
    }
    let mut out = integer_text(int, neg);
    if d > 0 {
        // `std::abs(static_cast<int>(frac))`, digit by digit from the right
        let mut r = i64::from((frac as i32).wrapping_abs());
        let mut digits = vec![b'0'; d as usize];
        for slot in digits.iter_mut().rev() {
            *slot = (b'0' as i64 + r % 10) as u8;
            r /= 10;
        }
        out.push('.');
        out.push_str(&String::from_utf8_lossy(&digits));
    }
    out
}

fn value(d: &Datum) -> Option<(f64, i128)> {
    match d {
        Datum::Double(v) => Some((*v, 0)),
        Datum::Int(v) => Some((0.0, *v)),
        _ => None,
    }
}

/// `money_format(x)`; a NULL row is NULL.
pub fn money_format(t: &ColType, args: &[Datum]) -> Vec<Datum> {
    args.iter()
        .map(|d| match value(d) {
            None => Datum::Null,
            Some((v, _)) if t.ptype == T::DOUBLE => Datum::Bytes(double_text(double_round(v, 2), 2).into_bytes()),
            Some((_, raw)) => Datum::Bytes(money(t, raw).into_bytes()),
        })
        .collect()
}

/// `format_round(x, places)`; places outside 0..=1024 fail on the first such row.
pub fn format_round(t: &ColType, xs: &[Datum], places: &[Datum]) -> Result<Vec<Datum>, ExecError> {
    xs.iter()
        .zip(places)
        .map(|(x, p)| {
            let (Some((v, raw)), Datum::Int(d)) = (value(x), p) else {
                return Ok(Datum::Null);
            };
            let d = *d as i64;
            if !(0..=1024).contains(&d) {
                return Err(ExecError::Coded {
                    code: 33,
                    name: "INVALID_ARGUMENT",
                    msg: format!("The second argument is {d}, it should be in range [0, 1024]."),
                });
            }
            let s = if t.ptype == T::DOUBLE {
                double_text(double_round(v, d), d as usize)
            } else {
                round_fixed(t, raw, d)
            };
            Ok(Datum::Bytes(s.into_bytes()))
        })
        .collect()
}
