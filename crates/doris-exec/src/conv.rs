//! `conv(num, from_base, to_base)` as the C++ BE computes it (`function_conv.cpp:57-222`,
//! `exprs/math_functions.cpp:101-179`, `util/string_parser.hpp:599-650`): see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-math.md` ("conv").
use doris_segment::writer::Datum;

/// Whether a base is outside 2..=36 in absolute value (the row is then NULL).
fn bad_base(b: i128) -> bool {
    !(2..=36).contains(&b.abs())
}

/// `decimal_to_base`: digits of `n` in base `|dst|`, upper case, no leading zeros; a negative
/// `dst` keeps a negative value's sign.
fn decimal_to_base(n: i64, dst: i128) -> Vec<u8> {
    let mut t = n as u64;
    let neg = dst < 0 && n < 0;
    if neg {
        t = 0u64.wrapping_sub(t);
    }
    let base = dst.unsigned_abs() as u64;
    let mut digits = Vec::new();
    loop {
        let d = (t % base) as u8;
        digits.push(if d < 10 { b'0' + d } else { b'A' + d - 10 });
        t /= base;
        if t == 0 {
            break;
        }
    }
    if neg {
        digits.push(b'-');
    }
    digits.reverse();
    digits
}

/// A BIGINT: its decimal digits read as base-`src` digits up to the first one ≥ `src`, the sign
/// reapplied; `None` when a negative base meets a non-negative number.
fn from_bigint(num: i64, src: i128) -> Option<i64> {
    if src < 0 && num >= 0 {
        return None;
    }
    if src == 10 {
        return Some(num);
    }
    let base = src.unsigned_abs() as u64;
    let mut value: u64 = 0;
    let mut overflow = false;
    for c in num.unsigned_abs().to_string().bytes() {
        let digit = u64::from(c - b'0');
        if digit >= base {
            break;
        }
        match value.checked_mul(base).and_then(|v| v.checked_add(digit)) {
            Some(v) => value = v,
            None => {
                overflow = true;
                break;
            }
        }
    }
    if overflow {
        return Some(-1);
    }
    let v = value as i64;
    Some(if num < 0 { 0i64.wrapping_sub(v) } else { v })
}

enum Parsed {
    Value(i64),
    Overflow,
    Failure,
}

/// `string_to_int_internal<uint64_t>` in base `base`, stopping at the first digit ≥ the base.
fn parse_u64(s: &[u8], base: u64) -> Parsed {
    let (neg, digits) = match s.first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() {
        return Parsed::Failure;
    }
    // with a leading '-', max_val is u64::MAX + 1 = 0
    let max_val: u64 = if neg { 0 } else { u64::MAX };
    let (max_div, max_mod) = (max_val / base, max_val % base);
    let mut val: u64 = 0;
    for (i, &c) in digits.iter().enumerate() {
        let digit = match c {
            b'0'..=b'9' => u64::from(c - b'0'),
            b'a'..=b'z' => u64::from(c - b'a') + 10,
            b'A'..=b'Z' => u64::from(c - b'A') + 10,
            _ => {
                let rest_blank = digits[i..].iter().all(|b| b.is_ascii_whitespace() || *b == 0x0b);
                return if i == 0 || !rest_blank {
                    Parsed::Failure
                } else {
                    Parsed::Value(finish(val, neg))
                };
            }
        };
        if digit >= base {
            break;
        }
        // unsigned, as C++: with max_div 0 a nonzero digit wraps the bound to u64::MAX
        if val > max_div.wrapping_sub(u64::from(digit > max_mod)) {
            return Parsed::Overflow;
        }
        val = val.wrapping_mul(base).wrapping_add(digit);
    }
    Parsed::Value(finish(val, neg))
}

fn finish(val: u64, neg: bool) -> i64 {
    (if neg { 0u64.wrapping_sub(val) } else { val }) as i64
}

/// A string: cut at the first '.', trimmed, parsed in base `src`.
fn from_text(s: &[u8], src: i128, dst: i128) -> Option<Vec<u8>> {
    let cut = s.iter().position(|c| *c == b'.').map_or(s, |i| &s[..i]);
    let blank = |c: &u8| c.is_ascii_whitespace() || *c == 0x0b;
    let start = cut.iter().position(|c| !blank(c)).unwrap_or(cut.len());
    let end = cut.iter().rposition(|c| !blank(c)).map_or(start, |e| e + 1);
    let n = match parse_u64(&cut[start..end], src.unsigned_abs() as u64) {
        Parsed::Failure => return Some(b"0".to_vec()),
        Parsed::Overflow => -1,
        Parsed::Value(v) => v,
    };
    if src < 0 && n >= 0 {
        return None;
    }
    Some(decimal_to_base(n, dst))
}

/// `conv` over rows: `text` says the first argument is a string.
pub fn eval(text: bool, args: &[&[Datum]], rows: usize) -> Vec<Datum> {
    (0..rows)
        .map(|r| {
            let (Datum::Int(src), Datum::Int(dst)) = (&args[1][r], &args[2][r]) else {
                return Datum::Null;
            };
            if bad_base(*src) || bad_base(*dst) {
                return Datum::Null;
            }
            let out = match &args[0][r] {
                Datum::Bytes(s) if text => from_text(s, *src, *dst),
                Datum::Int(v) if !text => from_bigint(*v as i64, *src).map(|n| decimal_to_base(n, *dst)),
                _ => None,
            };
            out.map_or(Datum::Null, Datum::Bytes)
        })
        .collect()
}
