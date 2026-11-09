//! Arithmetic as the C++ BE computes it (`be/src/exprs/function/`, "F/" below):
//! - integers wrap at the result's width (`F/plus.cpp:31`, `F/multiply.cpp:54-57`);
//! - a zero divisor gives NULL (`F/divide.cpp:348-351`, `F/modulo.cpp:491-502`);
//! - decimals work on raw scaled integers: a product is scaled down to the result's scale half
//!   away from zero and checked against its precision, past 128 bits in 256
//!   (`F/multiply.cpp:428-496`); a quotient is the raw dividend over the raw divisor, truncated,
//!   the FE having scaled the dividend so the scales line up (`F/divide.cpp:433-440`).
//!
//! Only the signatures the FE plans for this story's suites are accepted (`arithmetic_supported`).
use doris_segment::writer::Datum;
use doris_thrift::gen::status::TStatusCode;
use doris_thrift::gen::types::TPrimitiveType as T;
use ethnum::I256;

use crate::row_text::decimal_text;
use crate::value::ColType;
use crate::ExecError;

/// The width of an integer type in bits.
fn int_bits(t: &ColType) -> Option<u32> {
    Some(match t.ptype {
        T::TINYINT => 8,
        T::SMALLINT => 16,
        T::INT => 32,
        T::BIGINT => 64,
        T::LARGEINT => 128,
        _ => return None,
    })
}

/// The native width of a decimal type in bits; DECIMAL256 and DECIMALV2 are not handled.
fn decimal_bits(t: &ColType) -> Option<u32> {
    Some(match t.ptype {
        T::DECIMAL32 => 32,
        T::DECIMAL64 => 64,
        T::DECIMAL128I => 128,
        _ => return None,
    })
}

/// `v` as a signed integer of `bits` (C++'s `static_cast` to a narrower type).
fn wrap(v: i128, bits: u32) -> i128 {
    if bits >= 128 {
        v
    } else {
        (v << (128 - bits)) >> (128 - bits)
    }
}

fn is_double(t: &ColType) -> bool {
    t.ptype == T::DOUBLE
}

/// Whether `arithmetic` (or `abs`, op "abs") evaluates this signature.
pub fn arithmetic_supported(op: &str, args: &[ColType], to: &ColType) -> bool {
    if op == "abs" {
        return match (args, int_bits(to)) {
            ([a], Some(tb)) => int_bits(a).is_some_and(|ab| ab <= tb),
            _ => false,
        };
    }
    let [a, b] = args else { return false };
    if let (Some(_), Some(_), Some(_)) = (int_bits(a), int_bits(b), int_bits(to)) {
        return a.ptype == to.ptype && b.ptype == to.ptype && matches!(op, "add" | "subtract" | "multiply" | "mod");
    }
    if is_double(a) && is_double(b) && is_double(to) {
        return matches!(op, "add" | "subtract" | "multiply" | "divide");
    }
    if let (Some(_), Some(_), Some(_)) = (decimal_bits(a), decimal_bits(b), decimal_bits(to)) {
        return match op {
            "multiply" => a.scale + b.scale >= to.scale,
            "divide" => a.scale - b.scale == to.scale,
            _ => false,
        };
    }
    false
}

fn unsupported(op: &str) -> ExecError {
    ExecError::Unsupported {
        what: format!("function {op}"),
    }
}

/// `op` over two columns, giving a column of type `to`. NULL in, NULL out.
pub fn arithmetic(op: &str, a: (&ColType, &[Datum]), b: (&ColType, &[Datum]), to: &ColType) -> Result<Vec<Datum>, ExecError> {
    let (ta, va) = a;
    let (tb, vb) = b;
    if !arithmetic_supported(op, &[ta.clone(), tb.clone()], to) {
        return Err(unsupported(op));
    }
    va.iter()
        .zip(vb)
        .map(|(x, y)| match (x, y) {
            (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
            (Datum::Double(x), Datum::Double(y)) => Ok(double_op(op, *x, *y)),
            (Datum::Int(x), Datum::Int(y)) if int_bits(to).is_some() => int_op(op, *x, *y, int_bits(to).unwrap_or(128)),
            (Datum::Int(x), Datum::Int(y)) => decimal_op(op, (ta, *x), (tb, *y), to),
            _ => Err(ExecError::BadRequest(format!("{op} of {x:?} and {y:?}"))),
        })
        .collect()
}

fn double_op(op: &str, x: f64, y: f64) -> Datum {
    Datum::Double(match op {
        "add" => x + y,
        "subtract" => x - y,
        "multiply" => x * y,
        _ if y == 0.0 => return Datum::Null,
        _ => x / y,
    })
}

fn int_op(op: &str, x: i128, y: i128, bits: u32) -> Result<Datum, ExecError> {
    Ok(Datum::Int(wrap(
        match op {
            "add" => x.wrapping_add(y),
            "subtract" => x.wrapping_sub(y),
            "multiply" => x.wrapping_mul(y),
            _ if y == 0 => return Ok(Datum::Null),
            _ if y == -1 && x == wrap(1i128 << (bits - 1), bits) => {
                return Err(ExecError::Coded {
                    code: TStatusCode::INVALID_ARGUMENT.0,
                    name: "INVALID_ARGUMENT",
                    msg: format!("Division of minimal signed number by minus one is an undefined behavior, {x} % {y}. "),
                })
            }
            _ => x % y,
        },
        bits,
    )))
}

fn i256_text(v: I256, scale: i32) -> String {
    let digits = v.unsigned_abs().to_string();
    let sign = if v < 0 { "-" } else { "" };
    let scale = scale.max(0) as usize;
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    format!("{sign}{int}.{frac}")
}

fn overflow(ta: &ColType, x: i128, tb: &ColType, y: i128, result: String, to: &ColType) -> ExecError {
    ExecError::Coded {
        code: TStatusCode::INTERNAL_ERROR.0,
        name: "ARITHMETIC_OVERFLOW_ERRROR",
        msg: format!(
            "Arithmetic overflow: {} multiply {} = {}, result type: Decimal({}, {})",
            decimal_text(x, ta.scale),
            decimal_text(y, tb.scale),
            result,
            to.precision,
            to.scale
        ),
    }
}

fn decimal_op(op: &str, a: (&ColType, i128), b: (&ColType, i128), to: &ColType) -> Result<Datum, ExecError> {
    let ((ta, x), (tb, y)) = (a, b);
    let bits = decimal_bits(to).ok_or_else(|| unsupported(op))?;
    if op == "divide" {
        if y == 0 {
            return Ok(Datum::Null);
        }
        // `(ResultNative)a / b`, then the quotient as the result's native type
        return Ok(Datum::Int(wrap(wrap(x, bits).wrapping_div(y), bits)));
    }
    let max = 10i128.pow(to.precision as u32) - 1;
    let m = 10i128.pow((ta.scale + tb.scale - to.scale) as u32);
    // the product in the result's native type: an overflow there is the 256-bit path for
    // DECIMAL128I, and an error at once for the narrower ones (`F/multiply.cpp:440-475`)
    let (xa, yb) = (wrap(x, bits), wrap(y, bits));
    let native = xa.checked_mul(yb).filter(|p| bits == 128 || wrap(*p, bits) == *p);
    let Some(p) = native else {
        if bits < 128 {
            let wrapped = wrap(xa.wrapping_mul(yb), bits);
            return Err(overflow(ta, x, tb, y, decimal_text(wrapped, to.scale), to));
        }
        let mut p = I256::from(xa) * I256::from(yb);
        if m > 1 {
            let half = I256::from(m / 2);
            p = if p > 0 { (p + half) / I256::from(m) } else { (p - half) / I256::from(m) };
        }
        if p > I256::from(max) || p < I256::from(-max) {
            return Err(overflow(ta, x, tb, y, i256_text(p, to.scale), to));
        }
        return Ok(Datum::Int(p.as_i128()));
    };
    let r = if m > 1 {
        if p >= 0 {
            p.wrapping_add(m / 2) / m
        } else {
            p.wrapping_sub(m / 2) / m
        }
    } else {
        p
    };
    if r > max || r < -max {
        return Err(overflow(ta, x, tb, y, decimal_text(r, to.scale), to));
    }
    Ok(Datum::Int(r))
}

/// `abs` of an integer column into a type at least as wide: `~a + 1` in the wide type
/// (`F/math.cpp:217-227`), so the narrow type's minimum has an exact absolute value.
pub fn abs(a: (&ColType, &[Datum]), to: &ColType) -> Result<Vec<Datum>, ExecError> {
    let (ta, va) = a;
    if !arithmetic_supported("abs", std::slice::from_ref(ta), to) {
        return Err(unsupported("abs"));
    }
    let bits = int_bits(to).unwrap_or(128);
    va.iter()
        .map(|d| match d {
            Datum::Null => Ok(Datum::Null),
            Datum::Int(v) => Ok(Datum::Int(wrap(v.wrapping_abs(), bits))),
            other => Err(ExecError::BadRequest(format!("abs of {other:?}"))),
        })
        .collect()
}
