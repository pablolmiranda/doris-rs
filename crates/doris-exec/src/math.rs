//! Math functions, as the C++ BE computes them (`be/src/exprs/function/math.cpp`,
//! `function_math_unary_alway_nullable.h`, `function_math_log.h`, `modulo.cpp`,
//! `least_greast.cpp`, `function_width_bucket.cpp`): see
//! `docs/superpowers/research/2026-11-10-m6-s7b3-math.md`.
//!
//! Transcendentals come from `core_math`, which is correctly rounded: the C++ BE's glibc gives
//! the correctly rounded value on every recorded input, and the host's libm does not.
use doris_segment::writer::Datum;
use doris_thrift::gen::status::TStatusCode;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::value::ColType;
use crate::ExecError;

const UNARY: &[&str] = &["cot", "sec", "csc", "acos", "acosh", "asin", "atanh", "sqrt", "dsqrt", "ln"];
const BINARY: &[&str] = &["pow", "power", "dpow", "fpow", "atan2"];
const OTHER: &[&str] = &["factorial", "pmod", "least", "greatest", "bin", "width_bucket"];

/// Whether `name` is a function this module evaluates (for some signature).
pub fn is_math_function(name: &str) -> bool {
    UNARY.contains(&name) || BINARY.contains(&name) || OTHER.contains(&name)
}

fn is_int(t: &ColType) -> bool {
    matches!(t.ptype, T::TINYINT | T::SMALLINT | T::INT | T::BIGINT)
}

fn is_text(t: &ColType) -> bool {
    matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING)
}

/// Whether `name` over `args` into `to` is a call this module evaluates.
pub fn supported(name: &str, args: &[ColType], to: &ColType) -> bool {
    let double = |t: &ColType| t.ptype == T::DOUBLE;
    let bigint = |t: &ColType| t.ptype == T::BIGINT;
    match name {
        _ if UNARY.contains(&name) => args.len() == 1 && double(&args[0]) && double(to),
        _ if BINARY.contains(&name) => args.len() == 2 && args.iter().all(double) && double(to),
        "factorial" => args.len() == 1 && bigint(&args[0]) && bigint(to),
        "pmod" => args.len() == 2 && (args.iter().all(bigint) && bigint(to) || args.iter().all(double) && double(to)),
        // compared as stored: integers, doubles, text by bytes, one decimal scale, packed dates
        "least" | "greatest" => {
            !args.is_empty()
                && args.iter().all(|a| a.ptype == to.ptype && a.scale == to.scale)
                && (is_int(to)
                    || is_text(to)
                    || matches!(
                        to.ptype,
                        T::LARGEINT | T::DOUBLE | T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I | T::DATEV2 | T::DATETIMEV2
                    ))
        }
        "bin" => args.len() == 1 && bigint(&args[0]) && is_text(to),
        "width_bucket" => {
            args.len() == 4
                && bigint(to)
                && is_int(&args[3])
                && ((args[..3].iter().all(is_int) && args[1].ptype == args[0].ptype && args[2].ptype == args[0].ptype)
                    || args[..3].iter().all(double))
        }
        _ => false,
    }
}

/// Every row with a NULL argument is NULL; the others go through `f`.
fn rows(args: &[&[Datum]], n: usize, mut f: impl FnMut(&[&Datum]) -> Result<Datum, ExecError>) -> Result<Vec<Datum>, ExecError> {
    (0..n)
        .map(|r| {
            let row: Vec<&Datum> = args.iter().map(|a| &a[r]).collect();
            if row.iter().any(|d| matches!(d, Datum::Null)) {
                Ok(Datum::Null)
            } else {
                f(&row)
            }
        })
        .collect()
}

fn num(d: &Datum) -> f64 {
    match d {
        Datum::Double(v) => *v,
        Datum::Float(v) => f64::from(*v),
        Datum::Int(v) => *v as f64,
        _ => 0.0,
    }
}

fn int(d: &Datum) -> i128 {
    match d {
        Datum::Int(v) => *v,
        _ => 0,
    }
}

/// An always-nullable unary function's value, or NULL outside its domain; NaN stays NaN
/// (`function_math_unary_alway_nullable.h:85-93`, `function_math_log.h`).
fn unary(name: &str, x: f64) -> Option<f64> {
    let invalid = match name {
        "acos" | "asin" => !(-1.0..=1.0).contains(&x) && !x.is_nan(),
        "acosh" => x < 1.0,
        "atanh" => x <= -1.0 || x >= 1.0,
        "sqrt" | "dsqrt" => x < 0.0,
        "ln" => x <= 0.0,
        _ => false,
    };
    if invalid {
        return None;
    }
    Some(match name {
        "cot" => 1.0 / core_math::tan(x),
        "sec" => 1.0 / core_math::cos(x),
        "csc" => 1.0 / core_math::sin(x),
        "acos" => core_math::acos(x),
        "acosh" => core_math::acosh(x),
        "asin" => core_math::asin(x),
        "atanh" => core_math::atanh(x),
        "sqrt" | "dsqrt" => x.sqrt(),
        _ => core_math::log(x),
    })
}

/// `n!` for 0..=20, else NULL (math.cpp:413-480).
fn factorial(n: i128) -> Datum {
    if !(0..=20).contains(&n) {
        return Datum::Null;
    }
    Datum::Int((1..=n).product::<i128>().max(1))
}

/// The binary digits of the value as a u64, without leading zeros (math.cpp:513-543).
fn bin(v: i128) -> Datum {
    Datum::Bytes(format!("{:b}", v as i64 as u64).into_bytes())
}

/// Evaluates math function `name`; `types` are the arguments' types.
pub fn eval(name: &str, types: &[ColType], args: &[&[Datum]], n: usize) -> Result<Vec<Datum>, ExecError> {
    match name {
        _ if UNARY.contains(&name) => rows(args, n, |r| Ok(unary(name, num(r[0])).map_or(Datum::Null, Datum::Double))),
        "atan2" => rows(args, n, |r| Ok(Datum::Double(core_math::atan2(num(r[0]), num(r[1]))))),
        _ if BINARY.contains(&name) => {
            // a constant exponent of exactly 2.0 squares the leading integral rows up to 2^26
            // (math.cpp:649-667); x*x is then what pow gives anyway, as pow is correctly rounded
            rows(args, n, |r| Ok(Datum::Double(core_math::pow(num(r[0]), num(r[1])))))
        }
        "factorial" => rows(args, n, |r| Ok(factorial(int(r[0])))),
        "pmod" if types.first().is_some_and(|t| t.ptype == T::DOUBLE) => rows(args, n, |r| {
            let (a, b) = (num(r[0]), num(r[1]));
            Ok(if b == 0.0 { Datum::Null } else { Datum::Double(((a % b) + b) % b) })
        }),
        "pmod" => rows(args, n, |r| {
            let (a, b) = (int(r[0]) as i64, int(r[1]) as i64);
            if b == 0 {
                return Ok(Datum::Null);
            }
            if a == i64::MIN && b == -1 {
                return Err(ExecError::Coded {
                    code: TStatusCode::INVALID_ARGUMENT.0,
                    name: "INVALID_ARGUMENT",
                    msg: format!("Division of minimal signed number by minus one is an undefined behavior, {a} % {b}. "),
                });
            }
            Ok(Datum::Int(i128::from((a % b).wrapping_add(b) % b)))
        }),
        "least" | "greatest" => {
            // `Compare::less`: NaN the greatest, -0.0 equal to 0.0 (accurate_comparison.h)
            let t = types
                .first()
                .cloned()
                .ok_or_else(|| ExecError::BadRequest(format!("{name} without arguments")))?;
            rows(args, n, |r| {
                let mut best = r[0];
                for d in &r[1..] {
                    let ord = crate::eval::compare(d, &t, best, &t).unwrap_or(std::cmp::Ordering::Equal);
                    let better = if name == "least" { ord.is_lt() } else { ord.is_gt() };
                    if better {
                        best = d;
                    }
                }
                Ok(best.clone())
            })
        }
        "bin" => rows(args, n, |r| Ok(bin(int(r[0])))),
        "width_bucket" => width_bucket(types, args, n),
        other => Err(ExecError::Unsupported {
            what: format!("function {other}"),
        }),
    }
}

/// `width_bucket(x, min, max, n)` (function_width_bucket.cpp:63-122): `n` is row 0's; integers
/// are subtracted in their own type (BIGINT wrapping) and divided as doubles. A NULL argument
/// nulls its row; a NULL count everywhere (a NULL constant) nulls every row.
fn width_bucket(types: &[ColType], args: &[&[Datum]], n: usize) -> Result<Vec<Datum>, ExecError> {
    let counts = args.get(3).copied().unwrap_or_default();
    if n == 0 {
        return Ok(vec![]);
    }
    if counts.iter().all(|d| matches!(d, Datum::Null)) {
        return Ok(vec![Datum::Null; n]);
    }
    let buckets = counts.first().map_or(0, int);
    if buckets <= 0 {
        return Err(ExecError::Coded {
            code: TStatusCode::INTERNAL_ERROR.0,
            name: "INTERNAL_ERROR",
            msg: format!("The desired number({buckets}) of buckets must be a positive integer value."),
        });
    }
    let bigint = types.first().is_some_and(|t| t.ptype == T::BIGINT);
    let integer = types.first().is_some_and(is_int);
    let sub = |a: i128, b: i128| if bigint { i128::from((a as i64).wrapping_sub(b as i64)) } else { a - b };
    rows(args, n, |r| {
        let bucket = if integer {
            let (x, lo, hi) = (int(r[0]), int(r[1]), int(r[2]));
            let range = sub(hi, lo);
            if x < lo {
                0
            } else if x >= hi {
                buckets + 1
            } else if range / buckets == 0 {
                0
            } else {
                (1.0 + sub(x, lo) as f64 / (range as f64 / (1.0 * buckets as f64))) as i64 as i128
            }
        } else {
            let (x, lo, hi) = (num(r[0]), num(r[1]), num(r[2]));
            if x < lo {
                0
            } else if x >= hi {
                buckets + 1
            } else if (hi - lo) / buckets as f64 == 0.0 {
                0
            } else {
                (1.0 + (x - lo) / ((hi - lo) / (1.0 * buckets as f64))) as i64 as i128
            }
        };
        Ok(Datum::Int(bucket))
    })
}
