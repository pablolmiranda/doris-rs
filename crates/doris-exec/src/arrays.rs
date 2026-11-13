//! The array functions (`exprs/function/array/`; research
//! `docs/superpowers/research/2026-11-12-m6-s7b6-arrays.md` §2): `array_with_constant`,
//! `array_join`, `array_range` (`sequence`) and its time-unit forms, `array_pushback`,
//! `array_pushfront`, `array_popback` and `array_popfront`.
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::datetime::{add, Dt, Unit};
use crate::value::ColType;
use crate::ExecError;

/// `max_array_size_as_field` (`column_array.h:64`).
const MAX_ARRAY_SIZE: i128 = 1_000_000;

/// The most elements one call builds over a block. A Datum takes several times C++'s bytes per
/// element, so past this doris-rs refuses where C++ would answer or fail on its memory limit.
const MAX_BLOCK_ELEMENTS: i128 = 1 << 24;

/// Counts the elements a call has built over a block; refuses past `MAX_BLOCK_ELEMENTS`.
struct Budget(std::cell::Cell<i128>);

impl Budget {
    fn new() -> Budget {
        Budget(std::cell::Cell::new(0))
    }

    fn take(&self, name: &str, n: i128) -> Result<(), ExecError> {
        let total = self.0.get() + n;
        if total > MAX_BLOCK_ELEMENTS {
            return Err(ExecError::Unsupported {
                what: format!("{name} building more than {MAX_BLOCK_ELEMENTS} elements in one block"),
            });
        }
        self.0.set(total);
        Ok(())
    }
}

const UNITS: &[(&str, Unit, i64)] = &[
    ("array_range_second_unit", Unit::Second, 1),
    ("array_range_minute_unit", Unit::Minute, 1),
    ("array_range_hour_unit", Unit::Hour, 1),
    ("array_range_day_unit", Unit::Day, 1),
    ("array_range_week_unit", Unit::Week, 1),
    ("array_range_month_unit", Unit::Month, 1),
    ("array_range_quarter_unit", Unit::Month, 3),
    ("array_range_year_unit", Unit::Year, 1),
];

pub fn is_array_function(name: &str) -> bool {
    matches!(
        name,
        "array_with_constant" | "array_join" | "array_range" | "sequence" | "array_pushback" | "array_pushfront" | "array_popback" | "array_popfront"
    ) || UNITS.iter().any(|(n, _, _)| *n == name)
}

/// Whether doris-rs runs this call, by its signature.
pub fn supported(name: &str, args: &[ColType], to: &ColType) -> bool {
    let int = |t: &ColType| matches!(t.ptype, T::TINYINT | T::SMALLINT | T::INT | T::BIGINT);
    let text = |t: &ColType| matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING);
    let array = |t: &ColType| t.ptype == T::ARRAY;
    match (name, args) {
        ("array_with_constant", [n, _]) => int(n) && array(to),
        ("array_join", [a, rest @ ..]) => {
            (1..=2).contains(&rest.len()) && a.element().is_some_and(|e| text(&e) || e.ptype == T::NULL_TYPE) && rest.iter().all(text)
        }
        ("array_range" | "sequence", _) if args.iter().all(|t| t.ptype == T::INT) => (1..=3).contains(&args.len()),
        ("array_range" | "sequence", [a, b]) => a.ptype == T::DATETIMEV2 && b.ptype == T::DATETIMEV2,
        (_, [a, b, s]) if UNITS.iter().any(|(n, _, _)| *n == name) => a.ptype == T::DATETIMEV2 && b.ptype == T::DATETIMEV2 && s.ptype == T::INT,
        ("array_pushback" | "array_pushfront", [a, _]) => array(a) && array(to),
        ("array_popback" | "array_popfront", [a]) => array(a),
        _ => false,
    }
}

fn invalid(msg: String) -> ExecError {
    ExecError::Coded {
        code: doris_thrift::gen::status::TStatusCode::INVALID_ARGUMENT.0,
        name: "INVALID_ARGUMENT",
        msg,
    }
}

fn too_large() -> ExecError {
    invalid(format!("Array size exceeds the limit {MAX_ARRAY_SIZE}"))
}

pub fn eval(name: &str, types: &[ColType], to: &ColType, args: &[&[Datum]], rows: usize) -> Result<Vec<Datum>, ExecError> {
    let elem = to.element().unwrap_or_default();
    let budget = Budget::new();
    match name {
        "array_with_constant" => with_constant(types, &elem, args, rows),
        "array_join" => Ok(join(args, rows)),
        "array_pushback" | "array_pushfront" => {
            let (at, vt) = (types[0].element().unwrap_or_default(), &types[1]);
            (0..rows)
                .map(|r| {
                    // a NULL array is NULL; a NULL value is a null element
                    let Datum::Array(a) = &args[0][r] else { return Ok(Datum::Null) };
                    let mut out = a
                        .iter()
                        .map(|d| crate::eval::coerce(d.clone(), &at, &elem))
                        .collect::<Result<Vec<_>, _>>()?;
                    let v = crate::eval::coerce(args[1][r].clone(), vt, &elem)?;
                    if name == "array_pushback" {
                        out.push(v);
                    } else {
                        out.insert(0, v);
                    }
                    Ok(Datum::Array(out))
                })
                .collect()
        }
        "array_popback" | "array_popfront" => Ok(args[0]
            .iter()
            .map(|d| match d {
                Datum::Array(a) if a.is_empty() => Datum::Array(vec![]),
                Datum::Array(a) if name == "array_popback" => Datum::Array(a[..a.len() - 1].to_vec()),
                Datum::Array(a) => Datum::Array(a[1..].to_vec()),
                _ => Datum::Null,
            })
            .collect()),
        _ if types.first().is_some_and(|t| t.ptype == T::DATETIMEV2) => {
            let (unit, times) = UNITS.iter().find(|(n, _, _)| *n == name).map_or((Unit::Day, 1), |(_, u, k)| (*u, *k));
            (0..rows)
                .map(|r| {
                    let step = match args.get(2) {
                        None => Some(1),
                        Some(s) => match s[r] {
                            Datum::Int(s) => Some(s as i64),
                            _ => None,
                        },
                    };
                    match (&args[0][r], &args[1][r], step) {
                        (Datum::Int(a), Datum::Int(b), Some(s)) if s > 0 => time_range(*a, *b, unit, s * times, &|n| budget.take(name, n)),
                        _ => Ok(Datum::Null),
                    }
                })
                .collect()
        }
        _ => (0..rows)
            .map(|r| {
                let at = |i: usize| args.get(i).map(|c| &c[r]);
                let (start, end, step) = match args.len() {
                    1 => (Some(&Datum::Int(0)), at(0), Some(&Datum::Int(1))),
                    2 => (at(0), at(1), Some(&Datum::Int(1))),
                    _ => (at(0), at(1), at(2)),
                };
                match (start, end, step) {
                    (Some(Datum::Int(a)), Some(Datum::Int(b)), Some(Datum::Int(s))) => int_range(*a, *b, *s, &|n| budget.take(name, n)),
                    _ => Ok(Datum::Null),
                }
            })
            .collect(),
    }
}

/// `array_with_constant(n, v)`: n copies of v, every row's n checked before any output. A NULL
/// n reads as 0 (C++ reads the value under the NULL, 0 for every NULL a scan or cast gives).
fn with_constant(types: &[ColType], elem: &ColType, args: &[&[Datum]], rows: usize) -> Result<Vec<Datum>, ExecError> {
    let counts: Vec<i128> = args[0].iter().map(|d| if let Datum::Int(n) = d { *n } else { 0 }).collect();
    if counts.iter().any(|n| !(0..=MAX_ARRAY_SIZE).contains(n)) {
        return Err(invalid(format!(
            "Array size should in range(0, {MAX_ARRAY_SIZE}) in function: array_with_constant"
        )));
    }
    Budget::new().take("array_with_constant", counts.iter().sum())?;
    (0..rows)
        .map(|r| {
            Ok(Datum::Array(vec![
                crate::eval::coerce(args[1][r].clone(), &types[1], elem)?;
                counts[r] as usize
            ]))
        })
        .collect()
}

/// `array_join(a, sep[, null_replace])`: the non-NULL elements joined by sep; a NULL element is
/// skipped, or replaced by a non-empty null_replace. Any NULL argument is a NULL row.
fn join(args: &[&[Datum]], rows: usize) -> Vec<Datum> {
    (0..rows)
        .map(|r| {
            let (Datum::Array(a), Datum::Bytes(sep)) = (&args[0][r], &args[1][r]) else {
                return Datum::Null;
            };
            let replace: &[u8] = match args.get(2).map(|c| &c[r]) {
                None => b"",
                Some(Datum::Bytes(b)) => b,
                Some(_) => return Datum::Null,
            };
            let mut out = Vec::new();
            let mut first = true;
            for e in a {
                let part: &[u8] = match e {
                    Datum::Bytes(b) => b,
                    _ if !replace.is_empty() => replace,
                    _ => continue,
                };
                if !first {
                    out.extend_from_slice(sep);
                }
                first = false;
                out.extend_from_slice(part);
            }
            Datum::Bytes(out)
        })
        .collect()
}

/// The INT `array_range(start, end, step)`: NULL when a bound is negative or the step is not
/// positive; `[]` when end <= start; at most 1000000 elements.
fn int_range(start: i128, end: i128, step: i128, take: &dyn Fn(i128) -> Result<(), ExecError>) -> Result<Datum, ExecError> {
    if start < 0 || end < 0 || step <= 0 {
        return Ok(Datum::Null);
    }
    let distance = end - start;
    if distance <= 0 {
        return Ok(Datum::Array(vec![]));
    }
    let size = (distance - 1) / step + 1;
    if size > MAX_ARRAY_SIZE {
        return Err(too_large());
    }
    take(size)?;
    Ok(Datum::Array((0..size).map(|i| Datum::Int(start + i * step)).collect()))
}

/// A DATETIMEV2 range: `start`, then each value plus `step` units while below `end` (packed
/// values compare in time order); it stops when the next value leaves the date range.
fn time_range(start: i128, end: i128, unit: Unit, step: i64, take: &dyn Fn(i128) -> Result<(), ExecError>) -> Result<Datum, ExecError> {
    let mut out = Vec::new();
    let mut at = start;
    while at < end {
        if out.len() as i128 >= MAX_ARRAY_SIZE {
            return Err(too_large());
        }
        take(1)?;
        out.push(Datum::Int(at));
        match add(unit, Dt::unpack(at, true), step, true) {
            Some(next) => at = next.pack(true),
            None => break,
        }
    }
    Ok(Datum::Array(out))
}
