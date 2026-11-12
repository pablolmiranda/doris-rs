//! Time rounding (`{year..second}_{floor,ceil}`, `function_datetime_floor_ceil.cpp`), and the
//! sub-second and interval-string adds (`milliseconds_add`, `day_hour_add`, …,
//! `function_date_or_datetime_computation.h`): see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-dates.md` §1-3.
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::datetime::{add, calc_daynr, date_from_daynr, Dt, Unit};
use crate::value::ColType;
use crate::ExecError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundUnit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
}

/// `year_floor` → (Year, ceil = false).
fn parse_round(name: &str) -> Option<(RoundUnit, bool)> {
    let (unit, op) = name.rsplit_once('_')?;
    let ceil = match op {
        "floor" => false,
        "ceil" => true,
        _ => return None,
    };
    let unit = match unit {
        "year" => RoundUnit::Year,
        "quarter" => RoundUnit::Quarter,
        "month" => RoundUnit::Month,
        "week" => RoundUnit::Week,
        "day" => RoundUnit::Day,
        "hour" => RoundUnit::Hour,
        "minute" => RoundUnit::Minute,
        "second" => RoundUnit::Second,
        _ => return None,
    };
    Some((unit, ceil))
}

/// The interval-string adds' parts (`get_interval_value`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Union {
    DayHour,
    DaySecond,
    MinuteSecond,
    SecondMicrosecond,
}

impl Union {
    fn parse(name: &str) -> Option<(Union, bool)> {
        let (unit, op) = name.rsplit_once('_')?;
        let sub = match op {
            "add" => false,
            "sub" => true,
            _ => return None,
        };
        let u = match unit {
            "day_hour" => Union::DayHour,
            "day_second" => Union::DaySecond,
            "minute_second" => Union::MinuteSecond,
            "second_microsecond" => Union::SecondMicrosecond,
            _ => return None,
        };
        Some((u, sub))
    }

    fn name(self) -> &'static str {
        match self {
            Union::DayHour => "day_hour_add",
            Union::DaySecond => "day_second_add",
            Union::MinuteSecond => "minute_second_add",
            Union::SecondMicrosecond => "second_microsecond_add",
        }
    }

    /// Seconds per part, microseconds last for SECOND_MICROSECOND.
    fn parts(self) -> &'static [i64] {
        match self {
            Union::DayHour => &[86_400, 3600],
            Union::DaySecond => &[86_400, 3600, 60, 1],
            Union::MinuteSecond => &[60, 1],
            Union::SecondMicrosecond => &[1, 0],
        }
    }
}

fn is_date(t: &ColType) -> bool {
    matches!(t.ptype, T::DATEV2 | T::DATETIMEV2)
}

fn same(a: &ColType, b: &ColType) -> bool {
    a.ptype == b.ptype && (a.ptype != T::DATETIMEV2 || a.scale == b.scale)
}

/// Whether this module evaluates `name` over `args` into `to`.
pub fn supported(name: &str, args: &[ColType], to: &ColType) -> bool {
    if parse_round(name).is_some() {
        let Some(a) = args.first() else { return false };
        let origin = |t: &ColType| t.ptype == a.ptype;
        let period = |t: &ColType| t.ptype == T::INT;
        let forms = match args {
            [_] => true,
            [_, b] => origin(b) || period(b),
            [_, b, c] => period(b) && origin(c),
            _ => false,
        };
        return is_date(a) && forms && same(a, to);
    }
    if matches!(name, "milliseconds_add" | "milliseconds_sub") {
        return matches!(args, [a, b] if a.ptype == T::DATETIMEV2 && b.ptype == T::BIGINT) && same(&args[0], to);
    }
    if Union::parse(name).is_some() {
        let text = |t: &ColType| matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING);
        return matches!(args, [a, b] if a.ptype == T::DATETIMEV2 && text(b)) && same(&args[0], to);
    }
    false
}

pub fn is_function(name: &str) -> bool {
    parse_round(name).is_some() || matches!(name, "milliseconds_add" | "milliseconds_sub") || Union::parse(name).is_some()
}

fn int(d: &Datum) -> i64 {
    match d {
        Datum::Int(v) => *v as i64,
        _ => 0,
    }
}

fn out_of_range(msg: String) -> ExecError {
    ExecError::Coded {
        code: -218,
        name: "E-218",
        msg,
    }
}

/// `0001-01-01 00:00:00`, the default origin (`FIRST_DAY`).
const FIRST_DAY: Dt = Dt {
    year: 1,
    month: 1,
    day: 1,
    hour: 0,
    minute: 0,
    second: 0,
    micro: 0,
};

/// `time_round_two_args` (`function_datetime_floor_ceil.cpp:646-781`): None when the result
/// leaves 0000-01-01..9999-12-31.
fn time_round(unit: RoundUnit, ceil: bool, a: Dt, period: i64, o: Dt, datetime: bool) -> Option<Dt> {
    let (va, vo) = (a.pack(datetime) as u64, o.pack(datetime) as u64);
    let (da, dor) = (calc_daynr(a.year, a.month, a.day), calc_daynr(o.year, o.month, o.day));
    let secs = |t: &Dt| t.hour * 3600 + t.minute * 60 + t.second;
    // the difference in whole units, and the parts below the unit, which decide the rounding
    let (mut diff, ta, to): (i64, i64, i64) = match unit {
        RoundUnit::Year => {
            let mask = if datetime { (1u64 << 46) - 1 } else { 0x1ff };
            (a.year - o.year, (va & mask) as i64, (vo & mask) as i64)
        }
        RoundUnit::Quarter => {
            let (shift, mask) = if datetime { (1i64 << 42, (1u64 << 42) - 1) } else { (32, 0x1f) };
            let trivial = |t: &Dt, v: u64| ((t.month - 1) % 3) * shift + (v & mask) as i64;
            ((a.year - o.year) * 4 + (a.month - o.month) / 3, trivial(&a, va), trivial(&o, vo))
        }
        RoundUnit::Month => {
            let mask = if datetime { (1u64 << 42) - 1 } else { 0x1f };
            ((a.year - o.year) * 12 + (a.month - o.month), (va & mask) as i64, (vo & mask) as i64)
        }
        RoundUnit::Week => {
            let trivial = |t: &Dt, d: i64| ((d % 7) * 86_400 + secs(t)) * 1_000_000 + t.micro;
            (da / 7 - dor / 7, trivial(&a, da), trivial(&o, dor))
        }
        RoundUnit::Day => {
            let mask = (1u64 << 37) - 1;
            let t = |v: u64| if datetime { (v & mask) as i64 } else { 0 };
            (da - dor, t(va), t(vo))
        }
        RoundUnit::Hour => {
            let mask = (1u64 << 32) - 1;
            let t = |v: u64| if datetime { (v & mask) as i64 } else { 0 };
            ((da - dor) * 24 + (a.hour - o.hour), t(va), t(vo))
        }
        RoundUnit::Minute => {
            let mask = (1u64 << 26) - 1;
            let t = |v: u64| if datetime { (v & mask) as i64 } else { 0 };
            (((da - dor) * 24 + (a.hour - o.hour)) * 60 + (a.minute - o.minute), t(va), t(vo))
        }
        RoundUnit::Second => ((da - dor) * 86_400 + (secs(&a) - secs(&o)), a.micro, o.micro),
    };
    if ceil {
        diff += i64::from(ta > to);
    } else {
        diff -= i64::from(ta < to);
    }
    let delta = if diff >= 0 { diff % period } else { (diff % period + period) % period };
    let step = diff - delta + if ceil && delta != 0 { period } else { 0 };
    let (u, n) = match unit {
        RoundUnit::Year => (Unit::Year, step),
        RoundUnit::Quarter => (Unit::Month, step.checked_mul(3)?),
        RoundUnit::Month => (Unit::Month, step),
        RoundUnit::Week => (Unit::Week, step),
        RoundUnit::Day => (Unit::Day, step),
        RoundUnit::Hour => (Unit::Hour, step),
        RoundUnit::Minute => (Unit::Minute, step),
        RoundUnit::Second => (Unit::Second, step),
    };
    add(u, o, n, datetime)
}

/// Adds seconds and microseconds as `date_add_interval` does for MILLISECOND and the union
/// units (`vdatetime_value.cpp:2524-2566`).
fn add_time(t: Dt, secs: i64, micros: i64) -> Option<Dt> {
    let mut us = t.micro.wrapping_add(micros);
    let extra = us / 1_000_000;
    us -= extra * 1_000_000;
    let mut s = ((t.day - 1) * 86_400 + t.hour * 3600 + t.minute * 60 + t.second)
        .wrapping_add(secs)
        .wrapping_add(extra);
    if us < 0 {
        s -= 1;
        us += 1_000_000;
    }
    let mut days = s / 86_400;
    s %= 86_400;
    if s < 0 {
        s += 86_400;
        days -= 1;
    }
    let (year, month, day) = date_from_daynr(calc_daynr(t.year, t.month, 1).checked_add(days)?)?;
    Some(Dt {
        year,
        month,
        day,
        hour: s / 3600,
        minute: (s / 60) % 60,
        second: s % 60,
        micro: us,
    })
}

/// `get_interval_value` / `extract_time_values`: the interval's sign and its parts, right
/// aligned; None when it is invalid.
fn parse_interval(text: &[u8], unit: Union) -> Option<(bool, Vec<i64>)> {
    let count = unit.parts().len();
    let (mut i, end) = (0, text.len());
    while i < end && (text[i] == b' ' || text[i] == b'\t') {
        i += 1;
    }
    let neg = i < end && text[i] == b'-';
    if neg {
        i += 1;
    }
    while i < end && !text[i].is_ascii_digit() {
        i += 1;
    }
    if i == end {
        return None;
    }
    let mut parts = Vec::with_capacity(count);
    let mut msec_length = 0;
    while parts.len() < count && i < end {
        let (mut value, start) = (0i64, i);
        while i < end && text[i].is_ascii_digit() {
            if value > (i64::MAX - 10) / 10 {
                return None;
            }
            value = value * 10 + i64::from(text[i] - b'0');
            i += 1;
        }
        msec_length = 6 - (i - start) as i64;
        parts.push(value);
        while i < end && !text[i].is_ascii_digit() {
            i += 1;
        }
    }
    if i != end {
        return None;
    }
    let mut aligned = vec![0; count - parts.len()];
    aligned.extend(parts);
    if unit == Union::SecondMicrosecond && (1..=6).contains(&msec_length) {
        let last = aligned.last_mut().expect("two parts");
        *last = last.wrapping_mul(10i64.pow(msec_length as u32));
    }
    Some((neg, aligned))
}

/// `StringRef::trim`: spaces only, both ends.
fn trim_spaces(s: &[u8]) -> &[u8] {
    let start = s.iter().position(|c| *c != b' ').unwrap_or(s.len());
    let end = s.iter().rposition(|c| *c != b' ').map_or(start, |e| e + 1);
    &s[start..end]
}

/// Evaluates `name`; `constant` says which arguments are constant expressions. A row with a
/// NULL argument is NULL and never throws.
pub fn eval(name: &str, types: &[ColType], args: &[&[Datum]], constant: &[bool], rows: usize) -> Result<Vec<Datum>, ExecError> {
    let datetime = types[0].ptype == T::DATETIMEV2;
    let dt = |d: &Datum| Dt::unpack(i128::from(int(d)), datetime);
    let null = |r: usize| args.iter().any(|a| a[r] == Datum::Null);
    if let Some((unit, ceil)) = parse_round(name) {
        let period_at = types.iter().position(|t| t.ptype == T::INT);
        let origin_at = (1..types.len()).find(|i| Some(*i) != period_at);
        // every row NULL: NULL rows, before the period is checked
        if rows > 0 && (0..rows).all(null) {
            return Ok(vec![Datum::Null; rows]);
        }
        // all arguments constant: C++ unwraps them and runs the column path, which names every
        // argument (the default origin too) in its errors
        let all_constant = constant.iter().all(|c| *c);
        if let Some(p) = period_at.filter(|p| constant[*p] && !all_constant) {
            let v = args[p].first().map(int).unwrap_or(1);
            if args[p].first() != Some(&Datum::Null) && v < 1 {
                return Err(out_of_range(format!("Operation {name} of {v} out of range")));
            }
        }
        // a constant default origin is the period form, in the error too
        // (`vector_const_const`: only when the period is constant too)
        let default_origin = !all_constant
            && period_at.is_some_and(|p| constant[p])
            && origin_at.is_some_and(|o| constant[o] && args[o].first().is_some_and(|d| *d != Datum::Null && dt(d) == FIRST_DAY));
        return (0..rows)
            .map(|r| {
                if null(r) {
                    return Ok(Datum::Null);
                }
                let a = dt(&args[0][r]);
                let period = period_at.map_or(1, |p| int(&args[p][r]));
                let o = origin_at.map_or(FIRST_DAY, |o| dt(&args[o][r]));
                let mut shown = vec![a.text(datetime)];
                if let Some(p) = period_at {
                    shown.push(int(&args[p][r]).to_string());
                }
                if let Some(o_at) = origin_at.filter(|_| !(default_origin && period_at.is_some())) {
                    shown.push(dt(&args[o_at][r]).text(datetime));
                }
                let fail = || out_of_range(format!("Operation {name} of {} out of range", shown.join(", ")));
                if period < 1 {
                    return Err(fail());
                }
                let t = time_round(unit, ceil, a, period, o, datetime).ok_or_else(fail)?;
                Ok(Datum::Int(t.pack(datetime)))
            })
            .collect();
    }
    if let Some(sub) = name.strip_prefix("milliseconds_").map(|op| op == "sub") {
        return (0..rows)
            .map(|r| {
                if null(r) {
                    return Ok(Datum::Null);
                }
                let t = dt(&args[0][r]);
                let delta = if sub { int(&args[1][r]).wrapping_neg() } else { int(&args[1][r]) };
                let fail = || out_of_range(format!("Operation millisecond_add of {}, {delta} out of range", t.text(true)));
                let out = add_time(t, 0, delta.wrapping_mul(1000)).ok_or_else(fail)?;
                Ok(Datum::Int(out.pack(true)))
            })
            .collect();
    }
    let (unit, sub) = Union::parse(name).ok_or_else(|| ExecError::Unsupported {
        what: format!("function {name}"),
    })?;
    (0..rows)
        .map(|r| {
            if null(r) {
                return Ok(Datum::Null);
            }
            let Datum::Bytes(given) = &args[1][r] else {
                return Err(ExecError::BadRequest(format!("{name} of a non-text interval")));
            };
            // _sub negates the trimmed string and goes through the add
            let text: Vec<u8> = if sub {
                let t = trim_spaces(given);
                match t.strip_prefix(b"-") {
                    Some(rest) => rest.to_vec(),
                    None => [b"-".as_slice(), t].concat(),
                }
            } else {
                given.clone()
            };
            let trimmed = trim_spaces(&text);
            let (neg, parts) = parse_interval(trimmed, unit).ok_or_else(|| ExecError::Coded {
                code: 33,
                name: "INVALID_ARGUMENT",
                msg: format!("Operation {} of {} is invalid", unit.name(), String::from_utf8_lossy(trimmed)),
            })?;
            let sign = if neg { -1 } else { 1 };
            let secs: i64 = unit
                .parts()
                .iter()
                .zip(&parts)
                .map(|(k, v)| k.wrapping_mul(*v))
                .fold(0i64, i64::wrapping_add);
            let micros = if unit == Union::SecondMicrosecond { parts[1] } else { 0 };
            let t = dt(&args[0][r]);
            let fail = || {
                out_of_range(format!(
                    "Operation {} of {}, {} out of range",
                    unit.name(),
                    t.text(true),
                    String::from_utf8_lossy(&text)
                ))
            };
            let out = add_time(t, secs.wrapping_mul(sign), micros.wrapping_mul(sign)).ok_or_else(fail)?;
            Ok(Datum::Int(out.pack(true)))
        })
        .collect()
}
