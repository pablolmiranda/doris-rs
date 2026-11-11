//! Unix time (`from_second`/`_millisecond`/`_microsecond`, `from_unixtime_new`,
//! `unix_timestamp_new`), `months_between`, `next_day` and `date_format` with its `%`
//! formatter, as the C++ BE computes them: see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-dates.md` §4-9. The query's zone is the
//! session's (`clock::current`); `from_unixtime` and `unix_timestamp` are their `_new`
//! versions, which the reference FE selects (`new_version_unix_timestamp`); without it they are
//! refused.
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::datetime::{calc_daynr, calc_week, calc_weekday, days_in_month, mysql_week_mode, str_to_date, Dt};
use crate::value::ColType;
use crate::ExecError;

/// `MAX_UNIX_TIMESTAMP_WITH_TIMEZONE`: 9999-12-31 23:59:59 UTC plus a day.
const MAX_UNIX_SECONDS: i64 = 253_402_300_799 + 86_400;
/// The longest format `open` accepts, after trimming spaces.
const MAX_FORMAT: usize = 128;

const NAMES: [&str; 8] = [
    "from_second",
    "from_millisecond",
    "from_microsecond",
    "from_unixtime",
    "unix_timestamp",
    "months_between",
    "next_day",
    "date_format",
];

pub fn is_function(name: &str) -> bool {
    NAMES.contains(&name)
}

fn text(t: &ColType) -> bool {
    matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING)
}

fn new_version() -> bool {
    crate::regexp::options().new_unix_timestamp
}

/// Whether this module evaluates `name` over `args` into `to`.
pub fn supported(name: &str, args: &[ColType], to: &ColType) -> bool {
    let dec = |t: &ColType, p: i32, s: i32| t.ptype == T::DECIMAL64 && t.precision == p && t.scale == s;
    match name {
        "from_second" | "from_millisecond" | "from_microsecond" => {
            let scale = match name {
                "from_second" => 0,
                "from_millisecond" => 3,
                _ => 6,
            };
            matches!(args, [a] if a.ptype == T::BIGINT) && to.ptype == T::DATETIMEV2 && to.scale == scale
        }
        "from_unixtime" => {
            new_version()
                && match args {
                    [a] | [a, _] => a.ptype == T::BIGINT || dec(a, 18, 6),
                    _ => false,
                }
                && args.get(1).is_none_or(text)
                && text(to)
        }
        "unix_timestamp" => {
            new_version()
                && match args {
                    [] => to.ptype == T::BIGINT,
                    [a] if a.ptype == T::DATEV2 => to.ptype == T::BIGINT,
                    [a] if a.ptype == T::DATETIMEV2 => dec(to, 12 + a.scale, a.scale),
                    [a, b] => text(a) && text(b) && dec(to, 18, 6),
                    _ => false,
                }
        }
        "months_between" => {
            matches!(args, [a, b, c] if a.ptype == T::DATEV2 && b.ptype == T::DATEV2 && c.ptype == T::BOOLEAN) && to.ptype == T::DOUBLE
        }
        "next_day" => matches!(args, [a, b] if a.ptype == T::DATEV2 && text(b)) && to.ptype == T::DATEV2,
        "date_format" => matches!(args, [a, b] if matches!(a.ptype, T::DATEV2 | T::DATETIMEV2) && text(b)) && text(to),
        _ => false,
    }
}

fn int(d: &Datum) -> i64 {
    match d {
        Datum::Int(v) => *v as i64,
        _ => 0,
    }
}

fn invalid(msg: String) -> ExecError {
    ExecError::Coded {
        code: 33,
        name: "INVALID_ARGUMENT",
        msg,
    }
}

fn out_of_range(msg: String) -> ExecError {
    ExecError::Coded {
        code: -218,
        name: "E-218",
        msg,
    }
}

fn zone() -> Result<crate::clock::QueryZone, ExecError> {
    crate::clock::current()
        .map(|c| c.zone)
        .ok_or_else(|| ExecError::Internal("a unix time function without the query's clock".into()))
}

fn civil_of(t: &Dt) -> Option<NaiveDateTime> {
    NaiveDate::from_ymd_opt(t.year as i32, t.month as u32, t.day as u32)?.and_hms_opt(t.hour as u32, t.minute as u32, t.second as u32)
}

fn dt_of(c: &NaiveDateTime, micro: i64) -> Dt {
    Dt {
        year: i64::from(c.year()),
        month: i64::from(c.month()),
        day: i64::from(c.day()),
        hour: i64::from(c.hour()),
        minute: i64::from(c.minute()),
        second: i64::from(c.second()),
        micro,
    }
}

/// The civil time of `seconds` in the session zone, when it is a valid date (year ≤ 9999).
fn local(zone: &crate::clock::QueryZone, seconds: i64, micro: i64) -> Option<Dt> {
    let c = crate::clock::local(zone, seconds)?;
    (c.year() <= 9999 && c.year() >= 0).then(|| dt_of(&c, micro))
}

const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DAYS: [&str; 7] = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

/// `string_to_impl`'s fast paths, then `to_format_string_without_check`'s `%` formatter
/// (`vdatetime_value.cpp:3113-3405`); None when the output passes the length guard.
fn format(t: &Dt, fmt: &[u8]) -> Option<String> {
    let (y, mo, d) = (t.year, t.month, t.day);
    let (h, mi, s) = (t.hour, t.minute, t.second);
    match fmt {
        b"%Y%m%d" | b"yyyyMMdd" => return Some(format!("{y:04}{mo:02}{d:02}")),
        b"%Y-%m-%d" | b"yyyy-MM-dd" => return Some(format!("{y:04}-{mo:02}-{d:02}")),
        b"%Y-%m-%d %H:%i:%s" | b"yyyy-MM-dd HH:mm:ss" => return Some(format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}")),
        b"yyyy-MM" => return Some(format!("{y:04}-{mo:02}")),
        b"yyyyMM" => return Some(format!("{y:04}{mo:02}")),
        b"yyyy" => return Some(format!("{y:04}")),
        _ => {}
    }
    let daynr = calc_daynr(y, mo, d);
    let hour12 = (h % 24 + 11) % 12 + 1;
    let mut out = String::new();
    let mut i = 0;
    while i < fmt.len() {
        if out.len() + 12 > 112 {
            return None;
        }
        let c = fmt[i];
        if c != b'%' || i + 1 == fmt.len() {
            out.push(c as char);
            i += 1;
            continue;
        }
        let spec = fmt[i + 1];
        i += 2;
        let week = |mode: i64| calc_week(*t, mysql_week_mode(mode));
        match spec {
            b'y' => out += &format!("{:02}", y % 100),
            b'Y' => out += &format!("{y:04}"),
            b'd' => out += &format!("{d:02}"),
            b'e' => out += &d.to_string(),
            b'm' => out += &format!("{mo:02}"),
            b'c' => out += &mo.to_string(),
            b'H' => out += &format!("{h:02}"),
            b'k' => out += &h.to_string(),
            b'h' | b'I' => out += &format!("{hour12:02}"),
            b'l' => out += &hour12.to_string(),
            b'i' => out += &format!("{mi:02}"),
            b's' | b'S' => out += &format!("{s:02}"),
            b'f' => out += &format!("{:06}", t.micro),
            b'p' => out += if h % 24 >= 12 { "PM" } else { "AM" },
            b'r' => out += &format!("{hour12:02}:{mi:02}:{s:02} {}", if h % 24 >= 12 { "PM" } else { "AM" }),
            b'T' => out += &format!("{h:02}:{mi:02}:{s:02}"),
            b'j' => out += &format!("{:03}", daynr - calc_daynr(y, 1, 1) + 1),
            b'a' => out += &DAYS[calc_weekday(daynr, false) as usize][..3],
            b'W' => out += DAYS[calc_weekday(daynr, false) as usize],
            b'b' => out += &MONTHS[(mo - 1) as usize][..3],
            b'M' => out += MONTHS[(mo - 1) as usize],
            b'D' => {
                let suffix = match (d % 100, d % 10) {
                    (11..=13, _) => "th",
                    (_, 1) => "st",
                    (_, 2) => "nd",
                    (_, 3) => "rd",
                    _ => "th",
                };
                out += &format!("{d}{suffix}");
            }
            b'w' => out += &calc_weekday(daynr, true).to_string(),
            b'u' => out += &format!("{:02}", week(1).0),
            b'U' => out += &format!("{:02}", week(0).0),
            b'v' => out += &format!("{:02}", week(3).0),
            b'V' => out += &format!("{:02}", week(2).0),
            b'x' => out += &format!("{:04}", week(3).1),
            b'X' => out += &format!("{:04}", week(2).1),
            other => out.push(other as char),
        }
    }
    Some(out)
}

/// `trim_spaces`: spaces only, both ends.
fn trim(s: &[u8]) -> &[u8] {
    let start = s.iter().position(|c| *c != b' ').unwrap_or(s.len());
    let end = s.iter().rposition(|c| *c != b' ').map_or(start, |e| e + 1);
    &s[start..end]
}

/// The constant format of `from_unixtime`/`date_format` (`open`): None when it is NULL or too
/// long, which fails at execute.
fn constant_format(name: &str, arg: &[Datum], constant: bool) -> Result<Option<Vec<u8>>, ExecError> {
    if !constant {
        return Err(invalid(format!("The second parameter of the function {name} must be a constant.")));
    }
    Ok(match arg.first() {
        Some(Datum::Bytes(f)) if trim(f).len() <= MAX_FORMAT => Some(trim(f).to_vec()),
        _ => None,
    })
}

/// `next_day`'s weekday names (`FunctionRelativeDay`): 1 Monday .. 7 Sunday.
fn weekday_number(s: &[u8]) -> Option<i64> {
    let up = s.to_ascii_uppercase();
    let n = match up.as_slice() {
        b"MO" | b"MON" | b"MONDAY" => 1,
        b"TU" | b"TUE" | b"TUESDAY" => 2,
        b"WE" | b"WED" | b"WEDNESDAY" => 3,
        b"TH" | b"THU" | b"THURSDAY" => 4,
        b"FR" | b"FRI" | b"FRIDAY" => 5,
        b"SA" | b"SAT" | b"SATURDAY" => 6,
        b"SU" | b"SUN" | b"SUNDAY" => 7,
        _ => return None,
    };
    Some(n)
}

/// `next_day` of one date: past 9999-12-31 the date stays.
fn next_day(t: Dt, wd: i64) -> Dt {
    let daynr = calc_daynr(t.year, t.month, t.day);
    let mut delta = (wd - (calc_weekday(daynr, false) + 1) + 7) % 7;
    if delta == 0 {
        delta = 7;
    }
    crate::datetime::add(crate::datetime::Unit::Day, t, delta, false).unwrap_or(t)
}

/// `months_between`'s `calc`.
fn months_between(a: Dt, b: Dt, round_off: bool) -> f64 {
    let diff = (a.year - b.year) * 12 + (a.month - b.month);
    let end = |t: &Dt| t.day == days_in_month(t.year, t.month);
    let day_term = if end(&a) && end(&b) { 0.0 } else { (a.day - b.day) as f64 / 31.0 };
    let v = diff as f64 + day_term;
    if round_off {
        (v * 1e8).round() / 1e8
    } else {
        v
    }
}

/// Evaluates `name`; `constant` says which arguments are constant expressions.
pub fn eval(name: &str, types: &[ColType], args: &[&[Datum]], constant: &[bool], rows: usize) -> Result<Vec<Datum>, ExecError> {
    let null = |r: usize| args.iter().any(|a| a[r] == Datum::Null);
    let datetime = |i: usize| types[i].ptype == T::DATETIMEV2;
    match name {
        "from_second" | "from_millisecond" | "from_microsecond" => {
            let ratio: i64 = match name {
                "from_second" => 1,
                "from_millisecond" => 1000,
                _ => 1_000_000,
            };
            let zone = zone()?;
            (0..rows)
                .map(|r| {
                    if null(r) {
                        return Ok(Datum::Null);
                    }
                    let v = int(&args[0][r]);
                    let fail = || out_of_range(format!("Operation {name} of {v} out of range"));
                    let seconds = v / ratio;
                    if v < 0 || seconds > MAX_UNIX_SECONDS {
                        return Err(fail());
                    }
                    let t = local(&zone, seconds, (v % ratio) * (1_000_000 / ratio)).ok_or_else(fail)?;
                    Ok(Datum::Int(t.pack(true)))
                })
                .collect()
        }
        "from_unixtime" => {
            let decimal = types[0].ptype == T::DECIMAL64;
            let fmt = match args.get(1) {
                Some(f) => constant_format(name, f, constant[1])?,
                None => Some(if decimal {
                    b"%Y-%m-%d %H:%i:%s.%f".to_vec()
                } else {
                    b"yyyy-MM-dd HH:mm:ss".to_vec()
                }),
            };
            let fmt = fmt.ok_or_else(|| invalid("Operation from_unixtime of invalid or oversized format is invalid".into()))?;
            let zone = zone()?;
            (0..rows)
                .map(|r| {
                    if null(r) {
                        return Ok(Datum::Null);
                    }
                    let v = int(&args[0][r]);
                    let (seconds, micro, shown) = if decimal {
                        (v / 1_000_000, v % 1_000_000, format!("{}.{}", v / 1_000_000, v % 1_000_000))
                    } else {
                        (v, 0, v.to_string())
                    };
                    let fail = || {
                        invalid(format!(
                            "Operation from_unixtime_new of {shown}, {} is invalid",
                            String::from_utf8_lossy(&fmt)
                        ))
                    };
                    let valid = if decimal {
                        seconds + micro.signum() >= 0
                    } else {
                        (0..=MAX_UNIX_SECONDS).contains(&seconds)
                    };
                    if !valid {
                        return Err(fail());
                    }
                    let t = local(&zone, seconds, micro).ok_or_else(fail)?;
                    format(&t, &fmt).map(|s| Datum::Bytes(s.into_bytes())).ok_or_else(fail)
                })
                .collect()
        }
        "unix_timestamp" => {
            let clock = crate::clock::current().ok_or_else(|| ExecError::Internal("unix_timestamp without the query's clock".into()))?;
            if types.is_empty() {
                return Ok(vec![Datum::Int(i128::from(clock.seconds)); rows]);
            }
            let zone = clock.zone;
            (0..rows)
                .map(|r| {
                    if null(r) {
                        return Ok(Datum::Null);
                    }
                    let (t, scale) = match args {
                        [a] => (
                            Dt::unpack(i128::from(int(&a[r])), datetime(0)),
                            if datetime(0) { Some(types[0].scale) } else { None },
                        ),
                        [s, f] => {
                            let (Datum::Bytes(s), Datum::Bytes(f)) = (&s[r], &f[r]) else {
                                return Err(ExecError::BadRequest("unix_timestamp of non-text values".into()));
                            };
                            let fail = || {
                                invalid(format!(
                                    "Operation unix_timestamp of {}, {} is invalid",
                                    String::from_utf8_lossy(s),
                                    String::from_utf8_lossy(f)
                                ))
                            };
                            (str_to_date(s, f, true)?.ok_or_else(fail)?, Some(6))
                        }
                        _ => return Err(ExecError::BadRequest("unix_timestamp of three arguments".into())),
                    };
                    let civil = civil_of(&t).ok_or_else(|| ExecError::BadRequest("unix_timestamp of an invalid date".into()))?;
                    let seconds = crate::clock::instant(&zone, civil);
                    // trim_timestamp: before 1970 is 0
                    let (seconds, micro) = if seconds < 0 { (0, 0) } else { (seconds, t.micro) };
                    Ok(Datum::Int(match scale {
                        None => i128::from(seconds),
                        Some(s) => i128::from(seconds) * 10i128.pow(s as u32) + i128::from(micro / 10i64.pow(6 - s as u32)),
                    }))
                })
                .collect()
        }
        "months_between" => (0..rows)
            .map(|r| {
                if null(r) {
                    return Ok(Datum::Null);
                }
                let (a, b) = (
                    Dt::unpack(i128::from(int(&args[0][r])), false),
                    Dt::unpack(i128::from(int(&args[1][r])), false),
                );
                Ok(Datum::Double(months_between(a, b, args[2][r] == Datum::Bool(true))))
            })
            .collect(),
        "next_day" => {
            let weekday = |r: usize| match &args[1][r] {
                Datum::Bytes(s) => {
                    weekday_number(s).ok_or_else(|| invalid(format!("Function next_day failed to parse weekday: {}", String::from_utf8_lossy(s))))
                }
                _ => Err(ExecError::BadRequest("next_day of a non-text weekday".into())),
            };
            // C++ moves a constant date in place: with a column weekday it accumulates
            let accumulate = constant[0] && !constant[1];
            let mut cur: Option<Dt> = None;
            (0..rows)
                .map(|r| {
                    if null(r) {
                        return Ok(Datum::Null);
                    }
                    let given = Dt::unpack(i128::from(int(&args[0][r])), false);
                    let from = if accumulate { cur.unwrap_or(given) } else { given };
                    let t = next_day(from, weekday(r)?);
                    cur = Some(t);
                    Ok(Datum::Int(t.pack(false)))
                })
                .collect()
        }
        "date_format" => {
            let fmt = constant_format(name, args[1], constant[1])?
                .ok_or_else(|| invalid("Operation date_format of invalid or oversized format is invalid".into()))?;
            (0..rows)
                .map(|r| {
                    if null(r) {
                        return Ok(Datum::Null);
                    }
                    let t = Dt::unpack(i128::from(int(&args[0][r])), datetime(0));
                    format(&t, &fmt).map(|s| Datum::Bytes(s.into_bytes())).ok_or_else(|| {
                        invalid(format!(
                            "Operation date_format of {}, {} is invalid",
                            t.text(datetime(0)),
                            String::from_utf8_lossy(&fmt)
                        ))
                    })
                })
                .collect()
        }
        _ => Err(ExecError::Unsupported {
            what: format!("function {name}"),
        }),
    }
}
