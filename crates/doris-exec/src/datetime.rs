//! Date functions, ported from the C++ BE (`core/value/vdatetime_value.cpp` and `.h`,
//! `util/time_lut.cpp`, `exprs/function/function_date_or_datetime_computation.h`,
//! `function_other_types_to_date.cpp`): the parts of a date, the add/sub family with its
//! ranges and error text, `days_diff`/`weeks_diff`, the week numbers, and `str_to_date`.
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::value::ColType;
use crate::ExecError;

/// The last day number C++ accepts: 9999-12-31 (`DATE_MAX_DAYNR`).
const DATE_MAX_DAYNR: i64 = 3_652_424;
const MAX_YEAR: i64 = 9999;
/// Days per month in a non-leap year, 1-based (`S_DAYS_IN_MONTH`).
const DAYS_IN_MONTH: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const WEEK_MONDAY_FIRST: u8 = 1;
const WEEK_YEAR: u8 = 2;
const WEEK_FIRST_WEEKDAY: u8 = 4;

/// A date or datetime, field by field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dt {
    pub year: i64,
    pub month: i64,
    pub day: i64,
    pub hour: i64,
    pub minute: i64,
    pub second: i64,
    pub micro: i64,
}

impl Dt {
    /// A packed DATEV2 (`y<<9 | m<<5 | d`) or DATETIMEV2
    /// (`y<<46 | m<<42 | d<<37 | h<<32 | mi<<26 | s<<20 | us`).
    pub fn unpack(v: i128, datetime: bool) -> Dt {
        let v = v as u64;
        if datetime {
            Dt {
                year: (v >> 46) as i64,
                month: ((v >> 42) & 0xf) as i64,
                day: ((v >> 37) & 0x1f) as i64,
                hour: ((v >> 32) & 0x1f) as i64,
                minute: ((v >> 26) & 0x3f) as i64,
                second: ((v >> 20) & 0x3f) as i64,
                micro: (v & 0xf_ffff) as i64,
            }
        } else {
            Dt {
                year: (v >> 9) as i64,
                month: ((v >> 5) & 0xf) as i64,
                day: (v & 0x1f) as i64,
                hour: 0,
                minute: 0,
                second: 0,
                micro: 0,
            }
        }
    }

    pub fn pack(&self, datetime: bool) -> i128 {
        let (y, m, d) = (self.year as u64, self.month as u64, self.day as u64);
        if datetime {
            let t = (self.hour as u64) << 32 | (self.minute as u64) << 26 | (self.second as u64) << 20 | self.micro as u64;
            i128::from(y << 46 | m << 42 | d << 37 | t)
        } else {
            i128::from(y << 9 | m << 5 | d)
        }
    }

    fn daynr(&self) -> i64 {
        calc_daynr(self.year, self.month, self.day)
    }

    fn time_micros(&self) -> i64 {
        ((self.hour * 60 + self.minute) * 60 + self.second) * 1_000_000 + self.micro
    }

    /// As `DateV2Value::to_string` prints it in an error: the fraction only when non-zero.
    fn text(&self, datetime: bool) -> String {
        let date = format!("{:04}-{:02}-{:02}", self.year, self.month, self.day);
        if !datetime {
            return date;
        }
        let time = format!("{date} {:02}:{:02}:{:02}", self.hour, self.minute, self.second);
        if self.micro == 0 {
            time
        } else {
            format!("{time}.{:06}", self.micro)
        }
    }
}

/// `is_leap` (`util/time_lut.h:36`): year 0 is not a leap year.
pub fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || (year % 400 == 0 && year != 0))
}

fn days_in_month(year: i64, month: i64) -> i64 {
    if month == 2 && is_leap(year) {
        29
    } else {
        DAYS_IN_MONTH[month as usize]
    }
}

/// `calc_daynr` (`vdatetime_value.h:1745-1772`): 0000-01-01 is day 1.
pub fn calc_daynr(year: i64, month: i64, day: i64) -> i64 {
    if year == 0 && month == 0 {
        return 0;
    }
    if year == 0 && month == 1 && day == 1 {
        return 1;
    }
    let mut y = year;
    let mut delsum = 365 * y + 31 * (month - 1) + day;
    if month <= 2 {
        y -= 1;
    } else {
        delsum -= (month * 4 + 23) / 10;
    }
    // C++'s division truncates toward zero, as Rust's does
    delsum + y / 4 - y / 100 + y / 400
}

/// `get_date_from_daynr` (`vdatetime_value.cpp:2404-2443`).
fn date_from_daynr(daynr: i64) -> Option<(i64, i64, i64)> {
    if daynr <= 0 || daynr > DATE_MAX_DAYNR {
        return None;
    }
    let mut year = daynr / 365;
    let mut before;
    loop {
        before = calc_daynr(year, 1, 1);
        if daynr >= before {
            break;
        }
        year -= 1;
    }
    let mut days_of_year = daynr - before + 1;
    let mut leap_day = 0;
    if is_leap(year) && days_of_year > 31 + 28 {
        days_of_year -= 1;
        if days_of_year == 31 + 28 {
            leap_day = 1;
        }
    }
    let mut month = 1;
    while days_of_year > DAYS_IN_MONTH[month as usize] {
        days_of_year -= DAYS_IN_MONTH[month as usize];
        month += 1;
    }
    (year <= MAX_YEAR).then_some((year, month, days_of_year + leap_day))
}

/// The add/sub family's units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    Year,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
}

impl Unit {
    fn parse(name: &str) -> Option<(Unit, bool)> {
        let (unit, op) = name.rsplit_once('_')?;
        let sub = match op {
            "add" => false,
            "sub" => true,
            _ => return None,
        };
        let unit = match unit {
            "years" => Unit::Year,
            "months" => Unit::Month,
            "weeks" => Unit::Week,
            "days" => Unit::Day,
            "hours" => Unit::Hour,
            "minutes" => Unit::Minute,
            "seconds" => Unit::Second,
            _ => return None,
        };
        Some((unit, sub))
    }

    /// `get_time_unit_name` (`datetime_errors.h:122-169`).
    fn op_name(self) -> &'static str {
        match self {
            Unit::Year => "year_add",
            Unit::Month => "month_add",
            Unit::Week => "week_add",
            Unit::Day => "day_add",
            Unit::Hour => "hour_add",
            Unit::Minute => "minute_add",
            Unit::Second => "second_add",
        }
    }
}

/// `date_time_add` (`function_date_or_datetime_computation.h:75-101`, `date_add_interval`
/// at `vdatetime_value.cpp:2533-2597`): None when the result leaves 0000-01-01..9999-12-31.
pub fn add(unit: Unit, t: Dt, delta: i64, datetime: bool) -> Option<Dt> {
    let mut out = t;
    match unit {
        Unit::Day | Unit::Week => {
            let days = delta.wrapping_mul(if unit == Unit::Week { 7 } else { 1 });
            let (y, m, d) = date_from_daynr(t.daynr().checked_add(days)?)?;
            (out.year, out.month, out.day) = (y, m, d);
        }
        Unit::Hour | Unit::Minute | Unit::Second => {
            let step = match unit {
                Unit::Hour => 3600,
                Unit::Minute => 60,
                _ => 1,
            };
            let mut seconds = (t.day - 1) * 86_400 + t.hour * 3600 + t.minute * 60 + t.second;
            seconds = seconds.wrapping_add(delta.wrapping_mul(step));
            let mut days = seconds / 86_400;
            seconds %= 86_400;
            if seconds < 0 {
                seconds += 86_400;
                days -= 1;
            }
            let (y, m, d) = date_from_daynr(calc_daynr(t.year, t.month, 1).checked_add(days)?)?;
            (out.year, out.month, out.day) = (y, m, d);
            if datetime {
                (out.hour, out.minute, out.second) = (seconds / 3600, (seconds / 60) % 60, seconds % 60);
            }
        }
        Unit::Year => {
            let year = t.year.checked_add(delta)?;
            if !(0..=MAX_YEAR).contains(&year) {
                return None;
            }
            out.year = year;
            if out.month == 2 && out.day == 29 && !is_leap(year) {
                out.day = 28;
            }
        }
        Unit::Month => {
            let months = (t.year * 12 + t.month - 1).checked_add(delta)?;
            if months < 0 || months / 12 > MAX_YEAR {
                return None;
            }
            out.year = months / 12;
            out.month = months % 12 + 1;
            if out.day > DAYS_IN_MONTH[out.month as usize] {
                out.day = days_in_month(out.year, out.month);
            }
        }
    }
    Some(out)
}

/// `throw_out_of_bound_date_int` (`datetime_errors.h:57-61`): code -218.
fn out_of_range(unit: Unit, t: Dt, datetime: bool, delta: i64) -> ExecError {
    ExecError::Coded {
        code: -218,
        name: "E-218",
        msg: format!("Operation {} of {}, {delta} out of range", unit.op_name(), t.text(datetime)),
    }
}

/// `date_diff_in_days_round_to_zero_by_time` (`vdatetime_value.h:1262-1271`): a − b.
pub fn days_diff(a: Dt, b: Dt) -> i64 {
    let mut day = a.daynr() - b.daynr();
    let time = a.time_micros() - b.time_micros();
    if day > 0 && time < 0 {
        day -= 1;
    } else if day < 0 && time > 0 {
        day += 1;
    }
    day
}

/// `mysql_week_mode` (`vdatetime_value.cpp:56-62`).
fn mysql_week_mode(mode: i64) -> u8 {
    let mut mode = (mode & 7) as u8;
    if mode & WEEK_MONDAY_FIRST == 0 {
        mode ^= WEEK_FIRST_WEEKDAY;
    }
    mode
}

/// `calc_weekday` (`util/time_lut.cpp:98-100`).
fn calc_weekday(daynr: i64, sunday_first: bool) -> i64 {
    (daynr + 5 + i64::from(sunday_first)) % 7
}

fn days_in_year(year: i64) -> i64 {
    if is_leap(year) {
        366
    } else {
        365
    }
}

/// `calc_week` (`vdatetime_value.cpp:3056-3113`): the week and the year it belongs to.
fn calc_week(t: Dt, mode: u8) -> (i64, i64) {
    if t.year == 0 {
        return (0, 0);
    }
    let monday_first = mode & WEEK_MONDAY_FIRST != 0;
    let mut week_year = mode & WEEK_YEAR != 0;
    let first_weekday = mode & WEEK_FIRST_WEEKDAY != 0;
    let mut daynr_first = calc_daynr(t.year, 1, 1);
    let mut weekday_first = calc_weekday(daynr_first, !monday_first);
    let mut year = t.year;
    if t.month == 1 && t.day <= 7 - weekday_first {
        if !week_year && ((first_weekday && weekday_first != 0) || (!first_weekday && weekday_first > 3)) {
            return (0, year);
        }
        year -= 1;
        week_year = true;
        let days = days_in_year(year);
        daynr_first -= days;
        weekday_first = (weekday_first + 53 * 7 - days) % 7;
    }
    let days = if (first_weekday && weekday_first != 0) || (!first_weekday && weekday_first > 3) {
        t.daynr() - (daynr_first + (7 - weekday_first))
    } else {
        t.daynr() - (daynr_first - weekday_first)
    };
    if week_year && days >= 52 * 7 {
        weekday_first = (weekday_first + days_in_year(year)) % 7;
        if (first_weekday && weekday_first == 0) || (!first_weekday && weekday_first <= 3) {
            return (1, year + 1);
        }
    }
    (days / 7 + 1, year)
}

/// `weekofyear`: `week(mysql_week_mode(3))`.
pub fn week_of_year(t: Dt) -> i64 {
    calc_week(t, mysql_week_mode(3)).0
}

/// `year_week` (`vdatetime_value.cpp:2379-2398`) of `mysql_week_mode(mode)`.
pub fn year_week(t: Dt, mode: i64) -> i64 {
    let mode = mysql_week_mode(mode);
    let (mut week, mut year) = calc_week(t, mode | WEEK_YEAR);
    if week == 53 && t.day >= 29 && mode & WEEK_FIRST_WEEKDAY == 0 {
        let monday_first = i64::from(mode & WEEK_MONDAY_FIRST);
        let last = calc_weekday(calc_daynr(t.year, 12, 31), monday_first == 0);
        if last - monday_first < 2 {
            year += 1;
            week = 1;
        }
    }
    year * 100 + week
}

/// `str_to_int64` (`vdatetime_value.cpp:895-962`) over at most `max` bytes: the value and the
/// bytes it used, or None without a digit.
fn read_int(v: &[u8], max: usize) -> Option<(i64, usize)> {
    let end = max.min(v.len());
    let mut i = 0;
    while i < end && (v[i] == b' ' || v[i] == b'\t') {
        i += 1;
    }
    let neg = i < end && v[i] == b'-';
    if i < end && (v[i] == b'-' || v[i] == b'+') {
        i += 1;
    }
    if i >= end || !v[i].is_ascii_digit() {
        return None;
    }
    let mut n: i64 = 0;
    while i < end && v[i].is_ascii_digit() {
        n = n * 10 + i64::from(v[i] - b'0');
        i += 1;
    }
    Some((if neg { -n } else { n }, i))
}

fn is_space(c: u8) -> bool {
    c == b' ' || (9..=13).contains(&c)
}

/// `str_to_date` (`function_other_types_to_date.cpp:73-243`, `from_date_format_str` at
/// `vdatetime_value.cpp:1792-2258`) for `%Y %y %m %c %d %e %H %k %i %s %S %%`: None for a value
/// that does not parse or is not a valid date; any other specifier is refused.
pub fn str_to_date(value: &[u8], format: &[u8], datetime: bool) -> Result<Option<Dt>, ExecError> {
    let format: &[u8] = match format {
        b"yyyyMMdd" => b"%Y%m%d",
        b"yyyy-MM-dd" => b"%Y-%m-%d",
        b"yyyy-MM-dd HH:mm:ss" => b"%Y-%m-%d %H:%i:%s",
        f => f,
    };
    let mut spec = format.iter();
    while let Some(c) = spec.next() {
        if *c == b'%' {
            match spec.next() {
                Some(b'Y' | b'y' | b'm' | b'c' | b'd' | b'e' | b'H' | b'k' | b'i' | b's' | b'S' | b'%') | None => {}
                Some(other) => {
                    return Err(ExecError::Unsupported {
                        what: format!("str_to_date with the format specifier %{}", *other as char),
                    })
                }
            }
        }
    }
    if value.is_empty() {
        return Ok(None);
    }
    const YEAR: u32 = 1;
    const MONTH: u32 = 2;
    const DAY: u32 = 4;
    const TIME: u32 = 8;
    let (mut ptr, mut val) = (0usize, 0usize);
    let mut used = 0u32;
    let mut t = Dt {
        year: 0,
        month: 0,
        day: 0,
        hour: 0,
        minute: 0,
        second: 0,
        micro: 0,
    };
    while ptr < format.len() && val < value.len() {
        while val < value.len() && is_space(value[val]) {
            val += 1;
        }
        if val >= value.len() {
            break;
        }
        if format[ptr] == b'%' && ptr + 1 < format.len() {
            let c = format[ptr + 1];
            ptr += 2;
            let rest = &value[val..];
            let (field, max, part): (Option<&mut i64>, usize, u32) = match c {
                b'Y' | b'y' => (Some(&mut t.year), if c == b'Y' { 4 } else { 2 }, YEAR),
                b'm' | b'c' => (Some(&mut t.month), 2, MONTH),
                b'd' | b'e' => (Some(&mut t.day), 2, DAY),
                b'H' | b'k' => (Some(&mut t.hour), 2, TIME),
                b'i' => (Some(&mut t.minute), 2, TIME),
                b's' | b'S' => (Some(&mut t.second), 2, TIME),
                _ => (None, 0, 0),
            };
            match field {
                Some(f) => {
                    let Some((n, used_bytes)) = read_int(rest, max) else { return Ok(None) };
                    *f = n;
                    if c == b'y' || (c == b'Y' && used_bytes <= 2) {
                        *f += if n >= 70 { 1900 } else { 2000 };
                    }
                    val += used_bytes;
                    used |= part;
                }
                // %%
                None => {
                    if rest.first() != Some(&b'%') {
                        return Ok(None);
                    }
                    val += 1;
                }
            }
        } else if !is_space(format[ptr]) {
            if format[ptr] != value[val] {
                return Ok(None);
            }
            ptr += 1;
            val += 1;
        } else {
            ptr += 1;
        }
    }
    // time specifiers left in the format still count as time parts
    while ptr < format.len() {
        if format[ptr] == b'%' && ptr + 1 < format.len() {
            if matches!(format[ptr + 1], b'H' | b'k' | b'i' | b's' | b'S') {
                used |= TIME;
            }
            ptr += 2;
        } else {
            ptr += 1;
        }
    }
    if used == 0 || (used & TIME != 0 && !datetime) {
        return Ok(None);
    }
    if used & TIME == 0 && used & DAY == 0 {
        t.day = 1;
        if used & MONTH == 0 {
            t.month = 1;
        }
    }
    let valid_date = (0..=MAX_YEAR).contains(&t.year) && (1..=12).contains(&t.month) && t.day >= 1 && t.day <= days_in_month(t.year, t.month);
    let valid_time = (0..24).contains(&t.hour) && (0..60).contains(&t.minute) && (0..60).contains(&t.second);
    Ok((valid_date && valid_time).then_some(t))
}

fn is_date(t: &ColType) -> bool {
    matches!(t.ptype, T::DATEV2 | T::DATETIMEV2)
}

/// Whether `name` over `args` into `to` is a date function this module evaluates.
pub fn supported(name: &str, args: &[ColType], to: &ColType) -> bool {
    let text = |t: &ColType| matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING);
    match name {
        "year" => args.len() == 1 && is_date(&args[0]) && to.ptype == T::SMALLINT,
        "month" | "dayofmonth" | "weekofyear" => args.len() == 1 && is_date(&args[0]) && to.ptype == T::TINYINT,
        "yearweek" => args.len() == 2 && is_date(&args[0]) && args[1].ptype == T::INT && to.ptype == T::INT,
        "days_diff" | "weeks_diff" => args.len() == 2 && args.iter().all(is_date) && to.ptype == T::BIGINT,
        "str_to_date" => args.len() == 2 && args.iter().all(text) && is_date(to),
        _ => match Unit::parse(name) {
            Some((unit, _)) => {
                let time_unit = matches!(unit, Unit::Hour | Unit::Minute | Unit::Second);
                args.len() == 2
                    && is_date(&args[0])
                    && (args[0].ptype == T::DATETIMEV2 || !time_unit)
                    && matches!(args[1].ptype, T::INT | T::BIGINT)
                    && to.ptype == args[0].ptype
                    && to.scale == args[0].scale
            }
            None => false,
        },
    }
}

/// The date functions' names, for the dispatch.
pub fn is_date_function(name: &str) -> bool {
    matches!(
        name,
        "year" | "month" | "dayofmonth" | "weekofyear" | "yearweek" | "days_diff" | "weeks_diff" | "str_to_date"
    ) || Unit::parse(name).is_some()
}

fn int(d: &Datum) -> i64 {
    match d {
        Datum::Int(v) => *v as i64,
        _ => 0,
    }
}

/// Evaluates date function `name`; a row with any NULL argument is NULL and never throws.
pub fn eval(name: &str, types: &[ColType], to: &ColType, args: &[&[Datum]], rows: usize) -> Result<Vec<Datum>, ExecError> {
    let dt_of = |i: usize, d: &Datum| Dt::unpack(i128::from(int(d)), types[i].ptype == T::DATETIMEV2);
    let mut out = Vec::with_capacity(rows);
    for r in 0..rows {
        let row: Vec<&Datum> = args.iter().map(|a| &a[r]).collect();
        if row.iter().any(|d| **d == Datum::Null) {
            out.push(Datum::Null);
            continue;
        }
        let v = match name {
            "year" => Datum::Int(i128::from(dt_of(0, row[0]).year)),
            "month" => Datum::Int(i128::from(dt_of(0, row[0]).month)),
            "dayofmonth" => Datum::Int(i128::from(dt_of(0, row[0]).day)),
            "weekofyear" => Datum::Int(i128::from(week_of_year(dt_of(0, row[0])))),
            "yearweek" => Datum::Int(i128::from(year_week(dt_of(0, row[0]), int(row[1])))),
            "days_diff" => Datum::Int(i128::from(days_diff(dt_of(0, row[0]), dt_of(1, row[1])))),
            "weeks_diff" => Datum::Int(i128::from(days_diff(dt_of(0, row[0]), dt_of(1, row[1])) / 7)),
            "str_to_date" => {
                let (Datum::Bytes(v), Datum::Bytes(f)) = (row[0], row[1]) else {
                    return Err(ExecError::BadRequest("str_to_date of a non-text value".into()));
                };
                let datetime = to.ptype == T::DATETIMEV2;
                match str_to_date(v, f, datetime)? {
                    Some(t) => Datum::Int(t.pack(datetime)),
                    None => Datum::Null,
                }
            }
            _ => {
                let (unit, sub) = Unit::parse(name).ok_or_else(|| ExecError::Unsupported {
                    what: format!("function {name}"),
                })?;
                let datetime = types[0].ptype == T::DATETIMEV2;
                let t = Dt::unpack(i128::from(int(row[0])), datetime);
                // `SubtractIntervalImpl` negates as Int64, then an INT interval narrows it back
                // (so INT_MIN stays INT_MIN)
                let delta = match (sub, types[1].ptype) {
                    (true, T::INT) => i64::from(int(row[1]).wrapping_neg() as i32),
                    (true, _) => int(row[1]).wrapping_neg(),
                    (false, _) => int(row[1]),
                };
                match add(unit, t, delta, datetime) {
                    Some(t) => Datum::Int(t.pack(datetime)),
                    None => return Err(out_of_range(unit, t, datetime, delta)),
                }
            }
        };
        out.push(v);
    }
    Ok(out)
}
