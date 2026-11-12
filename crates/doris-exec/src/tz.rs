//! `convert_tz` (`function_convert_tz.cpp:60-319`): see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-dates.md` §10. Zones resolve as the session
//! zone does (`clock::find_zone`, tzdata from chrono-tz). chrono-tz stops applying daylight
//! saving after 2099, where cctz goes on to 9999: a conversion past 2099 in a zone that keeps
//! daylight saving is refused.
use chrono::{Datelike, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Timelike};
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::clock::{find_zone, QueryZone};
use crate::datetime::Dt;
use crate::value::ColType;
use crate::ExecError;

pub fn is_function(name: &str) -> bool {
    name == "convert_tz"
}

/// `(DATETIMEV2(s), text, text) -> DATETIMEV2(s)`.
pub fn supported(args: &[ColType], to: &ColType) -> bool {
    let text = |t: &ColType| matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING);
    matches!(args, [a, b, c] if a.ptype == T::DATETIMEV2 && text(b) && text(c)) && to.ptype == T::DATETIMEV2 && to.scale == args[0].scale
}

fn zone(name: &[u8]) -> Result<QueryZone, ExecError> {
    let name = String::from_utf8_lossy(name);
    find_zone(&name).ok_or_else(|| ExecError::Coded {
        code: 33,
        name: "INVALID_ARGUMENT",
        msg: format!("Operation convert_tz invalid timezone: {name}"),
    })
}

/// `cctz::time_zone::name()`, as the error prints it.
fn zone_name(zone: &QueryZone) -> String {
    match zone {
        QueryZone::Named(tz) => tz.name().to_string(),
        QueryZone::Offset(0) => "UTC".into(),
        QueryZone::Offset(o) => {
            let a = o.unsigned_abs();
            format!(
                "Fixed/UTC{}{:02}:{:02}:{:02}",
                if *o < 0 { '-' } else { '+' },
                a / 3600,
                a / 60 % 60,
                a % 60
            )
        }
    }
}

/// The instant of civil `c` in `zone`, as cctz's lookup gives it: the earlier one for a
/// repeated time; for a skipped time the transition itself, and true (its microseconds are
/// dropped).
fn instant(zone: &QueryZone, c: NaiveDateTime) -> (i64, bool) {
    let QueryZone::Named(tz) = zone else {
        return (crate::clock::instant(zone, c), false);
    };
    match tz.from_local_datetime(&c) {
        LocalResult::Single(t) | LocalResult::Ambiguous(t, _) => (t.timestamp(), false),
        LocalResult::None => {
            let local = c.and_utc().timestamp();
            let before = crate::clock::offset_at(zone, local - 86_400);
            let after = crate::clock::offset_at(zone, local + 86_400);
            // the transition is in (local - after, local - before]: the first second at `after`
            let (mut lo, mut hi) = (local - after + 1, local - before);
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if crate::clock::offset_at(zone, mid) == after {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            (lo, true)
        }
    }
}

fn civil(t: &Dt) -> Option<NaiveDateTime> {
    NaiveDate::from_ymd_opt(t.year as i32, t.month as u32, t.day as u32)?.and_hms_opt(t.hour as u32, t.minute as u32, t.second as u32)
}

/// One row: None for a value past 9999 (or before year 0).
fn convert(t: Dt, from: &QueryZone, to: &QueryZone) -> Result<Option<Dt>, ExecError> {
    let c = civil(&t).ok_or_else(|| ExecError::BadRequest("convert_tz of an invalid datetime".into()))?;
    let (seconds, skipped) = instant(from, c);
    let local = crate::clock::local(to, seconds).ok_or_else(|| ExecError::BadRequest("convert_tz out of the clock's range".into()))?;
    crate::clock::refuse_past_2099(from, t.year, "convert_tz")?;
    crate::clock::refuse_past_2099(to, i64::from(local.year()), "convert_tz")?;
    if !(0..=9999).contains(&local.year()) {
        return Ok(None);
    }
    Ok(Some(Dt {
        year: i64::from(local.year()),
        month: i64::from(local.month()),
        day: i64::from(local.day()),
        hour: i64::from(local.hour()),
        minute: i64::from(local.minute()),
        second: i64::from(local.second()),
        micro: if skipped { 0 } else { t.micro },
    }))
}

/// Evaluates `convert_tz`; constant zones resolve once (an unknown one fails before any row,
/// a NULL one makes every row NULL).
pub fn eval(args: &[&[Datum]], constant: &[bool], rows: usize) -> Result<Vec<Datum>, ExecError> {
    // `open`: a NULL constant zone makes every row NULL, before any lookup; both zones constant
    // are looked up once (an unknown one fails before any row); otherwise per row
    let null_constant = (1..3).any(|i| constant[i] && args[i].first().is_some_and(|d| *d == Datum::Null));
    if null_constant {
        return Ok(vec![Datum::Null; rows]);
    }
    let mut fixed: [Option<QueryZone>; 2] = [None, None];
    if constant[1] && constant[2] {
        for (i, slot) in fixed.iter_mut().enumerate() {
            if let Some(Datum::Bytes(n)) = args[i + 1].first() {
                *slot = Some(zone(n)?);
            }
        }
    }
    (0..rows)
        .map(|r| {
            if args.iter().any(|a| a[r] == Datum::Null) {
                return Ok(Datum::Null);
            }
            let zone_at = |i: usize| -> Result<QueryZone, ExecError> {
                match fixed[i] {
                    Some(z) => Ok(z),
                    _ => match &args[i + 1][r] {
                        Datum::Bytes(n) => zone(n),
                        _ => Err(ExecError::BadRequest("convert_tz of a non-text zone".into())),
                    },
                }
            };
            let (from, to) = (zone_at(0)?, zone_at(1)?);
            let Datum::Int(v) = args[0][r] else {
                return Err(ExecError::BadRequest("convert_tz of a non-datetime".into()));
            };
            let t = Dt::unpack(v, true);
            match convert(t, &from, &to)? {
                Some(out) => Ok(Datum::Int(out.pack(true))),
                None => Err(ExecError::Coded {
                    code: -218,
                    name: "E-218",
                    msg: format!("Cannot convert {} from {} to {}", t.text(true), zone_name(&from), zone_name(&to)),
                }),
            }
        })
        .collect()
}
