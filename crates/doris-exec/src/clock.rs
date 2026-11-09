//! The query's clock and time zone: `now()`, `current_date()`, and text with a time zone cast to
//! a date or datetime, as the C++ BE evaluates them.
//!
//! - The BE has no clock of its own: `TQueryGlobals` carries the instant and the zone, the same for
//!   every instance of a query or load (`runtime/runtime_state.cpp:177-203`, `fragment_mgr.cpp:525`).
//! - `now(n)` keeps `n` digits of `nano_seconds`, cut, not rounded; `now()` none; `current_date()`
//!   is the instant's date in the query zone (`function_date_or_datetime_computation.h:1014-1150`).
//! - A zone after the time part converts the civil time from that zone to the query zone, the
//!   fraction unchanged; a DATEV2 target checks the zone but keeps its date
//!   (`cast_to_datetimev2_impl.hpp:635-742`, `cast_to_datev2_impl.hpp:505-556`). Zone names are
//!   looked up as `TimezoneUtils::find_cctz_time_zone` does (`util/timezone_utils.cpp:130-290`).
//!
//! The clock is set for the thread that evaluates a query instance or a load (`with_clock`).
use std::cell::RefCell;

use chrono::{DateTime, Datelike, Duration, LocalResult, NaiveDate, NaiveDateTime, Offset, TimeZone, Timelike};
use chrono_tz::Tz;
use doris_thrift::gen::palo_internal_service::TQueryGlobals;

/// A time zone: a fixed offset in seconds east of UTC, or a tzdata zone.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QueryZone {
    Offset(i32),
    Named(Tz),
}

/// The instant a query runs at, and its zone.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueryClock {
    pub zone: QueryZone,
    pub seconds: i64,
    pub nanos: i32,
}

const PLUS_EIGHT: QueryZone = QueryZone::Offset(8 * 3600);

/// `normalize_offset_string`: `±h[h][:mm]` (hour-only when allowed) as seconds, within
/// -12:00..+14:00.
fn offset_seconds(s: &str, hour_only: bool) -> Option<i32> {
    let (sign, rest) = match s.as_bytes().first()? {
        b'+' => (1, &s[1..]),
        b'-' => (-1, &s[1..]),
        _ => return None,
    };
    let digits = |v: &str, n: &[usize]| n.contains(&v.len()) && v.bytes().all(|b| b.is_ascii_digit());
    let (h, m) = match rest.split_once(':') {
        Some((h, m)) if digits(h, &[1, 2]) && digits(m, &[2]) => (h.parse::<i32>().ok()?, m.parse::<i32>().ok()?),
        None if hour_only && digits(rest, &[1, 2]) => (rest.parse::<i32>().ok()?, 0),
        _ => return None,
    };
    if m >= 60 || h * 60 + m > if sign > 0 { 14 * 60 } else { 12 * 60 } {
        return None;
    }
    Some(sign * (h * 3600 + m * 60))
}

/// A zone by name, as `find_cctz_time_zone` resolves it: tzdata names case-insensitively, `Z`,
/// `CST` (Asia/Shanghai), `±HH:MM` offsets, then `UTC±h[:mm]`, `GMT±h[:mm]` and `±h:mm`.
pub fn find_zone(name: &str) -> Option<QueryZone> {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "z" => return Some(QueryZone::Named(Tz::Zulu)),
        "cst" => return Some(QueryZone::Named(Tz::Asia__Shanghai)),
        _ => {}
    }
    if let Ok(tz) = Tz::from_str_insensitive(name) {
        return Some(QueryZone::Named(tz));
    }
    if matches!(lower.as_str(), "utc" | "etc/utc" | "zulu") {
        return Some(QueryZone::Named(Tz::UTC));
    }
    if (lower.starts_with("utc") || lower.starts_with("gmt")) && name.len() > 3 {
        return offset_seconds(&name[3..], true).map(QueryZone::Offset);
    }
    if name.starts_with(['+', '-']) {
        return offset_seconds(name, false).map(QueryZone::Offset);
    }
    None
}

fn local(zone: &QueryZone, seconds: i64) -> Option<NaiveDateTime> {
    let utc = DateTime::from_timestamp(seconds, 0)?;
    Some(match zone {
        QueryZone::Offset(o) => utc.naive_utc() + Duration::seconds(i64::from(*o)),
        QueryZone::Named(tz) => utc.with_timezone(tz).naive_local(),
    })
}

/// The instant of a civil time in a zone (`cctz::convert(civil, zone)`): an ambiguous time takes
/// its earlier instant, a skipped one the offset before the transition.
fn instant(zone: &QueryZone, civil: NaiveDateTime) -> i64 {
    match zone {
        QueryZone::Offset(o) => (civil - Duration::seconds(i64::from(*o))).and_utc().timestamp(),
        QueryZone::Named(tz) => match tz.from_local_datetime(&civil) {
            LocalResult::Single(t) | LocalResult::Ambiguous(t, _) => t.timestamp(),
            LocalResult::None => {
                let before = tz.offset_from_utc_datetime(&(civil - Duration::hours(24))).fix().local_minus_utc();
                (civil - Duration::seconds(i64::from(before))).and_utc().timestamp()
            }
        },
    }
}

impl QueryClock {
    pub fn from_globals(g: &TQueryGlobals) -> QueryClock {
        if let Some(name) = &g.time_zone {
            // an unknown name leaves cctz's default zone, UTC
            let zone = find_zone(name).unwrap_or(QueryZone::Named(Tz::UTC));
            return QueryClock {
                zone,
                seconds: g.timestamp_ms.unwrap_or(0) / 1000,
                nanos: g.nano_seconds.unwrap_or(0),
            };
        }
        let civil = NaiveDateTime::parse_from_str(&g.now_string, "%Y-%m-%d %H:%M:%S").ok();
        let seconds = civil.map_or(0, |c| instant(&PLUS_EIGHT, c));
        QueryClock {
            zone: PLUS_EIGHT,
            seconds,
            nanos: 0,
        }
    }

    /// The clock of a request that may carry no globals: +08:00 at instant 0, as C++'s
    /// `RuntimeState` defaults.
    pub fn from_optional(g: Option<&TQueryGlobals>) -> QueryClock {
        g.map_or(
            QueryClock {
                zone: PLUS_EIGHT,
                seconds: 0,
                nanos: 0,
            },
            QueryClock::from_globals,
        )
    }

    fn civil(&self) -> NaiveDateTime {
        local(&self.zone, self.seconds).unwrap_or_default()
    }

    /// `now(scale)` as a packed DATETIMEV2.
    pub fn now(&self, scale: i32) -> u64 {
        let scale = scale.clamp(0, 6) as u32;
        let us = (self.nanos.max(0) as u64) / 10u64.pow(9 - scale) * 10u64.pow(6 - scale);
        pack(&self.civil(), us)
    }

    /// `current_date()` as a packed DATEV2.
    pub fn today(&self) -> u32 {
        let c = self.civil();
        (c.year() as u32) << 9 | c.month() << 5 | c.day()
    }
}

fn pack(c: &NaiveDateTime, us: u64) -> u64 {
    (c.year() as u64) << 46
        | u64::from(c.month()) << 42
        | u64::from(c.day()) << 37
        | u64::from(c.hour()) << 32
        | u64::from(c.minute()) << 26
        | u64::from(c.second()) << 20
        | us
}

thread_local! {
    static CLOCK: RefCell<Option<QueryClock>> = const { RefCell::new(None) };
}

/// Runs `f` with `clock` as this thread's query clock.
pub fn with_clock<R>(clock: &QueryClock, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<QueryClock>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CLOCK.with(|c| *c.borrow_mut() = self.0);
        }
    }
    let _restore = Restore(CLOCK.with(|c| c.borrow_mut().replace(*clock)));
    f()
}

/// This thread's query clock, if a query or load is running on it.
pub fn current() -> Option<QueryClock> {
    CLOCK.with(|c| *c.borrow())
}

/// The cast grammar's zone after a time part: `±h[h][[:]mm]` with minutes 00, 30 or 45 (and +14
/// only at :00), or a name.
/// Trailing whitespace may follow a whole zone; an hour-only offset followed by anything, a space
/// too, must go on with two minute digits.
fn suffix_zone(z: &str) -> Option<QueryZone> {
    let Some(rest) = z.strip_prefix(['+', '-']) else {
        // a name runs to the first whitespace; only whitespace may follow it
        let name = z.trim_end();
        return if name.contains(char::is_whitespace) { None } else { find_zone(name) };
    };
    let sign = if z.starts_with('-') { -1 } else { 1 };
    let run = rest.bytes().take_while(u8::is_ascii_digit).count();
    let hour_len = if run == 1 || run == 3 { 1 } else { 2 };
    let h: i32 = rest.get(..hour_len)?.parse().ok()?;
    let mut tail = &rest[hour_len..];
    let mut m = 0;
    if !tail.is_empty() {
        tail = tail.strip_prefix(':').unwrap_or(tail);
        let digits = tail.get(..2).filter(|d| d.bytes().all(|b| b.is_ascii_digit()))?;
        if !tail[2..].trim_end().is_empty() {
            return None;
        }
        m = digits.parse().ok()?;
    }
    if h >= 15 || ![0, 30, 45].contains(&m) || (h == 14 && m != 0) {
        return None;
    }
    find_zone(&format!("{}{h:02}:{m:02}", if sign < 0 { '-' } else { '+' }))
}

/// Splits a date-time text at its zone: the first character after the time part's separator
/// that is not a digit, `:`, `.` or whitespace. None when the text has no zone.
pub fn split_zone(s: &str) -> Option<(&str, &str)> {
    let s = s.trim_start();
    let sep = s.find([' ', 'T'])?;
    let at = s[sep + 1..].find(|c: char| !(c.is_ascii_digit() || c == ':' || c == '.' || c.is_whitespace()))? + sep + 1;
    Some((s[..at].trim_end(), &s[at..]))
}

/// A packed DATETIMEV2 written in `zone`, as civil time in the query zone; None for an unknown
/// zone, no query clock, or a year past 9999.
pub fn convert_from(packed: u64, zone: &str) -> Option<u64> {
    let given = suffix_zone(zone)?;
    let query = current()?.zone;
    let y = (packed >> 46) as i32;
    let civil = NaiveDate::from_ymd_opt(y, ((packed >> 42) & 0xF) as u32, ((packed >> 37) & 0x1F) as u32)?.and_hms_opt(
        ((packed >> 32) & 0x1F) as u32,
        ((packed >> 26) & 0x3F) as u32,
        ((packed >> 20) & 0x3F) as u32,
    )?;
    let out = local(&query, instant(&given, civil))?;
    if out.year() > 9999 || out.year() < 0 {
        return None;
    }
    Some(pack(&out, packed & 0xF_FFFF))
}

/// Whether a zone is one the cast accepts (a DATEV2 target only checks it).
pub fn valid_zone(zone: &str) -> bool {
    suffix_zone(zone).is_some()
}
