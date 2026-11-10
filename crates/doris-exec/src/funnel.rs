//! `window_funnel_v2` (`aggregate_function_window_funnel_v2.h`), ported line for line: the matched
//! events of each row (conditions from the last to the first, all after the first flagged as the
//! same row), merged by a stable sort on the timestamp, then one of four modes.
use chrono::{Datelike, NaiveDate, Timelike};

use crate::ExecError;

const CONTINUATION: u8 = 0x80;
const IDX_MASK: u8 = 0x7F;
/// The most conditions an event index holds (7 bits).
pub const MAX_CONDITIONS: usize = IDX_MASK as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Default,
    Increase,
    Deduplication,
    Fixed,
    Invalid,
}

/// `string_to_window_funnel_mode`: case-sensitive.
pub fn mode(s: &[u8]) -> Mode {
    match s {
        b"default" => Mode::Default,
        b"increase" => Mode::Increase,
        b"deduplication" => Mode::Deduplication,
        b"fixed" => Mode::Fixed,
        _ => Mode::Invalid,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Funnel {
    pub event_count: usize,
    /// -1 until a row sets it
    pub window: i64,
    pub mode: Mode,
    /// (packed DATETIMEV2, event index with the continuation flag), in insertion order
    pub events: Vec<(u64, u8)>,
}

fn idx(raw: u8) -> i32 {
    i32::from(raw & IDX_MASK)
}

fn cont(raw: u8) -> bool {
    raw & CONTINUATION != 0
}

/// `date_add_interval<SECOND>` on a packed DATETIMEV2, microseconds kept; a result outside years
/// 1..=9999 leaves the value as it is.
fn add_seconds(packed: u64, secs: i64) -> u64 {
    let y = (packed >> 46) as i32;
    let mo = ((packed >> 42) & 0xF) as u32;
    let d = ((packed >> 37) & 0x1F) as u32;
    let h = ((packed >> 32) & 0x1F) as u32;
    let mi = ((packed >> 26) & 0x3F) as u32;
    let s = ((packed >> 20) & 0x3F) as u32;
    let us = packed & 0xF_FFFF;
    let Some(t) = NaiveDate::from_ymd_opt(y, mo, d).and_then(|d| d.and_hms_opt(h, mi, s)) else {
        return packed;
    };
    let Some(t) = chrono::TimeDelta::try_seconds(secs).and_then(|dt| t.checked_add_signed(dt)) else {
        return packed;
    };
    if !(1..=9999).contains(&t.year()) {
        return packed;
    }
    (t.year() as u64) << 46
        | u64::from(t.month()) << 42
        | u64::from(t.day()) << 37
        | u64::from(t.hour()) << 32
        | u64::from(t.minute()) << 26
        | u64::from(t.second()) << 20
        | us
}

impl Funnel {
    pub fn new(event_count: usize) -> Funnel {
        Funnel {
            event_count,
            window: -1,
            mode: Mode::Invalid,
            events: vec![],
        }
    }

    /// One row: its window and mode win; true conditions from the last to the first.
    pub fn add(&mut self, window: i64, mode: Mode, ts: u64, conditions: &[bool]) {
        self.window = window;
        self.mode = mode;
        let mut first = true;
        for i in (0..conditions.len().min(self.event_count)).rev() {
            if conditions[i] {
                let mut packed = (i + 1) as u8;
                if !first {
                    packed |= CONTINUATION;
                }
                first = false;
                self.events.push((ts, packed));
            }
        }
    }

    pub fn merge(&mut self, other: &Funnel) {
        if other.events.is_empty() {
            return;
        }
        self.events.extend_from_slice(&other.events);
        if self.event_count == 0 {
            self.event_count = other.event_count;
        }
        if self.window == -1 {
            self.window = other.window;
        }
        if self.mode == Mode::Invalid {
            self.mode = other.mode;
        }
    }

    /// `get()`, after the stable sort by timestamp.
    pub fn get(&self) -> Result<i32, ExecError> {
        if self.event_count == 0 || self.events.is_empty() {
            return Ok(0);
        }
        if self.window < 0 {
            return Err(ExecError::Coded {
                code: doris_thrift::gen::status::TStatusCode::INVALID_ARGUMENT.0,
                name: "INVALID_ARGUMENT",
                msg: format!("the sliding time window must be a positive integer, but got: {}", self.window),
            });
        }
        let mut ev = self.events.clone();
        ev.sort_by_key(|e| e.0);
        let s = Sorted {
            ev,
            n: self.event_count as i32,
            window: self.window,
        };
        Ok(match self.mode {
            Mode::Default => s.default(),
            Mode::Increase => s.increase(),
            Mode::Deduplication => s.deduplication(),
            Mode::Fixed => s.fixed(),
            Mode::Invalid => return Err(ExecError::Internal("Invalid window_funnel mode".into())),
        })
    }
}

#[derive(Clone, Copy, Default)]
struct Pair {
    first: u64,
    last: u64,
    at: usize,
}

impl Pair {
    fn set(&self) -> bool {
        self.first != 0
    }
}

struct Sorted {
    ev: Vec<(u64, u8)>,
    n: i32,
    window: i64,
}

impl Sorted {
    fn within(&self, base: u64, cur: u64) -> bool {
        cur <= add_seconds(base, self.window)
    }

    fn same_row(&self, prev: usize, cur: usize) -> bool {
        cur > prev && (prev + 1..=cur).all(|j| cont(self.ev[j].1))
    }

    fn next_row_start(&self, mut i: usize) -> usize {
        while i < self.ev.len() && cont(self.ev[i].1) {
            i += 1;
        }
        i
    }

    fn row_start(&self, mut i: usize) -> usize {
        while i > 0 && cont(self.ev[i].1) {
            i -= 1;
        }
        i
    }

    fn row_contains(&self, first: usize, level: i32) -> bool {
        if idx(self.ev[first].1) - 1 == level {
            return true;
        }
        let mut j = first + 1;
        while j < self.ev.len() && cont(self.ev[j].1) {
            if idx(self.ev[j].1) - 1 == level {
                return true;
            }
            j += 1;
        }
        false
    }

    fn default(&self) -> i32 {
        let mut ts = vec![Pair::default(); self.n as usize];
        for (i, &(t, raw)) in self.ev.iter().enumerate() {
            let e = idx(raw) - 1;
            if e == 0 {
                ts[0] = Pair { first: t, last: t, at: i };
            } else {
                let prev = ts[(e - 1) as usize];
                if prev.set() && !self.same_row(prev.at, i) && self.within(prev.first, t) {
                    ts[e as usize] = Pair {
                        first: prev.first,
                        last: t,
                        at: i,
                    };
                    if e + 1 == self.n {
                        return self.n;
                    }
                }
            }
        }
        (1..=self.n).rev().find(|&k| ts[(k - 1) as usize].set()).unwrap_or(0)
    }

    fn increase(&self) -> i32 {
        let mut max = 0;
        let len = self.ev.len();
        for start in 0..len {
            if (len - start) as i32 <= max {
                break;
            }
            if idx(self.ev[start].1) - 1 != 0 {
                continue;
            }
            let mut ts = vec![Pair::default(); self.n as usize];
            ts[0] = Pair {
                first: self.ev[start].0,
                last: self.ev[start].0,
                at: start,
            };
            let mut level = 0;
            for i in start + 1..len {
                let (t, raw) = self.ev[i];
                let e = idx(raw) - 1;
                if e == 0 {
                    continue;
                }
                let prev = ts[(e - 1) as usize];
                if prev.set() && !self.same_row(prev.at, i) && self.within(prev.first, t) && prev.last < t {
                    ts[e as usize] = Pair {
                        first: prev.first,
                        last: t,
                        at: i,
                    };
                    level = level.max(e);
                    if e + 1 == self.n {
                        return self.n;
                    }
                }
            }
            max = max.max(level + 1);
        }
        max
    }

    fn deduplication(&self) -> i32 {
        let mut max = 0;
        let len = self.ev.len();
        for start in 0..len {
            if (len - start) as i32 <= max {
                break;
            }
            if idx(self.ev[start].1) - 1 != 0 {
                continue;
            }
            let first = self.ev[start].0;
            let mut level = 0;
            let mut last = start;
            for i in start + 1..len {
                let e = idx(self.ev[i].1) - 1;
                if e != level + 1 || self.same_row(last, i) {
                    continue;
                }
                if !self.within(first, self.ev[i].0) {
                    break;
                }
                let (gap_start, gap_end) = (self.next_row_start(last + 1), self.row_start(i));
                if (gap_start..gap_end).any(|j| idx(self.ev[j].1) - 1 < e) {
                    break;
                }
                level = e;
                last = i;
                if level + 1 == self.n {
                    return self.n;
                }
            }
            max = max.max(level + 1);
        }
        max
    }

    fn fixed(&self) -> i32 {
        let mut max = -1;
        let len = self.ev.len();
        for start in 0..len {
            if idx(self.ev[start].1) != 1 {
                continue;
            }
            let base = self.ev[start].0;
            let mut level = 0;
            let mut i = start + 1;
            while i < len && level + 1 < self.n {
                i = self.next_row_start(i);
                if i >= len || !self.within(base, self.ev[i].0) {
                    break;
                }
                let expected = level + 1;
                if idx(self.ev[i].1) - 1 < expected || !self.row_contains(i, expected) {
                    break;
                }
                level = expected;
                i = self.next_row_start(i + 1);
            }
            if level > max {
                max = level;
                if max + 1 == self.n {
                    return self.n;
                }
            }
            if (len - start) as i32 <= max {
                break;
            }
        }
        max + 1
    }
}
