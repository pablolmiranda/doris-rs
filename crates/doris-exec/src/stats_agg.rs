//! Batch 3's aggregates, as the C++ BE computes them (`be/src/exprs/aggregate/`):
//! `percentile`/`percentile_array` (the `new_version_percentile` classes,
//! `aggregate_function_percentile.h:434-619`), `retention`, `linear_histogram`,
//! `exponential_moving_average` (`aggregate_function_ema.h`) and `group_bit_and/or/xor`. See
//! `docs/superpowers/research/2026-11-10-m6-s7b3-aggregates.md`. A partial state is doris-rs's
//! own encoding.
use std::collections::BTreeMap;

use doris_segment::writer::Datum;
use doris_thrift::gen::status::TStatusCode;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::value::ColType;
use crate::ExecError;

/// The functions this module computes.
pub const NAMES: &[&str] = &[
    "percentile",
    "percentile_array",
    "retention",
    "linear_histogram",
    "exponential_moving_average",
    "group_bit_and",
    "group_bit_or",
    "group_bit_xor",
];

/// Those without researched window semantics: refused over a window.
pub const NOT_OVER_WINDOWS: &[&str] = &["retention", "linear_histogram", "group_bit_and", "group_bit_or", "group_bit_xor"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOp {
    And,
    Or,
    Xor,
}

/// One function's running state.
#[derive(Debug, Clone, PartialEq)]
pub enum Stats {
    /// `PercentileExactState`: the first row's levels and every accepted value
    Percentile {
        levels: Option<Vec<f64>>,
        values: Vec<f64>,
        array: bool,
    },
    /// `retention`: event `i` seen (bit i), of `n` conditions
    Retention { events: u32, n: usize },
    /// `linear_histogram`: the count per bucket key; the interval and offset last added
    Histogram {
        buckets: BTreeMap<i32, u64>,
        interval: f64,
        offset: f64,
    },
    /// `exponential_moving_average`; a fresh state is a point (0, 0)
    Ema { value: f64, time: f64, half_decay: f64 },
    /// `group_bit_*` in two's complement
    Bit { op: BitOp, acc: i128 },
}

fn invalid(msg: String) -> ExecError {
    ExecError::Coded {
        code: TStatusCode::INVALID_ARGUMENT.0,
        name: "INVALID_ARGUMENT",
        msg,
    }
}

/// A number as the state keeps it: f32 and integers to f64, as C++ converts them on output; a
/// BOOLEAN as 0 or 1.
fn number(d: &Datum) -> Option<f64> {
    match d {
        Datum::Double(v) => Some(*v),
        Datum::Float(v) => Some(f64::from(*v)),
        Datum::Int(v) => Some(*v as f64),
        Datum::Bool(b) => Some(f64::from(u8::from(*b))),
        _ => None,
    }
}

/// `check_quantile` (percentile_util.h:37-43). Not `!(0.0..=1.0).contains(&q)`, which refuses NaN.
#[allow(clippy::manual_range_contains)]
fn check_quantile(q: f64) -> Result<f64, ExecError> {
    // `q < 0 || q > 1`: NaN passes
    if q < 0.0 || q > 1.0 {
        return Err(invalid(format!("quantile in func percentile should in [0, 1], but real data is:{q:.6}")));
    }
    Ok(q)
}

/// The order statistic at level `q` of `sorted` (`_get_result`, percentile.h:595-614).
fn at_level(sorted: &[f64], q: f64) -> f64 {
    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }
    let u = (n - 1) as f64 * q;
    let i = u as usize;
    if i + 1 >= n {
        return sorted[n - 1];
    }
    sorted[i] + (u - i as f64) * (sorted[i + 1] - sorted[i])
}

impl Stats {
    /// The state of `name` over arguments of `params`, or None if this module does not compute it.
    pub fn new(name: &str, params: &[ColType]) -> Option<Stats> {
        let int = |t: &ColType| matches!(t.ptype, T::TINYINT | T::SMALLINT | T::INT | T::BIGINT | T::LARGEINT);
        let double = |t: &ColType| t.ptype == T::DOUBLE;
        Some(match name {
            "percentile" | "percentile_array"
                if params.len() == 2
                    && (int(&params[0]) || matches!(params[0].ptype, T::FLOAT | T::DOUBLE))
                    && (double(&params[1]) || params[1].element().is_some_and(|e| e.ptype == T::DOUBLE)) =>
            {
                Stats::Percentile {
                    levels: None,
                    values: Vec::new(),
                    array: name == "percentile_array",
                }
            }
            "retention" if (1..=32).contains(&params.len()) && params.iter().all(|t| t.ptype == T::BOOLEAN) => {
                Stats::Retention { events: 0, n: params.len() }
            }
            "linear_histogram"
                if (2..=3).contains(&params.len())
                    && (int(&params[0]) || matches!(params[0].ptype, T::BOOLEAN | T::FLOAT | T::DOUBLE))
                    && params[1..].iter().all(double) =>
            {
                Stats::Histogram {
                    buckets: BTreeMap::new(),
                    interval: 0.0,
                    offset: 0.0,
                }
            }
            "exponential_moving_average" if params.len() == 3 && params.iter().all(double) => Stats::Ema {
                value: 0.0,
                time: 0.0,
                half_decay: 0.0,
            },
            "group_bit_and" | "group_bit_or" | "group_bit_xor" if params.len() == 1 && int(&params[0]) => {
                let op = match name {
                    "group_bit_and" => BitOp::And,
                    "group_bit_or" => BitOp::Or,
                    _ => BitOp::Xor,
                };
                Stats::Bit {
                    op,
                    acc: if op == BitOp::And { -1 } else { 0 },
                }
            }
            _ => return None,
        })
    }

    /// Whether the function's result is NULL with no accepted row (`NullableAggregateFunction`),
    /// or its own empty value (`NotNullableAggregateFunction`, helpers.h:104-135).
    fn null_when_empty(&self) -> bool {
        !matches!(self, Stats::Percentile { array: true, .. } | Stats::Histogram { .. })
    }

    /// Adds one row; a row with a NULL argument is skipped. Returns whether it was accepted.
    pub fn add(&mut self, args: &[&Datum]) -> Result<bool, ExecError> {
        if args.iter().any(|d| matches!(d, Datum::Null)) {
            return Ok(false);
        }
        let arg = |i: usize| args.get(i).and_then(|d| number(d));
        match self {
            Stats::Percentile { levels, values, array } => {
                let (Some(x), Some(level)) = (arg(0), args.get(1)) else {
                    return Ok(false);
                };
                if levels.is_none() {
                    let qs = match (level, *array) {
                        (Datum::Array(qs), true) => qs
                            .iter()
                            .map(|q| match q {
                                Datum::Null => Err(invalid("quantiles in func percentile_array should not have null".into())),
                                q => check_quantile(number(q).unwrap_or(0.0)),
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                        (q, false) => vec![check_quantile(number(q).unwrap_or(0.0))?],
                        _ => return Err(ExecError::BadRequest("a percentile level of the wrong shape".into())),
                    };
                    *levels = Some(qs);
                }
                values.push(x);
            }
            Stats::Retention { events, .. } => {
                for (i, d) in args.iter().enumerate() {
                    if matches!(d, Datum::Bool(true)) {
                        *events |= 1 << i;
                    }
                }
            }
            Stats::Histogram { buckets, interval, offset } => {
                let val = arg(0).unwrap_or(0.0);
                let iv = arg(1).unwrap_or(0.0);
                let off = arg(2);
                // the row's index in its block: the check fails on a block's first row whenever
                // the interval and offset are the constants the FE sends
                if iv <= 0.0 {
                    return Err(invalid(format!(
                        "Invalid interval {}, row_num 0, interval should be larger than 0",
                        crate::row_text::double_text(iv)
                    )));
                }
                if let Some(o) = off.filter(|o| *o < 0.0 || *o >= iv) {
                    return Err(invalid(format!(
                        "Invalid offset {}, row_num 0, offset should be in [0, interval)",
                        crate::row_text::double_text(o)
                    )));
                }
                let o = off.unwrap_or(0.0);
                let key = ((val - o) / iv).floor();
                if key <= f64::from(i32::MIN) || key >= f64::from(i32::MAX) {
                    return Err(invalid(format!("{} exceeds the bucket range limit", crate::row_text::double_text(val))));
                }
                *buckets.entry(key as i32).or_insert(0) += 1;
                *interval = iv;
                *offset = o;
            }
            Stats::Ema { value, time, half_decay } => {
                let (Some(hd), Some(v), Some(t)) = (arg(0), arg(1), arg(2)) else {
                    return Ok(false);
                };
                *half_decay = hd;
                ema_point(value, time, v, t, hd);
            }
            Stats::Bit { op, acc } => {
                let Some(Datum::Int(v)) = args.first() else {
                    return Ok(false);
                };
                bit(*op, acc, *v);
            }
        }
        Ok(true)
    }

    pub fn merge(&mut self, other: &Stats) {
        match (self, other) {
            (Stats::Percentile { levels, values, .. }, Stats::Percentile { levels: rl, values: rv, .. }) => {
                if levels.is_none() {
                    levels.clone_from(rl);
                }
                values.extend_from_slice(rv);
            }
            (Stats::Retention { events, .. }, Stats::Retention { events: r, .. }) => *events |= r,
            // an empty side (interval 0) adds nothing (linear_histogram.h:87-98)
            (
                Stats::Histogram { buckets, interval, offset },
                Stats::Histogram {
                    buckets: rb,
                    interval: ri,
                    offset: ro,
                },
            ) => {
                if *ri != 0.0 {
                    *interval = *ri;
                    *offset = *ro;
                    for (k, c) in rb {
                        *buckets.entry(*k).or_insert(0) += c;
                    }
                }
            }
            (
                Stats::Ema { value, time, half_decay },
                Stats::Ema {
                    value: rv,
                    time: rt,
                    half_decay: rh,
                },
            ) => {
                let hd = if *half_decay != 0.0 { *half_decay } else { *rh };
                if hd != 0.0 {
                    *half_decay = hd;
                    ema_point(value, time, *rv, *rt, hd);
                }
            }
            (Stats::Bit { op, acc }, Stats::Bit { acc: r, .. }) => bit(*op, acc, *r),
            _ => {}
        }
    }

    /// The result; `accepted` is whether any row was.
    pub fn finish(&self, accepted: bool, _ret: &ColType) -> Datum {
        if !accepted && self.null_when_empty() {
            return Datum::Null;
        }
        match self {
            Stats::Percentile { levels, values, array } => {
                let mut sorted = values.clone();
                sorted.sort_by(f64::total_cmp);
                let qs = levels.as_deref().unwrap_or_default();
                if *array {
                    if sorted.is_empty() {
                        return Datum::Array(vec![]);
                    }
                    Datum::Array(qs.iter().map(|q| Datum::Double(at_level(&sorted, *q))).collect())
                } else {
                    match (sorted.is_empty(), qs.first()) {
                        (false, Some(q)) => Datum::Double(at_level(&sorted, *q)),
                        _ => Datum::Null,
                    }
                }
            }
            // out[0] = e0; out[i] = e0 && ei
            Stats::Retention { events, n } => {
                let first = events & 1 == 1;
                Datum::Array((0..*n).map(|i| Datum::Bool(first && (i == 0 || events >> i & 1 == 1))).collect())
            }
            Stats::Histogram { buckets, interval, offset } => Datum::Bytes(histogram_json(buckets, *interval, *offset).into_bytes()),
            // `get`: value / sum_weights(half_decay); a half decay of 0 is 0
            Stats::Ema { value, half_decay, .. } => Datum::Double(if *half_decay == 0.0 {
                0.0
            } else {
                value / (1.0 / (1.0 - core_math::exp2(-1.0 / half_decay)))
            }),
            Stats::Bit { acc, .. } => Datum::Int(*acc),
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Stats::Percentile { levels, values, array } => {
                out.push(0);
                out.push(u8::from(*array));
                match levels {
                    None => out.push(0),
                    Some(qs) => {
                        out.push(1);
                        put_f64s(out, qs);
                    }
                }
                put_f64s(out, values);
            }
            Stats::Retention { events, n } => {
                out.push(1);
                out.extend(events.to_le_bytes());
                out.extend((*n as u64).to_le_bytes());
            }
            Stats::Histogram { buckets, interval, offset } => {
                out.push(2);
                out.extend(interval.to_bits().to_le_bytes());
                out.extend(offset.to_bits().to_le_bytes());
                out.extend((buckets.len() as u64).to_le_bytes());
                for (k, c) in buckets {
                    out.extend(k.to_le_bytes());
                    out.extend(c.to_le_bytes());
                }
            }
            Stats::Ema { value, time, half_decay } => {
                out.push(3);
                put_f64s(out, &[*value, *time, *half_decay]);
            }
            Stats::Bit { op, acc } => {
                out.push(4);
                out.push(*op as u8);
                out.extend(acc.to_le_bytes());
            }
        }
    }

    /// A state `encode` wrote, and the bytes it took.
    pub fn decode(b: &[u8]) -> Option<(Stats, usize)> {
        let mut r = Reader { b, at: 0 };
        let s = match r.u8()? {
            0 => {
                let array = r.u8()? == 1;
                let levels = if r.u8()? == 1 { Some(r.f64s()?) } else { None };
                Stats::Percentile {
                    levels,
                    values: r.f64s()?,
                    array,
                }
            }
            1 => {
                let events = u32::from_le_bytes(r.take(4)?.try_into().ok()?);
                Stats::Retention {
                    events,
                    n: r.u64()? as usize,
                }
            }
            2 => {
                let interval = f64::from_bits(r.u64()?);
                let offset = f64::from_bits(r.u64()?);
                let n = r.u64()? as usize;
                let mut buckets = BTreeMap::new();
                for _ in 0..n {
                    let k = i32::from_le_bytes(r.take(4)?.try_into().ok()?);
                    buckets.insert(k, r.u64()?);
                }
                Stats::Histogram { buckets, interval, offset }
            }
            3 => {
                let v = r.f64s()?;
                Stats::Ema {
                    value: *v.first()?,
                    time: *v.get(1)?,
                    half_decay: *v.get(2)?,
                }
            }
            4 => {
                let op = match r.u8()? {
                    0 => BitOp::And,
                    1 => BitOp::Or,
                    _ => BitOp::Xor,
                };
                Stats::Bit {
                    op,
                    acc: i128::from_le_bytes(r.take(16)?.try_into().ok()?),
                }
            }
            _ => return None,
        };
        Some((s, r.at))
    }
}

fn bit(op: BitOp, acc: &mut i128, v: i128) {
    match op {
        BitOp::And => *acc &= v,
        BitOp::Or => *acc |= v,
        BitOp::Xor => *acc ^= v,
    }
}

/// `merge_point` (aggregate_function_ema.h): a point `(v, t)` into the running `(value, time)`.
fn ema_point(value: &mut f64, time: &mut f64, v: f64, t: f64, hd: f64) {
    let scale = |dt: f64| core_math::exp2(-dt / hd);
    if *time > t {
        *value += v * scale(*time - t);
    } else if *time < t {
        *value = v + *value * scale(t - *time);
        *time = t;
    } else {
        *value += v;
    }
}

/// The histogram as rapidjson's compact `Writer` writes it (linear_histogram.h:131-177): every key
/// from the least to the greatest, bounds accumulated from the first.
fn histogram_json(buckets: &BTreeMap<i32, u64>, interval: f64, offset: f64) -> String {
    let (Some(lo), Some(hi)) = (buckets.keys().next(), buckets.keys().next_back()) else {
        return r#"{"num_buckets":0,"buckets":[]}"#.into();
    };
    let mut out = format!(r#"{{"num_buckets":{},"buckets":["#, (i64::from(*hi) - i64::from(*lo) + 1) as u32);
    let mut left = f64::from(*lo) * interval + offset;
    let mut acc = 0u64;
    for k in *lo..=*hi {
        let count = buckets.get(&k).copied().unwrap_or(0);
        acc += count;
        let lower = left;
        left += interval;
        if k != *lo {
            out.push(',');
        }
        out += &format!(
            r#"{{"lower":{},"upper":{},"count":{count},"acc_count":{acc}}}"#,
            json_double(lower),
            json_double(left)
        );
    }
    out + "]}"
}

/// A double as rapidjson writes it (`internal::dtoa`, Grisu2 + `Prettify`): always a fraction or
/// an exponent. The shortest round-trip digits stand in for Grisu2's.
pub fn json_double(x: f64) -> String {
    if x == 0.0 {
        return if x.is_sign_negative() { "-0.0".into() } else { "0.0".into() };
    }
    let sci = format!("{:e}", x.abs());
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let kk = exp.parse::<i32>().unwrap_or(0) + 1;
    let len = digits.len() as i32;
    let k = kk - len;
    let sign = if x < 0.0 { "-" } else { "" };
    let body = if k >= 0 && kk <= 21 {
        format!("{digits}{}.0", "0".repeat(k as usize))
    } else if 0 < kk && kk <= 21 {
        format!("{}.{}", &digits[..kk as usize], &digits[kk as usize..])
    } else if -6 < kk && kk <= 0 {
        format!("0.{}{digits}", "0".repeat((-kk) as usize))
    } else if len == 1 {
        format!("{digits}e{}", kk - 1)
    } else {
        format!("{}.{}e{}", &digits[..1], &digits[1..], kk - 1)
    };
    format!("{sign}{body}")
}

fn put_f64s(out: &mut Vec<u8>, v: &[f64]) {
    out.extend((v.len() as u64).to_le_bytes());
    for x in v {
        out.extend(x.to_bits().to_le_bytes());
    }
}

struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let s = self.b.get(self.at..self.at + n)?;
        self.at += n;
        Some(s)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    fn u64(&mut self) -> Option<u64> {
        self.take(8).map(|b| u64::from_le_bytes(b.try_into().unwrap_or([0; 8])))
    }

    fn f64s(&mut self) -> Option<Vec<f64>> {
        let n = self.u64()? as usize;
        (0..n).map(|_| self.u64().map(f64::from_bits)).collect()
    }
}
