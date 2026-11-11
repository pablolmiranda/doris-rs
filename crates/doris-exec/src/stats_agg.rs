//! Batch 3's aggregates, as the C++ BE computes them (`be/src/exprs/aggregate/`):
//! `percentile`/`percentile_array` (the `new_version_percentile` classes,
//! `aggregate_function_percentile.h:434-619`). See
//! `docs/superpowers/research/2026-11-10-m6-s7b3-aggregates.md`. A partial state is doris-rs's
//! own encoding.
use doris_segment::writer::Datum;
use doris_thrift::gen::status::TStatusCode;

use crate::value::ColType;
use crate::ExecError;

/// The functions this module computes.
pub const NAMES: &[&str] = &["percentile", "percentile_array"];

/// One function's running state.
#[derive(Debug, Clone, PartialEq)]
pub enum Stats {
    /// `PercentileExactState`: the first row's levels and every accepted value
    Percentile {
        levels: Option<Vec<f64>>,
        values: Vec<f64>,
        array: bool,
    },
}

fn invalid(msg: String) -> ExecError {
    ExecError::Coded {
        code: TStatusCode::INVALID_ARGUMENT.0,
        name: "INVALID_ARGUMENT",
        msg,
    }
}

/// A number as the state keeps it: f32 and integers to f64, as C++ converts them on output.
fn number(d: &Datum) -> Option<f64> {
    match d {
        Datum::Double(v) => Some(*v),
        Datum::Float(v) => Some(f64::from(*v)),
        Datum::Int(v) => Some(*v as f64),
        _ => None,
    }
}

/// `check_quantile` (percentile_util.h:37-43).
fn check_quantile(q: f64) -> Result<f64, ExecError> {
    if !(0.0..=1.0).contains(&q) {
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
    pub fn new(name: &str, _params: &[ColType]) -> Option<Stats> {
        Some(match name {
            "percentile" | "percentile_array" => Stats::Percentile {
                levels: None,
                values: Vec::new(),
                array: name == "percentile_array",
            },
            _ => return None,
        })
    }

    /// Whether the function's result is NULL with no accepted row (`NullableAggregateFunction`),
    /// or its own empty value (`NotNullableAggregateFunction`, helpers.h:104-135).
    fn null_when_empty(&self) -> bool {
        !matches!(self, Stats::Percentile { array: true, .. })
    }

    /// Adds one row; a row with a NULL argument is skipped. Returns whether it was accepted.
    pub fn add(&mut self, args: &[&Datum]) -> Result<bool, ExecError> {
        if args.iter().any(|d| matches!(d, Datum::Null)) {
            return Ok(false);
        }
        match self {
            Stats::Percentile { levels, values, array } => {
                let (Some(x), Some(level)) = (args.first().and_then(|d| number(d)), args.get(1)) else {
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
            _ => return None,
        };
        Some((s, r.at))
    }
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
