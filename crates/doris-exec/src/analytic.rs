//! ANALYTIC_EVAL_NODE over rows already sorted by partition keys, then order keys, as the C++
//! BE's analytic operators (`analytic_sink_operator.cpp`, `aggregate_function_window.h`)
//! compute it:
//! - a partition is a run of equal partition keys and a peer group a run of equal order keys,
//!   where NULL equals NULL and floats compare by value (-0.0 = 0.0, NaN = NaN);
//! - each row's frame is clamped to its partition, and an aggregate adds the frame's rows in row
//!   order, so float sums agree bit for bit;
//! - an empty frame gives NULL, but 0 for `count`.
//!
//! The FE forces the rank family's and lead/lag's frames and never sends RANGE with offsets
//! (`WindowFunctionChecker`); `check_plan` refuses both, so this module need not guess.
use std::cmp::Ordering;

use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::{TExpr, TExprNodeType as N};
use doris_thrift::gen::plan_nodes::{TAnalyticWindow, TAnalyticWindowBoundary, TAnalyticWindowBoundaryType as B, TAnalyticWindowType as W};

use crate::aggregate::{subtree_end, Accumulator, AggSpec};
use crate::eval::{check_read, compare, AGGREGATES};
use crate::value::ColType;
use crate::ExecError;

/// A row's frame, by kind. Offsets count rows from the current one: PRECEDING n is -n.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Frame {
    /// The whole partition.
    Partition,
    /// RANGE from the partition's start to the end of the current peer group.
    RangeToCurrent,
    /// ROWS; a missing bound is the partition's start or end.
    Rows { start: Option<i64>, end: Option<i64> },
}

/// The frame of a node's window; no window, or no bounds, is the whole partition.
pub fn frame(w: Option<&TAnalyticWindow>) -> Result<Frame, ExecError> {
    let Some(w) = w else { return Ok(Frame::Partition) };
    match (w.type_, &w.window_start, &w.window_end) {
        (_, None, None) => Ok(Frame::Partition),
        (W::RANGE, None, Some(e)) if e.type_ == B::CURRENT_ROW => Ok(Frame::RangeToCurrent),
        (W::RANGE, _, _) => Err(ExecError::Unsupported {
            what: "a RANGE window with offsets".into(),
        }),
        (W::ROWS, s, e) => Ok(Frame::Rows {
            start: s.as_ref().map(offset).transpose()?,
            end: e.as_ref().map(offset).transpose()?,
        }),
        (other, _, _) => Err(ExecError::BadRequest(format!("a window of type {}", other.0))),
    }
}

fn offset(b: &TAnalyticWindowBoundary) -> Result<i64, ExecError> {
    let n = || {
        b.rows_offset_value
            .ok_or_else(|| ExecError::BadRequest("a ROWS bound without its offset".into()))
    };
    match b.type_ {
        B::CURRENT_ROW => Ok(0),
        B::PRECEDING => Ok(-n()?),
        B::FOLLOWING => n(),
        other => Err(ExecError::BadRequest(format!("a window bound of type {}", other.0))),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowKind {
    RowNumber,
    Rank,
    DenseRank,
    /// `lead(x, offset, default)`.
    Lead(i64),
    /// `lag(x, offset, default)`.
    Lag(i64),
    First {
        ignore_nulls: bool,
    },
    Last {
        ignore_nulls: bool,
    },
    /// `ntile(n)`: the frame is ignored, the bucket comes from the partition's size.
    Ntile(i64),
    /// `percent_rank()`: the peer group's rank, from 0 to 1 (aggregate_function_window.h).
    PercentRank,
    /// `cume_dist()`: the share of the partition up to the end of the peer group.
    CumeDist,
    /// An aggregate over the frame.
    Agg(AggSpec),
}


/// One function of the node.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFn {
    pub kind: WindowKind,
    pub ret: ColType,
    /// The argument trees, evaluated over the node's input.
    pub args: Vec<TExpr>,
}

impl WindowFn {
    /// An AGG_EXPR tree: the call, then its argument trees.
    pub fn from_expr(e: &TExpr) -> Result<WindowFn, ExecError> {
        let root = e.nodes.first().ok_or_else(|| ExecError::BadRequest("an empty window function".into()))?;
        if root.node_type != N::AGG_EXPR {
            return Err(ExecError::BadRequest("a window function that is not an AGG_EXPR".into()));
        }
        let name = root
            .fn_
            .as_ref()
            .ok_or_else(|| ExecError::BadRequest("an AGG_EXPR without its function".into()))?
            .name
            .function_name
            .to_ascii_lowercase();
        let ret = ColType::from_type_desc("", &root.type_, root.is_nullable.unwrap_or(true)).ok_or_else(|| ExecError::Unsupported {
            what: format!("{name} of a nested type"),
        })?;
        let mut args = Vec::new();
        let mut at = 1;
        for _ in 0..root.num_children.max(0) {
            let end = subtree_end(&e.nodes, at)?;
            args.push(TExpr::new(e.nodes[at..end].to_vec()));
            at = end;
        }
        for a in &args {
            check_read(a)?;
        }
        let kind = match name.as_str() {
            "row_number" => WindowKind::RowNumber,
            "ntile" => WindowKind::Ntile(args.first().and_then(int_literal).filter(|n| *n > 0).ok_or_else(|| ExecError::Unsupported {
                what: "an ntile bucket count that is not a positive literal".into(),
            })?),
            "rank" => WindowKind::Rank,
            "percent_rank" => WindowKind::PercentRank,
            "cume_dist" => WindowKind::CumeDist,
            "dense_rank" => WindowKind::DenseRank,
            "lead" | "lag" => {
                let off = args
                    .get(1)
                    .and_then(int_literal)
                    .filter(|n| *n >= 0)
                    .ok_or_else(|| ExecError::Unsupported {
                        what: format!("a {name} offset that is not a non-negative literal"),
                    })?;
                if name == "lead" {
                    WindowKind::Lead(off)
                } else {
                    WindowKind::Lag(off)
                }
            }
            "first_value" | "last_value" => {
                // C++ picks the ignore-nulls variant whenever there are 2 arguments, whatever the
                // flag (aggregate_function_window.cpp); the FE rewrites (x, false) to (x)
                let ignore_nulls = args.len() >= 2;
                if name == "first_value" {
                    WindowKind::First { ignore_nulls }
                } else {
                    WindowKind::Last { ignore_nulls }
                }
            }
            // the M6 s7 aggregates' window semantics were not researched
            f @ ("sum0" | "group_concat" | "ndv" | "window_funnel_v2") => {
                return Err(ExecError::Unsupported {
                    what: format!("{f} over a window"),
                })
            }
            f if crate::moments::arity(f).is_some() || crate::stats_agg::NOT_OVER_WINDOWS.contains(&f) => {
                return Err(ExecError::Unsupported {
                    what: format!("{f} over a window"),
                })
            }
            f if AGGREGATES.contains(&f) => {
                let spec = AggSpec::from_expr(e)?;
                if spec.merge {
                    return Err(ExecError::BadRequest(format!("a merging {f} in a window")));
                }
                // its argument types are checked now, not at the first row
                crate::aggregate::Accumulator::new(&spec)?;
                WindowKind::Agg(spec)
            }
            other => {
                return Err(ExecError::Unsupported {
                    what: format!("function {other}"),
                })
            }
        };
        Ok(WindowFn { kind, ret, args })
    }

    /// The function's name, for messages.
    pub fn name(&self) -> &str {
        match &self.kind {
            WindowKind::RowNumber => "row_number",
            WindowKind::Rank => "rank",
            WindowKind::DenseRank => "dense_rank",
            WindowKind::Lead(_) => "lead",
            WindowKind::Lag(_) => "lag",
            WindowKind::First { .. } => "first_value",
            WindowKind::Last { .. } => "last_value",
            WindowKind::Ntile(_) => "ntile",
            WindowKind::PercentRank => "percent_rank",
            WindowKind::CumeDist => "cume_dist",
            WindowKind::Agg(a) => &a.function,
        }
    }
}

/// The FE forces each rank-family function's frame (`WindowFunctionChecker`): `row_number`
/// over ROWS from the partition's start to the current row, `rank` and `dense_rank` over RANGE
/// to the current row. Any other is refused rather than guessed.
/// `check_frame` for a node's function: an aggregate over RANGE to the current row with no
/// ORDER BY (the FE's `OVER ()`) reads the whole partition, every row being a peer.
pub fn check_window(f: &WindowFn, frame: &Frame, ordered: bool) -> Result<(), ExecError> {
    match (&f.kind, frame) {
        (WindowKind::Agg(_), Frame::RangeToCurrent) if !ordered => check_frame(f, &Frame::Partition),
        _ => check_frame(f, frame),
    }
}

pub fn check_frame(f: &WindowFn, frame: &Frame) -> Result<(), ExecError> {
    let ok = match f.kind {
        WindowKind::RowNumber => *frame == Frame::Rows { start: None, end: Some(0) },
        WindowKind::Rank | WindowKind::DenseRank | WindowKind::PercentRank | WindowKind::CumeDist => *frame == Frame::RangeToCurrent,
        // lead and lag read their offset's row, which is the end of the frame the FE forces
        WindowKind::Lead(off) | WindowKind::Lag(off) => {
            let end = if matches!(f.kind, WindowKind::Lead(_)) { off } else { -off };
            if *frame != (Frame::Rows { start: None, end: Some(end) }) {
                return Err(ExecError::Unsupported {
                    what: format!("{} over a window other than ROWS to its offset", f.name()),
                });
            }
            true
        }
        // the distinct combinator clears only its set when it outputs, so in a frame C++ does not
        // reset (a cumulative one) each output re-adds the values its step brought: refused there
        // (multi_distinct_count is a plain set, right in every frame)
        WindowKind::Agg(ref a) if matches!(a.function.as_str(), "multi_distinct_sum" | "multi_distinct_group_concat") => {
            matches!(frame, Frame::Partition | Frame::Rows { start: Some(_), .. })
        }
        _ => true,
    };
    if ok {
        return Ok(());
    }
    let kind = match frame {
        Frame::Partition => "whole-partition",
        Frame::RangeToCurrent => "RANGE",
        Frame::Rows { .. } => "ROWS",
    };
    Err(ExecError::Unsupported {
        what: format!("{} over a {kind} window", f.name()),
    })
}

fn int_literal(e: &TExpr) -> Option<i64> {
    match e.nodes.as_slice() {
        [n] if n.node_type == N::INT_LITERAL => n.int_literal.as_ref().map(|l| l.value),
        _ => None,
    }
}

/// Two key values are equal as the C++ BE's `compare_at` finds them.
fn same(a: &Datum, b: &Datum, t: &ColType) -> bool {
    match (a == &Datum::Null, b == &Datum::Null) {
        (true, true) => true,
        (false, false) => compare(a, t, b, t) == Some(Ordering::Equal),
        _ => false,
    }
}

fn same_row(a: &[Datum], b: &[Datum], types: &[ColType]) -> bool {
    a.iter().zip(b).zip(types).all(|((a, b), t)| same(a, b, t))
}

/// One function over a node's input: its frame, its argument columns, and the input's block size
/// (`batch_size`, which `ntile` needs).
#[derive(Clone, Copy)]
pub struct Window<'a> {
    pub frame: &'a Frame,
    pub f: &'a WindowFn,
    pub args: &'a [(ColType, Vec<Datum>)],
    pub batch_size: usize,
}

/// One function's value for each row. `part` and `order` hold each row's partition and order
/// key values (rows sorted by them).
pub fn window_columns(
    part: &[Vec<Datum>],
    part_types: &[ColType],
    order: &[Vec<Datum>],
    order_types: &[ColType],
    w: &Window,
) -> Result<Vec<Datum>, ExecError> {
    let n = part.len();
    let mut out = Vec::with_capacity(n);
    let mut ps = 0;
    while ps < n {
        let mut pe = ps + 1;
        while pe < n && same_row(&part[ps], &part[pe], part_types) {
            pe += 1;
        }
        // each row's peer group, as its start, its end and its number in the partition
        let mut groups = Vec::with_capacity(pe - ps);
        let (mut gs, mut idx) = (ps, 0);
        while gs < pe {
            let mut ge = gs + 1;
            while ge < pe && same_row(&order[gs], &order[ge], order_types) {
                ge += 1;
            }
            groups.extend(std::iter::repeat_n((gs, ge, idx), ge - gs));
            gs = ge;
            idx += 1;
        }
        partition(&mut out, ps, pe, &groups, w)?;
        ps = pe;
    }
    Ok(out)
}

/// The values of one partition `[ps, pe)`.
fn partition(
    out: &mut Vec<Datum>,
    ps: usize,
    pe: usize,
    groups: &[(usize, usize, usize)],
    w: &Window,
) -> Result<(), ExecError> {
    let Window { frame, f, args, batch_size } = *w;
    let col = |i: usize| args.get(i).map(|(_, v)| v.as_slice());
    let x = || col(0).ok_or_else(|| ExecError::BadRequest(format!("{} without its argument", f.name())));
    let default = |cur: usize| col(2).map_or(Datum::Null, |d| d[cur].clone());
    // an aggregate's state over [acc_start, acc_end), extended while the frame only grows
    let mut acc: Option<(Accumulator, usize, usize)> = None;
    for cur in ps..pe {
        let (gs, ge, gi) = groups[cur - ps];
        let (fs, fe) = bounds(frame, cur, ps, pe, ge);
        let v = match &f.kind {
            WindowKind::RowNumber => Datum::Int((cur - ps + 1) as i128),
            WindowKind::Rank => Datum::Int((gs - ps + 1) as i128),
            WindowKind::DenseRank => Datum::Int((gi + 1) as i128),
            // aggregate_function_window.h:203-334, over the peer group [gs, ge)
            WindowKind::PercentRank => {
                let n = pe - ps;
                Datum::Double(if n <= 1 { 0.0 } else { (gs - ps) as f64 / (n - 1) as f64 })
            }
            WindowKind::CumeDist => Datum::Double((ge - ps) as f64 / (pe - ps) as f64),
            // WindowFunctionNTile::add_range_single_place
            WindowKind::Ntile(n) => {
                // C++ sizes a partition from the rows received so far: past the first input block
                // (batch_size rows) it numbers buckets from a partial size
                if pe > batch_size {
                    return Err(ExecError::Unsupported { what: format!("ntile over a partition past the first {batch_size} rows") });
                }
                let (row, size, n) = ((cur - ps) as i64, (pe - ps) as i64, *n);
                let (small, big) = (size / n, size % n);
                let first_small = big * (small + 1);
                let bucket = if row >= first_small { big + 1 + (row - first_small) / small } else { row / (small + 1) + 1 };
                Datum::Int(bucket.into())
            }
            WindowKind::Lead(off) => match cur.checked_add(*off as usize).filter(|t| *t < pe) {
                Some(t) => x()?[t].clone(),
                None => default(cur),
            },
            WindowKind::Lag(off) => match cur.checked_sub(*off as usize).filter(|t| *t >= ps) {
                Some(t) => x()?[t].clone(),
                None => default(cur),
            },
            WindowKind::First { ignore_nulls } => {
                let x = x()?;
                let mut rows = fs..fe;
                let at = if *ignore_nulls {
                    rows.find(|r| x[*r] != Datum::Null)
                } else {
                    rows.next()
                };
                at.map_or(Datum::Null, |r| x[r].clone())
            }
            WindowKind::Last { ignore_nulls } => {
                let x = x()?;
                let mut rows = (fs..fe).rev();
                let at = if *ignore_nulls {
                    rows.find(|r| x[*r] != Datum::Null)
                } else {
                    rows.next()
                };
                at.map_or(Datum::Null, |r| x[r].clone())
            }
            WindowKind::Agg(spec) => {
                let fresh = !matches!(&acc, Some((_, s, e)) if *s == fs && *e <= fe);
                if fresh {
                    acc = Some((Accumulator::new(spec)?, fs, fs));
                }
                let (a, _, end) = acc.as_mut().ok_or_else(|| ExecError::Internal("no window state".into()))?;
                for r in *end..fe {
                    let row: Vec<(&ColType, &Datum)> = args.iter().map(|(t, v)| (t, &v[r])).collect();
                    a.update(&row)?;
                }
                *end = (*end).max(fe);
                a.finish()?
            }
        };
        out.push(v);
    }
    Ok(())
}

/// A row's frame `[start, end)`, clamped to its partition; empty when start >= end.
fn bounds(frame: &Frame, cur: usize, ps: usize, pe: usize, peer_end: usize) -> (usize, usize) {
    match frame {
        Frame::Partition => (ps, pe),
        Frame::RangeToCurrent => (ps, peer_end),
        Frame::Rows { start, end } => {
            let at = |o: i64| (cur as i64).saturating_add(o);
            let s = start.map_or(ps as i64, at).max(ps as i64);
            let e = end.map_or(pe as i64, |o| at(o).saturating_add(1)).min(pe as i64);
            let s = s.min(pe as i64) as usize;
            (s, (e.max(s as i64)) as usize)
        }
    }
}

/// Whether two neighbouring rows have partition keys equal in value but not in bits (-0.0 and
/// 0.0, or two NaNs). C++ hashes partition keys by their bytes, so in several instances such
/// rows can reach different instances and be different partitions; doris-rs's exchange sends
/// them to one, so it can't answer as C++ does.
pub fn split_float_keys(part: &[Vec<Datum>], types: &[ColType]) -> bool {
    part.windows(2).any(|w| {
        w[0].iter().zip(&w[1]).zip(types).any(|((a, b), t)| match (a, b) {
            (Datum::Float(x), Datum::Float(y)) => x.to_bits() != y.to_bits() && same(a, b, t),
            (Datum::Double(x), Datum::Double(y)) => x.to_bits() != y.to_bits() && same(a, b, t),
            _ => false,
        })
    })
}
