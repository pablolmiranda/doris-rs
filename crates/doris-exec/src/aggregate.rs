//! AGGREGATION_NODE ([agg] A): one phase (update, finalize), the first of two (update,
//! serialize) and the second (merge, finalize). A partial state is doris-rs's own encoding in a
//! `Datum::Bytes`: it never leaves the BE ([agg] A.3). The merge phase takes a function's
//! argument types from `agg_expr.param_types`, never from its `VARCHAR(65533)` state slot, and
//! result types and nullability come from the FE.
use std::collections::{BTreeSet, HashMap};

use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::{TExpr, TExprNodeType as N};
use doris_thrift::gen::plan_nodes::TAggregationNode;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::block::Block;
use crate::eval::{check_read, compare, eval_column, AGGREGATES};
use crate::exchange::Chunk;
use crate::pipeline::slot_map;
use crate::value::ColType;
use crate::ExecError;

/// One aggregate call of the node.
#[derive(Debug, Clone, PartialEq)]
pub struct AggSpec {
    pub function: String,
    /// Merging partial states (the second phase).
    pub merge: bool,
    pub ret: ColType,
    /// The original argument types.
    pub param_types: Vec<ColType>,
    pub args: Vec<TExpr>,
}

impl AggSpec {
    /// An AGG_EXPR tree: the call, then its argument trees.
    pub fn from_expr(e: &TExpr) -> Result<AggSpec, ExecError> {
        let root = e
            .nodes
            .first()
            .ok_or_else(|| ExecError::BadRequest("an empty aggregate expression".into()))?;
        if root.node_type != N::AGG_EXPR {
            return Err(ExecError::BadRequest("an aggregate function that is not an AGG_EXPR".into()));
        }
        let f = root
            .fn_
            .as_ref()
            .ok_or_else(|| ExecError::BadRequest("an AGG_EXPR without its function".into()))?;
        let function = f.name.function_name.to_ascii_lowercase();
        if !AGGREGATES.contains(&function.as_str()) {
            return Err(ExecError::Unsupported {
                what: format!("function {function}"),
            });
        }
        let nullable = root.is_nullable.unwrap_or(true);
        let ret = ColType::from_type_desc("", &root.type_, nullable).ok_or_else(|| ExecError::Unsupported {
            what: format!("{function} of a nested type"),
        })?;
        let merge = root.agg_expr.as_ref().is_some_and(|a| a.is_merge_agg);
        let param_types = root
            .agg_expr
            .as_ref()
            .and_then(|a| a.param_types.clone())
            .unwrap_or_else(|| f.arg_types.clone())
            .iter()
            .map(|t| {
                ColType::from_type_desc("", t, true).ok_or_else(|| ExecError::Unsupported {
                    what: format!("{function} of a nested type"),
                })
            })
            .collect::<Result<_, _>>()?;
        // the argument trees follow the root, pre-order
        let mut args = Vec::new();
        let mut at = 1;
        for _ in 0..root.num_children.max(0) {
            let end = subtree_end(&e.nodes, at)?;
            args.push(TExpr::new(e.nodes[at..end].to_vec()));
            at = end;
        }
        if !merge {
            for a in &args {
                check_read(a)?;
            }
        }
        Ok(AggSpec {
            function,
            merge,
            ret,
            param_types,
            args,
        })
    }
}

pub(crate) fn subtree_end(nodes: &[doris_thrift::gen::exprs::TExprNode], at: usize) -> Result<usize, ExecError> {
    let n = nodes
        .get(at)
        .ok_or_else(|| ExecError::BadRequest("an aggregate's arguments end early".into()))?;
    let mut end = at + 1;
    for _ in 0..n.num_children.max(0) {
        end = subtree_end(nodes, end)?;
    }
    Ok(end)
}

/// One aggregate's running state, the AGGREGATION_NODE's own: the window node folds a frame's
/// rows through it, in row order.
#[derive(Debug)]
pub struct Accumulator {
    spec: AggSpec,
    state: State,
}

impl Accumulator {
    pub fn new(spec: &AggSpec) -> Result<Accumulator, ExecError> {
        Ok(Accumulator { spec: spec.clone(), state: State::new(spec)? })
    }

    /// Adds one row's arguments.
    pub fn update(&mut self, args: &[(&ColType, &Datum)]) -> Result<(), ExecError> {
        self.state.update(&self.spec, args)
    }

    /// The value over the rows added so far: NULL over none, but 0 for `count`.
    pub fn finish(&self) -> Result<Datum, ExecError> {
        self.state.finish(&self.spec)
    }
}

/// An aggregation over its input.
#[derive(Debug, Clone, PartialEq)]
pub struct Aggregation {
    pub group_by: Vec<TExpr>,
    pub functions: Vec<AggSpec>,
    /// Final values (true) or partial states (false).
    pub finalize: bool,
}

impl Aggregation {
    pub fn from_thrift(n: &TAggregationNode) -> Result<Aggregation, ExecError> {
        let group_by = n.grouping_exprs.clone().unwrap_or_default();
        for g in &group_by {
            check_read(g)?;
        }
        Ok(Aggregation {
            group_by,
            functions: n.aggregate_functions.iter().map(AggSpec::from_expr).collect::<Result<_, _>>()?,
            finalize: n.need_finalize,
        })
    }

    /// The output rows: grouping keys, then one column per function. A scalar aggregation
    /// finalizes to one row even over no input; its first phase sends nothing then
    /// (`_get_without_key_result`, `_serialize_without_key`).
    pub fn run(&self, input: &Chunk) -> Result<Block, ExecError> {
        let slots = slot_map(&input.slots);
        let mut key_types = Vec::new();
        let mut key_cols = Vec::new();
        for g in &self.group_by {
            let (t, v) = eval_column(g, &input.block, &slots)?;
            key_types.push(t);
            key_cols.push(v);
        }
        let mut arg_cols = Vec::new();
        for f in &self.functions {
            let mut cols = Vec::new();
            for a in &f.args {
                cols.push(eval_column(a, &input.block, &slots)?);
            }
            arg_cols.push(cols);
        }
        let mut groups: Vec<(Vec<Datum>, Vec<State>)> = Vec::new();
        let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
        let fresh = || self.functions.iter().map(State::new).collect::<Result<Vec<_>, _>>();
        if self.group_by.is_empty() && (self.finalize || !input.block.is_empty()) {
            groups.push((Vec::new(), fresh()?));
        }
        for r in 0..input.block.len() {
            let g = if self.group_by.is_empty() {
                0
            } else {
                let key: Vec<Datum> = key_cols.iter().map(|c| c[r].clone()).collect();
                let k = canonical(&key);
                match index.get(&k) {
                    Some(g) => *g,
                    None => {
                        groups.push((key, fresh()?));
                        index.insert(k, groups.len() - 1);
                        groups.len() - 1
                    }
                }
            };
            for (i, f) in self.functions.iter().enumerate() {
                let state = &mut groups[g].1[i];
                if f.merge {
                    let (_, v) = &arg_cols[i][0];
                    if let Datum::Bytes(b) = &v[r] {
                        state.merge(&State::decode(b)?)?;
                    }
                } else {
                    let args: Vec<(&ColType, &Datum)> = arg_cols[i].iter().map(|(t, v)| (t, &v[r])).collect();
                    state.update(f, &args)?;
                }
            }
        }
        let mut types = key_types;
        for f in &self.functions {
            types.push(if self.finalize {
                f.ret.clone()
            } else {
                ColType {
                    name: String::new(),
                    ptype: T::VARCHAR,
                    len: 65533,
                    precision: 0,
                    scale: 0,
                    nullable: false,
                }
            });
        }
        let rows = groups
            .into_iter()
            .map(|(mut key, states)| {
                for (s, f) in states.iter().zip(&self.functions) {
                    key.push(if self.finalize { s.finish(f)? } else { Datum::Bytes(s.encode()) });
                }
                Ok(key)
            })
            .collect::<Result<_, ExecError>>()?;
        Ok(Block { types, rows })
    }
}

/// A canonical form of values: equal values (as compared) have equal forms.
pub fn canonical(values: &[Datum]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in values {
        encode_datum(v, &mut out);
    }
    out
}

fn norm(f: f64) -> f64 {
    if f == 0.0 {
        0.0
    } else if f.is_nan() {
        f64::NAN
    } else {
        f
    }
}

fn encode_datum(v: &Datum, out: &mut Vec<u8>) {
    match v {
        Datum::Null => out.push(0),
        Datum::Bool(b) => out.extend([1, u8::from(*b)]),
        Datum::Int(i) => {
            out.push(2);
            out.extend(i.to_le_bytes());
        }
        Datum::Float(f) => {
            out.push(3);
            out.extend(norm(f64::from(*f)).to_bits().to_le_bytes());
        }
        Datum::Double(f) => {
            out.push(4);
            out.extend(norm(*f).to_bits().to_le_bytes());
        }
        Datum::Bytes(b) => {
            out.push(5);
            out.extend((b.len() as u64).to_le_bytes());
            out.extend(b);
        }
    }
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], ExecError> {
        if self.0.len() < n {
            return Err(ExecError::Internal("a partial aggregate state is cut short".into()));
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }

    fn u8(&mut self) -> Result<u8, ExecError> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64, ExecError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }

    fn i128(&mut self) -> Result<i128, ExecError> {
        Ok(i128::from_le_bytes(self.take(16)?.try_into().expect("16 bytes")))
    }

    fn datum(&mut self) -> Result<Datum, ExecError> {
        Ok(match self.u8()? {
            0 => Datum::Null,
            1 => Datum::Bool(self.u8()? != 0),
            2 => Datum::Int(self.i128()?),
            3 => Datum::Float(f64::from_bits(self.u64()?) as f32),
            4 => Datum::Double(f64::from_bits(self.u64()?)),
            5 => {
                let n = self.u64()? as usize;
                Datum::Bytes(self.take(n)?.to_vec())
            }
            t => return Err(ExecError::Internal(format!("a partial aggregate state has value tag {t}"))),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Num {
    Int(i128),
    Float(f64),
}

/// One function's running state.
#[derive(Debug, Clone, PartialEq)]
enum State {
    Count(i64),
    Sum(Option<Num>),
    Min(Option<Datum>),
    Max(Option<Datum>),
    Avg { sum: Num, count: i64 },
    Distinct(BTreeSet<Vec<u8>>),
}

fn overflow(what: &str) -> ExecError {
    ExecError::Internal(format!("Arithmetic overflow in {what}"))
}

fn is_decimal(t: &ColType) -> bool {
    matches!(t.ptype, T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I | T::DECIMAL256)
}

fn is_float(t: &ColType) -> bool {
    matches!(t.ptype, T::FLOAT | T::DOUBLE)
}

/// The largest magnitude a result type holds.
fn bound(t: &ColType) -> i128 {
    match t.ptype {
        T::TINYINT => i8::MAX.into(),
        T::SMALLINT => i16::MAX.into(),
        T::INT => i32::MAX.into(),
        T::BIGINT => i64::MAX.into(),
        T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I => 10i128.checked_pow(t.precision.clamp(1, 38) as u32).map_or(i128::MAX, |p| p - 1),
        _ => i128::MAX,
    }
}

fn number(d: &Datum, t: &ColType, to_scale: i32) -> Result<Option<Num>, ExecError> {
    Ok(match d {
        Datum::Null => None,
        Datum::Float(f) => Some(Num::Float(f64::from(*f))),
        Datum::Double(f) => Some(Num::Float(*f)),
        Datum::Bool(b) => Some(Num::Int(i128::from(*b))),
        Datum::Int(v) => {
            let from = if is_decimal(t) { t.scale } else { 0 };
            if to_scale < from {
                return Err(ExecError::Unsupported {
                    what: format!("a sum of scale {from} to a lower scale {to_scale}"),
                });
            }
            let v = if to_scale > from {
                10i128
                    .checked_pow((to_scale - from) as u32)
                    .and_then(|p| v.checked_mul(p))
                    .ok_or_else(|| overflow("a decimal"))?
            } else {
                *v
            };
            Some(Num::Int(v))
        }
        Datum::Bytes(_) => {
            return Err(ExecError::Unsupported {
                what: "a sum of strings".into(),
            })
        }
    })
}

fn add(a: Num, b: Num, ret: &ColType) -> Result<Num, ExecError> {
    Ok(match (a, b) {
        (Num::Int(x), Num::Int(y)) => {
            let s = x.checked_add(y).ok_or_else(|| overflow("sum"))?;
            if s.unsigned_abs() > bound(ret).unsigned_abs() && !is_float(ret) {
                return Err(overflow(&format!("sum as {}", ret.type_name())));
            }
            Num::Int(s)
        }
        (x, y) => Num::Float(as_f64(x) + as_f64(y)),
    })
}

fn as_f64(n: Num) -> f64 {
    match n {
        Num::Int(v) => v as f64,
        Num::Float(f) => f,
    }
}

impl State {
    fn new(f: &AggSpec) -> Result<State, ExecError> {
        Ok(match f.function.as_str() {
            "count" => State::Count(0),
            "sum" => State::Sum(None),
            "min" => State::Min(None),
            "max" => State::Max(None),
            "avg" => State::Avg {
                sum: if f.param_types.first().is_some_and(is_float) {
                    Num::Float(0.0)
                } else {
                    Num::Int(0)
                },
                count: 0,
            },
            "multi_distinct_count" => State::Distinct(BTreeSet::new()),
            other => {
                return Err(ExecError::Unsupported {
                    what: format!("function {other}"),
                })
            }
        })
    }

    fn update(&mut self, f: &AggSpec, args: &[(&ColType, &Datum)]) -> Result<(), ExecError> {
        let first = args.first().copied();
        let null = first.is_some_and(|(_, d)| *d == Datum::Null);
        match self {
            State::Count(n) => {
                if !args.iter().any(|(_, d)| **d == Datum::Null) {
                    *n += 1;
                }
            }
            State::Sum(s) => {
                let (t, d) = first.ok_or_else(|| ExecError::BadRequest("sum without its argument".into()))?;
                let scale = if is_decimal(&f.ret) { f.ret.scale } else { 0 };
                if let Some(v) = number(d, t, scale)? {
                    let v = if is_float(&f.ret) { Num::Float(as_f64(v)) } else { v };
                    *s = Some(match *s {
                        None => v,
                        Some(a) => add(a, v, &f.ret)?,
                    });
                }
            }
            State::Min(m) | State::Max(m) => {
                let want_less = matches!(self_kind(f), Kind::Min);
                if let Some((t, d)) = first.filter(|_| !null) {
                    let better = match m {
                        None => true,
                        Some(cur) => {
                            let o = compare(d, t, cur, t).unwrap_or(std::cmp::Ordering::Equal);
                            if want_less {
                                o.is_lt()
                            } else {
                                o.is_gt()
                            }
                        }
                    };
                    if better {
                        *m = Some(d.clone());
                    }
                }
            }
            State::Avg { sum, count } => {
                if let Some((t, d)) = first {
                    if let Some(v) = number(d, t, if is_decimal(t) { t.scale } else { 0 })? {
                        *sum = match (*sum, v) {
                            (Num::Int(a), Num::Int(b)) => Num::Int(a.checked_add(b).ok_or_else(|| overflow("avg"))?),
                            (a, b) => Num::Float(as_f64(a) + as_f64(b)),
                        };
                        *count += 1;
                    }
                }
            }
            State::Distinct(set) => {
                if let Some((_, d)) = first.filter(|_| !null) {
                    set.insert(canonical(std::slice::from_ref(d)));
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &State) -> Result<(), ExecError> {
        match (self, other) {
            (State::Count(a), State::Count(b)) => *a += b,
            (State::Sum(a), State::Sum(b)) => {
                if let Some(b) = b {
                    *a = Some(match *a {
                        None => *b,
                        Some(x) => match (x, *b) {
                            (Num::Int(x), Num::Int(y)) => Num::Int(x.checked_add(y).ok_or_else(|| overflow("sum"))?),
                            (x, y) => Num::Float(as_f64(x) + as_f64(y)),
                        },
                    });
                }
            }
            (State::Min(a), State::Min(b)) | (State::Max(a), State::Max(b)) if b.is_some() && a.is_none() => *a = b.clone(),
            (State::Min(a), State::Min(Some(b))) => {
                if let Some(x) = a.as_ref() {
                    if datum_cmp(b, x).is_lt() {
                        *a = Some(b.clone());
                    }
                }
            }
            (State::Max(a), State::Max(Some(b))) => {
                if let Some(x) = a.as_ref() {
                    if datum_cmp(b, x).is_gt() {
                        *a = Some(b.clone());
                    }
                }
            }
            (State::Min(_), State::Min(None)) | (State::Max(_), State::Max(None)) => {}
            (State::Avg { sum, count }, State::Avg { sum: s2, count: c2 }) => {
                *sum = match (*sum, *s2) {
                    (Num::Int(a), Num::Int(b)) => Num::Int(a.checked_add(b).ok_or_else(|| overflow("avg"))?),
                    (a, b) => Num::Float(as_f64(a) + as_f64(b)),
                };
                *count += c2;
            }
            (State::Distinct(a), State::Distinct(b)) => a.extend(b.iter().cloned()),
            (a, b) => return Err(ExecError::Internal(format!("cannot merge a {b:?} state into {a:?}"))),
        }
        Ok(())
    }

    fn finish(&self, f: &AggSpec) -> Result<Datum, ExecError> {
        let ret = &f.ret;
        Ok(match self {
            State::Count(n) => Datum::Int((*n).into()),
            State::Sum(s) => match s {
                // an empty or all-NULL sum is NULL; sum0's non-null result is 0
                None if ret.nullable => Datum::Null,
                None => zero(ret),
                Some(v) => num_datum(*v, ret)?,
            },
            State::Min(m) | State::Max(m) => m.clone().unwrap_or(Datum::Null),
            State::Avg { sum, count } => {
                if *count == 0 {
                    return Ok(Datum::Null);
                }
                if is_decimal(ret) {
                    let in_scale = f.param_types.first().filter(|t| is_decimal(t)).map_or(0, |t| t.scale);
                    let Num::Int(s) = *sum else {
                        return Err(ExecError::Internal("a decimal average of floats".into()));
                    };
                    let scaled = 10i128
                        .checked_pow((ret.scale - in_scale).max(0) as u32)
                        .and_then(|p| s.checked_mul(p))
                        .ok_or_else(|| overflow("avg"))?;
                    // integer division of the scaled sum: cut toward zero
                    Datum::Int(scaled / i128::from(*count))
                } else {
                    let v = as_f64(*sum) / *count as f64;
                    if ret.ptype == T::FLOAT {
                        Datum::Float(v as f32)
                    } else {
                        Datum::Double(v)
                    }
                }
            }
            State::Distinct(set) => Datum::Int(set.len() as i128),
        })
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let num = |n: &Num, out: &mut Vec<u8>| match n {
            Num::Int(v) => {
                out.push(0);
                out.extend(v.to_le_bytes());
            }
            Num::Float(f) => {
                out.push(1);
                out.extend(f.to_bits().to_le_bytes());
            }
        };
        match self {
            State::Count(n) => {
                out.push(0);
                out.extend(n.to_le_bytes());
            }
            State::Sum(s) => {
                out.push(1);
                match s {
                    None => out.push(0),
                    Some(n) => {
                        out.push(1);
                        num(n, &mut out);
                    }
                }
            }
            State::Min(m) | State::Max(m) => {
                out.push(if matches!(self, State::Min(_)) { 2 } else { 3 });
                encode_datum(m.as_ref().unwrap_or(&Datum::Null), &mut out);
                out.push(u8::from(m.is_some()));
            }
            State::Avg { sum, count } => {
                out.push(4);
                num(sum, &mut out);
                out.extend(count.to_le_bytes());
            }
            State::Distinct(set) => {
                out.push(5);
                out.extend((set.len() as u64).to_le_bytes());
                for k in set {
                    out.extend((k.len() as u64).to_le_bytes());
                    out.extend(k);
                }
            }
        }
        out
    }

    fn decode(b: &[u8]) -> Result<State, ExecError> {
        let mut r = Reader(b);
        let num = |r: &mut Reader| -> Result<Num, ExecError> {
            Ok(match r.u8()? {
                0 => Num::Int(r.i128()?),
                _ => Num::Float(f64::from_bits(r.u64()?)),
            })
        };
        Ok(match r.u8()? {
            0 => State::Count(r.u64()? as i64),
            1 => State::Sum(if r.u8()? == 1 { Some(num(&mut r)?) } else { None }),
            t @ (2 | 3) => {
                let d = r.datum()?;
                let some = r.u8()? == 1;
                let v = some.then_some(d);
                if t == 2 {
                    State::Min(v)
                } else {
                    State::Max(v)
                }
            }
            4 => State::Avg {
                sum: num(&mut r)?,
                count: r.u64()? as i64,
            },
            5 => {
                let n = r.u64()? as usize;
                let mut set = BTreeSet::new();
                for _ in 0..n {
                    let len = r.u64()? as usize;
                    set.insert(r.take(len)?.to_vec());
                }
                State::Distinct(set)
            }
            t => return Err(ExecError::Internal(format!("unknown partial aggregate state {t}"))),
        })
    }
}

enum Kind {
    Min,
    Max,
}

fn self_kind(f: &AggSpec) -> Kind {
    if f.function == "min" {
        Kind::Min
    } else {
        Kind::Max
    }
}

/// Values of one min/max state share a type; their stored forms order as the values do.
fn datum_cmp(a: &Datum, b: &Datum) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Datum::Int(x), Datum::Int(y)) => x.cmp(y),
        (Datum::Bool(x), Datum::Bool(y)) => x.cmp(y),
        (Datum::Bytes(x), Datum::Bytes(y)) => x.cmp(y),
        (Datum::Float(x), Datum::Float(y)) => crate::eval::float_cmp(f64::from(*x), f64::from(*y)),
        (Datum::Double(x), Datum::Double(y)) => crate::eval::float_cmp(*x, *y),
        _ => Ordering::Equal,
    }
}

fn zero(ret: &ColType) -> Datum {
    if is_float(ret) {
        if ret.ptype == T::FLOAT {
            Datum::Float(0.0)
        } else {
            Datum::Double(0.0)
        }
    } else {
        Datum::Int(0)
    }
}

fn num_datum(v: Num, ret: &ColType) -> Result<Datum, ExecError> {
    Ok(match (v, ret.ptype) {
        (Num::Float(f), T::FLOAT) => Datum::Float(f as f32),
        (Num::Float(f), _) => Datum::Double(f),
        (Num::Int(i), T::DOUBLE) => Datum::Double(i as f64),
        (Num::Int(i), T::FLOAT) => Datum::Float(i as f32),
        (Num::Int(i), _) => {
            if i.unsigned_abs() > bound(ret).unsigned_abs() {
                return Err(overflow(&format!("sum as {}", ret.type_name())));
            }
            Datum::Int(i)
        }
    })
}

/// An aggregation's limit with `agg_sort_info_by_group_key`: the `limit` groups first in that
/// order of the leading group keys (`AggSinkOperatorX`'s sort limit), not any `limit` groups.
pub fn top_groups(block: Block, info: &doris_thrift::gen::plan_nodes::TSortInfo, limit: usize) -> Block {
    let keys = crate::sort::keys(info, &(0..info.is_asc_order.len()).collect::<Vec<_>>());
    crate::sort::top_n(block, &keys, 0, Some(limit))
}
