//! Read-side expressions over blocks: the literals, slot references and casts of M4, plus the
//! comparison, logical, IN and NULL predicates and `substring` that M5 queries filter with.
//! Predicates follow SQL's three-valued logic: NULL is neither true nor false, and `filter`
//! keeps a row only when every conjunct is true.
use std::cmp::Ordering;
use std::collections::HashMap;

use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::{TExpr, TExprNode, TExprNodeType as N};
use doris_thrift::gen::opcodes::TExprOpcode;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::block::Block;
use crate::expr;
use crate::row_text::{datetimev2_text, datev2_text, decimal_text};
use crate::value::{to_datum, ColType, Value};
use crate::ExecError;

/// Slot id → column index in the input block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlotMap(pub HashMap<i32, usize>);

const COMPARISONS: &[&str] = &["eq", "ne", "lt", "gt", "le", "ge"];
/// The aggregate functions an aggregation node computes.
pub const AGGREGATES: &[&str] = &["count", "sum", "min", "max", "avg", "multi_distinct_count"];

fn function_name(n: &TExprNode) -> Option<String> {
    n.fn_
        .as_ref()
        .map(|f| f.name.function_name.to_ascii_lowercase())
}

fn describe(n: &TExprNode) -> String {
    match function_name(n) {
        Some(f) if n.node_type == N::FUNCTION_CALL || n.node_type == N::AGG_EXPR => {
            format!("function {f}")
        }
        _ => format!("expression {}", node_name(n)),
    }
}

/// A node type's Thrift name.
pub fn node_name(n: &TExprNode) -> String {
    let names = [
        (N::AGG_EXPR, "AGG_EXPR"),
        (N::BINARY_PRED, "BINARY_PRED"),
        (N::CASE_EXPR, "CASE_EXPR"),
        (N::COMPOUND_PRED, "COMPOUND_PRED"),
        (N::IN_PRED, "IN_PRED"),
        (N::ARITHMETIC_EXPR, "ARITHMETIC_EXPR"),
        (N::FUNCTION_CALL, "FUNCTION_CALL"),
        (N::LIKE_PRED, "LIKE_PRED"),
        (N::IS_NULL_PRED, "IS_NULL_PRED"),
    ];
    names
        .iter()
        .find(|(t, _)| *t == n.node_type)
        .map_or_else(|| format!("{:?}", n.node_type), |(_, s)| s.to_string())
}

fn comparison(n: &TExprNode) -> Option<&'static str> {
    let by_op = match n.opcode {
        Some(TExprOpcode::EQ) => Some("eq"),
        Some(TExprOpcode::NE) => Some("ne"),
        Some(TExprOpcode::LT) => Some("lt"),
        Some(TExprOpcode::GT) => Some("gt"),
        Some(TExprOpcode::LE) => Some("le"),
        Some(TExprOpcode::GE) => Some("ge"),
        _ => None,
    };
    by_op.or_else(|| {
        let f = function_name(n)?;
        COMPARISONS.iter().copied().find(|c| *c == f)
    })
}

/// Checks an expression uses only what `eval_column` evaluates; names the first node that does not.
pub fn check_read(e: &TExpr) -> Result<(), ExecError> {
    check_nodes(e, false)
}

/// As `check_read`, also accepting the aggregate calls of an aggregation node.
pub fn check_nodes(e: &TExpr, aggregates: bool) -> Result<(), ExecError> {
    for n in &e.nodes {
        let ok = match n.node_type {
            N::INT_LITERAL
            | N::LARGE_INT_LITERAL
            | N::STRING_LITERAL
            | N::BOOL_LITERAL
            | N::FLOAT_LITERAL
            | N::DECIMAL_LITERAL
            | N::DATE_LITERAL
            | N::NULL_LITERAL
            | N::SLOT_REF
            | N::CAST_EXPR
            | N::IN_PRED => true,
            N::BINARY_PRED => comparison(n).is_some(),
            N::COMPOUND_PRED => matches!(
                n.opcode,
                Some(
                    TExprOpcode::COMPOUND_AND
                        | TExprOpcode::COMPOUND_OR
                        | TExprOpcode::COMPOUND_NOT
                )
            ),
            N::FUNCTION_CALL => matches!(
                function_name(n).as_deref(),
                Some("substring" | "is_null_pred" | "is_not_null_pred")
            ),
            N::AGG_EXPR => {
                aggregates && AGGREGATES.contains(&function_name(n).unwrap_or_default().as_str())
            }
            _ => false,
        };
        if !ok {
            return Err(ExecError::Unsupported { what: describe(n) });
        }
        // an aggregate's serialized state travels as a string; its own types are checked by name
        if n.node_type == N::AGG_EXPR {
            continue;
        }
        match ColType::from_type_desc("", &n.type_, true) {
            Some(c)
                if c.supported() || n.node_type == N::NULL_LITERAL || c.ptype == T::NULL_TYPE => {}
            Some(c) => {
                return Err(ExecError::Unsupported {
                    what: format!("type {}", c.type_name()),
                })
            }
            None => {
                return Err(ExecError::Unsupported {
                    what: "a nested type".into(),
                })
            }
        }
    }
    Ok(())
}

fn bad(msg: impl Into<String>) -> ExecError {
    ExecError::BadRequest(msg.into())
}

/// A stored value as the expression value M4's casts take.
pub fn datum_value(d: &Datum, t: &ColType) -> Value {
    match d {
        Datum::Null => Value::Null,
        Datum::Bool(b) => Value::Bool(*b),
        Datum::Float(f) => Value::Float(f64::from(*f)),
        Datum::Double(f) => Value::Float(*f),
        Datum::Bytes(b) => Value::Str(b.clone()),
        Datum::Int(v) => match t.ptype {
            T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I | T::DECIMAL256 => {
                Value::Decimal(decimal_text(*v, t.scale))
            }
            T::DATEV2 => Value::Date(datev2_text(*v as u32)),
            T::DATETIMEV2 => Value::Date(datetimev2_text(*v as u64, t.scale)),
            T::BOOLEAN => Value::Bool(*v != 0),
            _ => Value::Int(*v),
        },
    }
}

fn node_type(n: &TExprNode) -> Result<ColType, ExecError> {
    ColType::from_type_desc("", &n.type_, true).ok_or_else(|| bad("expression of a nested type"))
}

fn as_datum(v: &Value, t: &ColType) -> Result<Datum, ExecError> {
    let t = ColType {
        nullable: true,
        ..t.clone()
    };
    to_datum(v, &t, false).map_err(|e| bad(e.msg))
}

/// The value of an expression for every row of a block, with its type.
pub fn eval_column(
    e: &TExpr,
    block: &Block,
    slots: &SlotMap,
) -> Result<(ColType, Vec<Datum>), ExecError> {
    let mut pos = 0;
    let out = eval_at(&e.nodes, &mut pos, block, slots)?;
    Ok(out)
}

fn bools(v: impl IntoIterator<Item = Option<bool>>) -> Vec<Datum> {
    v.into_iter()
        .map(|b| b.map_or(Datum::Null, Datum::Bool))
        .collect()
}

fn truth(d: &Datum) -> Option<bool> {
    match d {
        Datum::Null => None,
        Datum::Bool(b) => Some(*b),
        Datum::Int(v) => Some(*v != 0),
        Datum::Float(f) => Some(*f != 0.0),
        Datum::Double(f) => Some(*f != 0.0),
        Datum::Bytes(b) => Some(!b.is_empty()),
    }
}

fn eval_at(
    nodes: &[TExprNode],
    pos: &mut usize,
    block: &Block,
    slots: &SlotMap,
) -> Result<(ColType, Vec<Datum>), ExecError> {
    let n = nodes
        .get(*pos)
        .ok_or_else(|| bad("expression ends early"))?;
    *pos += 1;
    let mut kids = Vec::with_capacity(n.num_children.max(0) as usize);
    for _ in 0..n.num_children.max(0) {
        kids.push(eval_at(nodes, pos, block, slots)?);
    }
    let rows = block.len();
    let boolean = || ColType {
        name: String::new(),
        ptype: T::BOOLEAN,
        len: -1,
        precision: 0,
        scale: 0,
        nullable: true,
    };
    Ok(match n.node_type {
        N::SLOT_REF => {
            let id = n
                .slot_ref
                .as_ref()
                .ok_or_else(|| bad("slot ref without its slot"))?
                .slot_id;
            let &col = slots
                .0
                .get(&id)
                .ok_or_else(|| bad(format!("slot {id} is not in the input")))?;
            let t = block
                .types
                .get(col)
                .cloned()
                .ok_or_else(|| bad(format!("slot {id}: no column {col}")))?;
            (t, block.rows.iter().map(|r| r[col].clone()).collect())
        }
        N::INT_LITERAL
        | N::LARGE_INT_LITERAL
        | N::STRING_LITERAL
        | N::BOOL_LITERAL
        | N::FLOAT_LITERAL
        | N::DECIMAL_LITERAL
        | N::DATE_LITERAL
        | N::NULL_LITERAL => {
            let t = node_type(n)?;
            let v = expr::eval(
                &TExpr::new(vec![TExprNode {
                    num_children: 0,
                    ..n.clone()
                }]),
                &expr::Slots::new(),
            )
            .map_err(|e| bad(e.msg))?;
            let d = if v == Value::Null {
                Datum::Null
            } else {
                as_datum(&v, &t)?
            };
            (t, vec![d; rows])
        }
        N::CAST_EXPR => {
            let to = node_type(n)?;
            let (from, vals) = kids
                .into_iter()
                .next()
                .ok_or_else(|| bad("cast without its argument"))?;
            let out = vals
                .iter()
                .map(|d| {
                    if let Some(r) = round_datetime(d, &from, &to) {
                        return Ok(r);
                    }
                    let v = expr::cast(datum_value(d, &from), &to);
                    if v == Value::Null {
                        Ok(Datum::Null)
                    } else {
                        as_datum(&v, &to)
                    }
                })
                .collect::<Result<_, _>>()?;
            (to, out)
        }
        N::BINARY_PRED => {
            let op = comparison(n).ok_or_else(|| ExecError::Unsupported { what: describe(n) })?;
            let [(ta, a), (tb, b)] =
                <[_; 2]>::try_from(kids).map_err(|_| bad("comparison needs two arguments"))?;
            refuse_mixed(&ta, &tb)?;
            let out = a.iter().zip(&b).map(|(x, y)| {
                compare(x, &ta, y, &tb).map(|o| match op {
                    "eq" => o == Ordering::Equal,
                    "ne" => o != Ordering::Equal,
                    "lt" => o == Ordering::Less,
                    "gt" => o == Ordering::Greater,
                    "le" => o != Ordering::Greater,
                    _ => o != Ordering::Less,
                })
            });
            (boolean(), bools(out))
        }
        N::COMPOUND_PRED => {
            let args: Vec<Vec<Option<bool>>> = kids
                .iter()
                .map(|(_, v)| v.iter().map(truth).collect())
                .collect();
            let out: Vec<Option<bool>> = match n.opcode {
                Some(TExprOpcode::COMPOUND_NOT) => args
                    .first()
                    .ok_or_else(|| bad("not without its argument"))?
                    .iter()
                    .map(|b| b.map(|b| !b))
                    .collect(),
                Some(op @ (TExprOpcode::COMPOUND_AND | TExprOpcode::COMPOUND_OR)) => {
                    let and = op == TExprOpcode::COMPOUND_AND;
                    (0..rows)
                        .map(|r| {
                            let vals = args.iter().map(|a| a[r]);
                            let mut unknown = false;
                            for v in vals {
                                match v {
                                    Some(b) if b != and => return Some(b),
                                    None => unknown = true,
                                    _ => {}
                                }
                            }
                            if unknown {
                                None
                            } else {
                                Some(and)
                            }
                        })
                        .collect()
                }
                _ => return Err(ExecError::Unsupported { what: describe(n) }),
            };
            (boolean(), bools(out))
        }
        N::IN_PRED => {
            let not = n.in_predicate.as_ref().is_some_and(|p| p.is_not_in)
                || n.opcode == Some(TExprOpcode::FILTER_NOT_IN);
            let mut kids = kids.into_iter();
            let (tv, vals) = kids.next().ok_or_else(|| bad("IN without its value"))?;
            let list: Vec<(ColType, Vec<Datum>)> = kids.collect();
            for (tl, _) in &list {
                refuse_mixed(&tv, tl)?;
            }
            let out = (0..rows).map(|r| {
                if vals[r] == Datum::Null {
                    return None;
                }
                let mut unknown = false;
                for (tl, l) in &list {
                    match compare(&vals[r], &tv, &l[r], tl) {
                        Some(Ordering::Equal) => return Some(!not),
                        None => unknown = true,
                        _ => {}
                    }
                }
                if unknown {
                    None
                } else {
                    Some(not)
                }
            });
            (boolean(), bools(out))
        }
        N::FUNCTION_CALL => match function_name(n).as_deref() {
            Some(f @ ("is_null_pred" | "is_not_null_pred")) => {
                let (_, v) = kids
                    .first()
                    .ok_or_else(|| bad(format!("{f} without its argument")))?;
                let null = f == "is_null_pred";
                (
                    boolean(),
                    v.iter()
                        .map(|d| Datum::Bool((*d == Datum::Null) == null))
                        .collect(),
                )
            }
            Some("substring") => {
                let t = node_type(n)?;
                let out = (0..rows)
                    .map(|r| {
                        let args: Vec<Value> =
                            kids.iter().map(|(t, v)| datum_value(&v[r], t)).collect();
                        match expr::substring(&args) {
                            Value::Str(s) if args.iter().all(|a| *a != Value::Null) => {
                                Datum::Bytes(s)
                            }
                            _ => Datum::Null,
                        }
                    })
                    .collect();
                (t, out)
            }
            _ => return Err(ExecError::Unsupported { what: describe(n) }),
        },
        _ => return Err(ExecError::Unsupported { what: describe(n) }),
    })
}

/// A comparable form of a value: integers and dates as integers at a scale, floats, bytes.
enum Key<'a> {
    Fixed(i128, i32),
    Float(f64),
    Bytes(&'a [u8]),
}

fn key<'a>(d: &'a Datum, t: &ColType) -> Option<Key<'a>> {
    Some(match d {
        Datum::Null => return None,
        Datum::Bool(b) => Key::Fixed(i128::from(*b), 0),
        Datum::Float(f) => Key::Float(f64::from(*f)),
        Datum::Double(f) => Key::Float(*f),
        Datum::Bytes(b) => Key::Bytes(b),
        Datum::Int(v) => match t.ptype {
            T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I | T::DECIMAL256 => Key::Fixed(*v, t.scale),
            // a date compares with a datetime as its midnight
            T::DATEV2 => {
                let v = *v as u64;
                Key::Fixed(
                    i128::from((v >> 9) << 46 | ((v >> 5) & 0xF) << 42 | (v & 0x1F) << 37),
                    0,
                )
            }
            _ => Key::Fixed(*v, 0),
        },
    })
}

/// Floats order as the C++ BE orders them: NaN equals NaN and is greater than any number.
pub fn float_cmp(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
    }
}

fn rescale(v: i128, from: i32, to: i32) -> Option<i128> {
    10i128
        .checked_pow((to - from) as u32)
        .and_then(|p| v.checked_mul(p))
}

/// Two values' order; None when either is NULL.
pub fn compare(a: &Datum, ta: &ColType, b: &Datum, tb: &ColType) -> Option<Ordering> {
    Some(match (key(a, ta)?, key(b, tb)?) {
        (Key::Fixed(x, sx), Key::Fixed(y, sy)) => {
            let s = sx.max(sy);
            match (rescale(x, sx, s), rescale(y, sy, s)) {
                (Some(x), Some(y)) => x.cmp(&y),
                _ => float_cmp(x as f64 / 10f64.powi(sx), y as f64 / 10f64.powi(sy)),
            }
        }
        (Key::Fixed(x, sx), Key::Float(y)) => float_cmp(x as f64 / 10f64.powi(sx), y),
        (Key::Float(x), Key::Fixed(y, sy)) => float_cmp(x, y as f64 / 10f64.powi(sy)),
        (Key::Float(x), Key::Float(y)) => float_cmp(x, y),
        (Key::Bytes(x), Key::Bytes(y)) => x.cmp(y),
        // the FE casts both sides to one type; text against a number has no order of its own
        (Key::Bytes(x), _) => x.cmp(datum_bytes(b, tb).as_slice()),
        (_, Key::Bytes(y)) => datum_bytes(a, ta).as_slice().cmp(y),
    })
}

/// Text compared with a number: the FE casts both sides to one type, so a comparison it left
/// mixed is refused rather than ordered by text.
fn refuse_mixed(a: &ColType, b: &ColType) -> Result<(), ExecError> {
    let text = |t: &ColType| matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING);
    let null = |t: &ColType| t.ptype == T::NULL_TYPE;
    if text(a) != text(b) && !null(a) && !null(b) {
        return Err(ExecError::Unsupported {
            what: format!("a comparison of text against {}", if text(a) { b.type_name() } else { a.type_name() }),
        });
    }
    Ok(())
}

fn datum_bytes(d: &Datum, t: &ColType) -> Vec<u8> {
    crate::row_text::cell_text(d, t).unwrap_or_default()
}

/// The rows of a block for which every conjunct is true.
pub fn filter(conjuncts: &[TExpr], block: Block, slots: &SlotMap) -> Result<Block, ExecError> {
    if conjuncts.is_empty() {
        return Ok(block);
    }
    let mut keep = vec![true; block.len()];
    for c in conjuncts {
        let (_, v) = eval_column(c, &block, slots)?;
        for (k, d) in keep.iter_mut().zip(&v) {
            *k &= truth(d) == Some(true);
        }
    }
    let Block { types, rows } = block;
    let rows = rows
        .into_iter()
        .zip(keep)
        .filter_map(|(r, k)| k.then_some(r))
        .collect();
    Ok(Block { types, rows })
}

fn same_storage(a: &ColType, b: &ColType) -> bool {
    let text = |t: &ColType| matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING);
    (text(a) && text(b)) || (a.ptype == b.ptype && a.scale == b.scale)
}

/// A DATETIMEV2 cast to a lower scale: rounded, not cut (`transform_date_scale`).
fn round_datetime(d: &Datum, from: &ColType, to: &ColType) -> Option<Datum> {
    match d {
        Datum::Int(v) if from.ptype == T::DATETIMEV2 && to.ptype == T::DATETIMEV2 && to.scale < from.scale => {
            Some(crate::value::round_datetimev2(*v as u64, from.scale, to.scale).map_or(Datum::Null, |r| Datum::Int(r.into())))
        }
        _ => None,
    }
}

/// A value of type `from` as a value of type `to` (a slot's type), cast when they differ.
pub fn coerce(d: Datum, from: &ColType, to: &ColType) -> Result<Datum, ExecError> {
    if d == Datum::Null || same_storage(from, to) || to.ptype == T::NULL_TYPE {
        return Ok(d);
    }
    if let Some(r) = round_datetime(&d, from, to) {
        return Ok(r);
    }
    let v = expr::cast(datum_value(&d, from), to);
    if v == Value::Null {
        return Ok(Datum::Null);
    }
    as_datum(&v, to)
}
