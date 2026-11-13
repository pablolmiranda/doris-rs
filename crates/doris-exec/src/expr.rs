//! The expressions write fragments carry: typed literals, slot references, casts, the
//! `substring(str, 1, len)` the FE adds to cut CHAR/VARCHAR values when inserts are not strict,
//! and `concat` in stream-load column mappings.
//! A `TExpr` is its nodes in pre-order, each with `num_children`.
use std::collections::HashMap;

use doris_thrift::gen::exprs::{TExpr, TExprNode, TExprNodeType};
use doris_thrift::gen::types::TPrimitiveType;

use crate::value::{parse_text, ColType, Value};
use crate::{ExecError, RowError};

/// Slot id → value, the input a row's expressions read.
pub type Slots = HashMap<i32, Value>;

/// Checks an expression uses only what `eval` evaluates; names the first node that does not.
pub fn check(e: &TExpr) -> Result<(), ExecError> {
    for (i, n) in e.nodes.iter().enumerate() {
        let ok = match n.node_type {
            TExprNodeType::INT_LITERAL
            | TExprNodeType::LARGE_INT_LITERAL
            | TExprNodeType::STRING_LITERAL
            | TExprNodeType::BOOL_LITERAL
            | TExprNodeType::FLOAT_LITERAL
            | TExprNodeType::DECIMAL_LITERAL
            | TExprNodeType::DATE_LITERAL
            | TExprNodeType::NULL_LITERAL
            | TExprNodeType::SLOT_REF => true,
            TExprNodeType::CAST_EXPR => ColType::from_type_desc("", &n.type_, true).is_some_and(|c| c.writable()),
            TExprNodeType::FUNCTION_CALL => {
                matches!(function_name(n).as_deref(), Some("substring" | "concat" | "now" | "current_date" | "curdate"))
                    || string_function(n)
                    || int_arithmetic(n)
                    || crypto_function(&e.nodes, i)
            }
            TExprNodeType::ARITHMETIC_EXPR => int_arithmetic(n),
            _ => false,
        };
        if !ok {
            return Err(ExecError::Unsupported { what: describe(n) });
        }
        if let Some(c) = ColType::from_type_desc("", &n.type_, true) {
            if !c.writable() && n.node_type != TExprNodeType::NULL_LITERAL {
                return Err(ExecError::Unsupported { what: format!("type {}", c.type_name()) });
            }
        } else {
            return Err(ExecError::Unsupported { what: "a nested type".into() });
        }
    }
    Ok(())
}

/// The index one past node `i`'s subtree (pre-order, `num_children` each).
fn subtree_end(nodes: &[TExprNode], i: usize) -> usize {
    let mut left = 1usize;
    let mut j = i;
    while left > 0 && j < nodes.len() {
        left = left - 1 + nodes[j].num_children.max(0) as usize;
        j += 1;
    }
    j
}

/// An encryption, digest or base64 call of `crypto` (a stream load's column mapping), with an
/// encrypt or decrypt's mode a literal `crypto` runs.
fn crypto_function(nodes: &[TExprNode], i: usize) -> bool {
    let n = &nodes[i];
    let Some(name) = function_name(n).filter(|f| crate::crypto::is_crypto_function(f)) else { return false };
    let Some(f) = n.fn_.as_ref() else { return false };
    let args: Option<Vec<ColType>> = f.arg_types.iter().map(|t| ColType::from_type_desc("", t, true)).collect();
    let Some(to) = ColType::from_type_desc("", &n.type_, true) else { return false };
    if !args.is_some_and(|a| crate::crypto::supported(&name, &a, &to)) {
        return false;
    }
    // the fourth argument's subtree: a lone STRING_LITERAL is the mode
    let mut at = i + 1;
    for _ in 0..3 {
        at = subtree_end(nodes, at);
    }
    let mode = nodes
        .get(at)
        .filter(|m| m.node_type == TExprNodeType::STRING_LITERAL)
        .and_then(|m| m.string_literal.as_ref())
        .map(|l| l.value.as_bytes());
    crate::crypto::writable(&name, mode, n.num_children == 5)
}

/// A `crypto` call on one row's values; NULL in, NULL out.
fn crypto_call(n: &TExprNode, children: Vec<Value>) -> Result<Value, RowError> {
    use doris_segment::writer::Datum;
    let name = function_name(n).unwrap_or_default();
    let mut cols = Vec::with_capacity(children.len());
    for v in children {
        cols.push(match v {
            Value::Null => return Ok(Value::Null),
            Value::Str(b) => vec![Datum::Bytes(b)],
            other => return Err(RowError::new(format!("{name} of {other:?}"))),
        });
    }
    let args: Vec<&[Datum]> = cols.iter().map(Vec::as_slice).collect();
    let out = crate::crypto::eval(&name, &args, 1).map_err(|e| RowError::new(e.to_string()))?;
    Ok(match out.into_iter().next() {
        Some(Datum::Bytes(b)) => Value::Str(b),
        _ => Value::Null,
    })
}

/// A text-to-text call of `strfn` (`unhex`, `char`, …) whose signature it evaluates.
fn string_function(n: &TExprNode) -> bool {
    let Some(f) = n.fn_.as_ref() else { return false };
    let name = f.name.function_name.to_ascii_lowercase();
    let args: Option<Vec<ColType>> = f.arg_types.iter().map(|t| ColType::from_type_desc("", t, true)).collect();
    let to = ColType::from_type_desc("", &n.type_, true);
    let text = |t: &ColType| matches!(t.ptype, TPrimitiveType::CHAR | TPrimitiveType::VARCHAR | TPrimitiveType::STRING);
    match (args, to) {
        // a failing call would only filter its row on this path, where C++ fails the statement
        // money_format and format_round take numbers, which this bridge does not carry
        (Some(_), Some(_)) if matches!(name.as_str(), "url_decode" | "parse_data_size" | "money_format" | "format_round") => false,
        (Some(args), Some(to)) => crate::strfn::is_string_function(&name) && text(&to) && crate::strfn::supported(&name, &args, &to),
        _ => false,
    }
}

/// Integer `add`/`subtract`/`multiply`/`mod`, or `abs` of an integer, that `arith` evaluates (a
/// generated column `abs(a + b)` arrives so).
fn int_arithmetic(n: &TExprNode) -> bool {
    let Some(f) = n.fn_.as_ref() else { return false };
    let name = f.name.function_name.to_ascii_lowercase();
    let args: Option<Vec<ColType>> = f.arg_types.iter().map(|t| ColType::from_type_desc("", t, true)).collect();
    let Some(to) = ColType::from_type_desc("", &n.type_, true) else { return false };
    let int = |t: &ColType| matches!(t.ptype, TPrimitiveType::TINYINT | TPrimitiveType::SMALLINT | TPrimitiveType::INT | TPrimitiveType::BIGINT | TPrimitiveType::LARGEINT);
    matches!(name.as_str(), "add" | "subtract" | "multiply" | "mod" | "abs")
        && args.as_ref().is_some_and(|a| a.iter().all(int))
        && int(&to)
        && args.is_some_and(|a| crate::arith::arithmetic_supported(&name, &a, &to))
}

/// An `int_arithmetic` call on one row's values; NULL in, NULL out.
fn int_arithmetic_call(n: &TExprNode, children: Vec<Value>) -> Result<Value, RowError> {
    use doris_segment::writer::Datum;
    let name = function_name(n).unwrap_or_default();
    let types: Vec<ColType> = n.fn_.iter().flat_map(|f| &f.arg_types).filter_map(|t| ColType::from_type_desc("", t, true)).collect();
    let to = ColType::from_type_desc("", &n.type_, true).ok_or_else(|| RowError::new(format!("{name} to a nested type")))?;
    let mut cols = Vec::with_capacity(children.len());
    for v in children {
        cols.push(match v {
            Value::Null => return Ok(Value::Null),
            Value::Int(i) => vec![Datum::Int(i)],
            other => return Err(RowError::new(format!("{name} of {other:?}"))),
        });
    }
    let out = match (name.as_str(), types.as_slice(), cols.as_slice()) {
        ("abs", [ta], [a]) => crate::arith::abs((ta, a), &to),
        (_, [ta, tb], [a, b]) => crate::arith::arithmetic(&name, (ta, a), (tb, b), &to),
        _ => return Err(RowError::new(format!("{name} of {} values", cols.len()))),
    }
    .map_err(|e| RowError::new(e.to_string()))?;
    Ok(match out.into_iter().next() {
        Some(Datum::Int(i)) => Value::Int(i),
        _ => Value::Null,
    })
}

/// A `strfn` call on one row's values.
fn string_call(n: &TExprNode, children: Vec<Value>) -> Result<Value, RowError> {
    let name = function_name(n).unwrap_or_default();
    let name = name.as_str();
    let types: Vec<ColType> = n.fn_.iter().flat_map(|f| &f.arg_types).filter_map(|t| ColType::from_type_desc("", t, true)).collect();
    use doris_segment::writer::Datum;
    let datum = |v: Value| match v {
        Value::Null => Ok(Datum::Null),
        Value::Int(i) => Ok(Datum::Int(i)),
        Value::Str(b) => Ok(Datum::Bytes(b)),
        other => Err(RowError::new(format!("{name} of {other:?}"))),
    };
    let cols: Vec<Vec<Datum>> = children.into_iter().map(|v| datum(v).map(|d| vec![d])).collect::<Result<_, _>>()?;
    let args: Vec<&[Datum]> = cols.iter().map(Vec::as_slice).collect();
    let out = crate::strfn::eval(name, &types, &args, 1).map_err(|e| RowError::new(e.to_string()))?;
    Ok(match out.into_iter().next() {
        Some(Datum::Bytes(b)) => Value::Str(b),
        Some(Datum::Int(i)) => Value::Int(i),
        _ => Value::Null,
    })
}

fn function_name(n: &TExprNode) -> Option<String> {
    n.fn_.as_ref().map(|f| f.name.function_name.to_ascii_lowercase())
}

fn describe(n: &TExprNode) -> String {
    match function_name(n) {
        Some(f) => format!("function {f}"),
        None => format!("expression {:?}", n.node_type),
    }
}

/// Evaluates an expression over a row's slots.
pub fn eval(e: &TExpr, slots: &Slots) -> Result<Value, RowError> {
    let mut pos = 0;
    eval_at(&e.nodes, &mut pos, slots)
}

fn eval_at(nodes: &[TExprNode], pos: &mut usize, slots: &Slots) -> Result<Value, RowError> {
    let n = nodes.get(*pos).ok_or_else(|| RowError::new("expression ends early"))?;
    *pos += 1;
    let mut children = Vec::with_capacity(n.num_children.max(0) as usize);
    for _ in 0..n.num_children.max(0) {
        children.push(eval_at(nodes, pos, slots)?);
    }
    let lit = || RowError::new(format!("literal without its value: {:?}", n.node_type));
    Ok(match n.node_type {
        TExprNodeType::NULL_LITERAL => Value::Null,
        TExprNodeType::INT_LITERAL => Value::Int(n.int_literal.as_ref().ok_or_else(lit)?.value.into()),
        TExprNodeType::LARGE_INT_LITERAL => {
            Value::Int(n.large_int_literal.as_ref().ok_or_else(lit)?.value.parse().map_err(|_| lit())?)
        }
        TExprNodeType::STRING_LITERAL => Value::Str(n.string_literal.as_ref().ok_or_else(lit)?.value.clone().into_bytes()),
        TExprNodeType::BOOL_LITERAL => Value::Bool(n.bool_literal.as_ref().ok_or_else(lit)?.value),
        TExprNodeType::FLOAT_LITERAL => Value::Float(n.float_literal.as_ref().ok_or_else(lit)?.value.0),
        TExprNodeType::DECIMAL_LITERAL => Value::Decimal(n.decimal_literal.as_ref().ok_or_else(lit)?.value.clone()),
        TExprNodeType::DATE_LITERAL => Value::Date(n.date_literal.as_ref().ok_or_else(lit)?.value.clone()),
        TExprNodeType::SLOT_REF => {
            let id = n.slot_ref.as_ref().ok_or_else(lit)?.slot_id;
            slots.get(&id).cloned().ok_or_else(|| RowError::new(format!("slot {id} has no value")))?
        }
        TExprNodeType::CAST_EXPR => {
            let to = ColType::from_type_desc("", &n.type_, true).ok_or_else(|| RowError::new("cast to a nested type"))?;
            cast(children.into_iter().next().unwrap_or(Value::Null), &to)
        }
        TExprNodeType::FUNCTION_CALL if function_name(n).as_deref() == Some("substring") => substring(&children),
        TExprNodeType::FUNCTION_CALL if function_name(n).as_deref() == Some("concat") => concat(&children),
        TExprNodeType::FUNCTION_CALL if string_function(n) => string_call(n, children)?,
        TExprNodeType::FUNCTION_CALL if function_name(n).is_some_and(|f| crate::crypto::is_crypto_function(&f)) => crypto_call(n, children)?,
        TExprNodeType::FUNCTION_CALL | TExprNodeType::ARITHMETIC_EXPR if int_arithmetic(n) => int_arithmetic_call(n, children)?,
        TExprNodeType::FUNCTION_CALL if matches!(function_name(n).as_deref(), Some("now" | "current_date" | "curdate")) => {
            let f = function_name(n).unwrap_or_default();
            let clock = crate::clock::current().ok_or_else(|| RowError::new(format!("{f}() without the query's clock")))?;
            if f == "now" {
                let scale = match children.first() {
                    None => 0,
                    Some(Value::Int(s)) => *s as i32,
                    Some(_) => return Ok(Value::Null),
                };
                Value::Date(crate::row_text::datetimev2_text(clock.now(scale), scale.clamp(0, 6)))
            } else {
                Value::Date(crate::row_text::datev2_text(clock.today()))
            }
        }
        _ => return Err(RowError::new(format!("unsupported {}", describe(n)))),
    })
}

/// A cast as the non-strict load path does it: a value that does not convert becomes NULL.
pub fn cast(v: Value, to: &ColType) -> Value {
    let text = match &v {
        Value::Null => return Value::Null,
        Value::Str(b) => String::from_utf8_lossy(b).into_owned(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => crate::row_text::double_text(*f),
        Value::Bool(b) => if *b { "1".into() } else { "0".into() },
        Value::Decimal(s) | Value::Date(s) => s.clone(),
    };
    parse_text(&text, to).unwrap_or(Value::Null)
}

/// `concat(str...)`: the arguments joined; NULL when any argument is NULL.
pub fn concat(args: &[Value]) -> Value {
    let mut out = Vec::new();
    for a in args {
        match a {
            Value::Null => return Value::Null,
            Value::Str(b) => out.extend_from_slice(b),
            other => match cast(other.clone(), &ColType { name: String::new(), ptype: TPrimitiveType::STRING, len: -1, precision: 0, scale: 0, nullable: true, ..ColType::default() }) {
                Value::Str(b) => out.extend_from_slice(&b),
                _ => return Value::Null,
            },
        }
    }
    Value::Str(out)
}

/// `substring(str, pos, len)`, positions in UTF-8 characters, 1-based.
pub fn substring(args: &[Value]) -> Value {
    let (Some(Value::Str(s)), Some(Value::Int(pos)), Some(Value::Int(len))) = (args.first(), args.get(1), args.get(2)) else {
        return Value::Null;
    };
    let text = String::from_utf8_lossy(s);
    let chars: Vec<char> = text.chars().collect();
    let n = chars.len() as i128;
    let start = if *pos > 0 { pos - 1 } else if *pos < 0 { n + pos } else { return Value::Str(Vec::new()) };
    if start < 0 || start >= n || *len <= 0 {
        return Value::Str(Vec::new());
    }
    let end = (start + len).min(n);
    Value::Str(chars[start as usize..end as usize].iter().collect::<String>().into_bytes())
}
