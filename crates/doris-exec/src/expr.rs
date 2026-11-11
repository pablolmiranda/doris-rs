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
    for n in &e.nodes {
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
                matches!(function_name(n).as_deref(), Some("substring" | "concat" | "now" | "current_date" | "curdate")) || string_function(n)
            }
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

/// A text-to-text call of `strfn` (`unhex`, `char`, …) whose signature it evaluates.
fn string_function(n: &TExprNode) -> bool {
    let Some(f) = n.fn_.as_ref() else { return false };
    let name = f.name.function_name.to_ascii_lowercase();
    let args: Option<Vec<ColType>> = f.arg_types.iter().map(|t| ColType::from_type_desc("", t, true)).collect();
    let to = ColType::from_type_desc("", &n.type_, true);
    let text = |t: &ColType| matches!(t.ptype, TPrimitiveType::CHAR | TPrimitiveType::VARCHAR | TPrimitiveType::STRING);
    match (args, to) {
        (Some(args), Some(to)) => crate::strfn::is_string_function(&name) && text(&to) && crate::strfn::supported(&name, &args, &to),
        _ => false,
    }
}

/// A `strfn` call on one row's values.
fn string_call(name: &str, children: Vec<Value>) -> Result<Value, RowError> {
    use doris_segment::writer::Datum;
    let datum = |v: Value| match v {
        Value::Null => Ok(Datum::Null),
        Value::Int(i) => Ok(Datum::Int(i)),
        Value::Str(b) => Ok(Datum::Bytes(b)),
        other => Err(RowError::new(format!("{name} of {other:?}"))),
    };
    let cols: Vec<Vec<Datum>> = children.into_iter().map(|v| datum(v).map(|d| vec![d])).collect::<Result<_, _>>()?;
    let args: Vec<&[Datum]> = cols.iter().map(Vec::as_slice).collect();
    let out = crate::strfn::eval(name, &args, 1).map_err(|e| RowError::new(e.to_string()))?;
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
        TExprNodeType::FUNCTION_CALL if string_function(n) => string_call(&function_name(n).unwrap_or_default(), children)?,
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
            other => match cast(other.clone(), &ColType { name: String::new(), ptype: TPrimitiveType::STRING, len: -1, precision: 0, scale: 0, nullable: true }) {
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
