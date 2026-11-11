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
pub const AGGREGATES: &[&str] = &[
    "count", "sum", "min", "max", "avg", "multi_distinct_count", "sum0", "stddev", "group_concat", "ndv", "window_funnel_v2", "kurt", "kurt_pop", "kurtosis", "skew",
    "skew_pop", "skewness", "regr_slope", "regr_intercept", "corr", "corr_welford", "covar", "covar_pop", "covar_samp", "avg_weighted",
];

fn function_name(n: &TExprNode) -> Option<String> {
    n.fn_
        .as_ref()
        .map(|f| f.name.function_name.to_ascii_lowercase())
}

/// A call's argument types (from its `TFunction`) and result type (the node's).
fn signature(n: &TExprNode) -> Option<(Vec<ColType>, ColType)> {
    let f = n.fn_.as_ref()?;
    let args = f.arg_types.iter().map(|t| ColType::from_type_desc("", t, true)).collect::<Option<Vec<_>>>()?;
    Some((args, ColType::from_type_desc("", &n.type_, true)?))
}

/// A call `arith::arithmetic_supported` accepts, by its name and signature.
fn arithmetic_call(n: &TExprNode) -> bool {
    let (Some(name), Some((args, to))) = (function_name(n), signature(n)) else { return false };
    crate::arith::arithmetic_supported(&name, &args, &to)
}

/// A call `bits::supported` accepts, by its name and signature.
fn bit_call(n: &TExprNode) -> bool {
    let (Some(name), Some((args, to))) = (function_name(n), signature(n)) else { return false };
    crate::bits::supported(&name, &args, &to)
}

/// A date add/sub function: the only ones the FE sends as a COMPUTE_FUNCTION_CALL.
fn interval_function(f: &str) -> bool {
    crate::datetime::is_date_function(f) && (f.ends_with("_add") || f.ends_with("_sub"))
}

/// A call `strfn::supported` accepts, by its name and signature.
fn strfn_call(n: &TExprNode) -> bool {
    let (Some(name), Some((args, to))) = (function_name(n), signature(n)) else { return false };
    crate::strfn::supported(&name, &args, &to)
}

/// A call `math::supported` accepts, by its name and signature.
fn math_call(n: &TExprNode) -> bool {
    let (Some(name), Some((args, to))) = (function_name(n), signature(n)) else { return false };
    crate::math::supported(&name, &args, &to)
}

/// A call `datetime::supported` accepts, by its name and signature.
fn date_call(n: &TExprNode) -> bool {
    let (Some(name), Some((args, to))) = (function_name(n), signature(n)) else { return false };
    crate::datetime::supported(&name, &args, &to)
}

/// A call `geo::supported` accepts, by its name and signature.
fn geo_call(n: &TExprNode) -> bool {
    let (Some(name), Some((args, to))) = (function_name(n), signature(n)) else { return false };
    crate::geo::supported(&name, &args, &to)
}

/// The bit functions, as ARITHMETIC_EXPR (`bitand` …) or FUNCTION_CALL (`bit_count` …).
const BIT_FUNCTIONS: &[&str] = &[
    "bitand", "bitor", "bitxor", "bitnot", "bit_count", "bit_shift_left", "bit_shift_right", "bit_test", "bit_test_all", "xor",
];

/// `if`, `ifnull`, `coalesce` over supported scalar types, `if`'s condition BOOLEAN.
fn conditional_call(n: &TExprNode) -> bool {
    let (Some(name), Some((args, to))) = (function_name(n), signature(n)) else { return false };
    let scalar = |t: &ColType| t.supported() || t.ptype == T::NULL_TYPE;
    let values = match name.as_str() {
        "if" if args.len() == 3 && args[0].ptype == T::BOOLEAN => &args[1..],
        "ifnull" if args.len() == 2 => &args[..],
        "coalesce" if !args.is_empty() => &args[..],
        _ => return false,
    };
    scalar(&to) && values.iter().all(scalar)
}

/// `like`, `concat`, `length` over text arguments.
fn string_call(n: &TExprNode) -> bool {
    let (Some(name), Some((args, to))) = (function_name(n), signature(n)) else { return false };
    let text = |t: &ColType| matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING);
    match name.as_str() {
        "like" => (args.len() == 2 || args.len() == 3) && args.iter().all(text) && to.ptype == T::BOOLEAN,
        "concat" => !args.is_empty() && args.iter().all(text) && text(&to),
        "length" => args.len() == 1 && text(&args[0]) && to.ptype == T::INT,
        "regexp" | "rlike" => args.len() == 2 && args.iter().all(text) && to.ptype == T::BOOLEAN,
        "concat_ws" => args.len() >= 2 && args.iter().all(text) && text(&to),
        "split_by_string" => args.len() == 2 && args.iter().all(text) && to.element().is_some_and(|e| text(&e)),
        _ => false,
    }
}

/// `now([INT])` into DATETIMEV2, `current_date()` (`curdate()`) into DATEV2.
fn clock_call(n: &TExprNode) -> bool {
    let (Some(name), Some((args, to))) = (function_name(n), signature(n)) else { return false };
    match name.as_str() {
        "now" => to.ptype == T::DATETIMEV2 && (args.is_empty() || (args.len() == 1 && args[0].ptype == T::INT)),
        _ => to.ptype == T::DATEV2 && args.is_empty(),
    }
}

/// The number of nodes in the subtree at `i` (pre-order, `num_children` each).
fn subtree_len(nodes: &[TExprNode], i: usize) -> usize {
    let mut len = 1;
    for _ in 0..nodes.get(i).map_or(0, |n| n.num_children.max(0)) {
        len += subtree_len(nodes, i + len);
    }
    len
}

/// `round(x[, d])` of a DOUBLE or a DECIMAL32/64/128I into the same type, `d` an INT constant.
/// `round`/`truncate`/`floor`/`ceil(x[, d])` on DOUBLE or a decimal. `d` may be a column
/// (`apply_vec_vec`, round.h:519-670) only for `truncate`, the one the corpus sends so.
fn round_call(nodes: &[TExprNode], i: usize) -> bool {
    let n = &nodes[i];
    let Some((args, to)) = signature(n) else { return false };
    let x_ok = match args.first() {
        Some(x) if x.ptype == T::DOUBLE => to.ptype == T::DOUBLE,
        Some(x) => matches!(x.ptype, T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I) && to.ptype == x.ptype,
        None => false,
    };
    let d_ok = match args.len() {
        1 => true,
        2 => {
            let start = i + 1 + subtree_len(nodes, i + 1);
            // a node list shorter than its num_children claim is malformed: not accepted
            let column_ok = function_name(n).as_deref() == Some("truncate");
            args[1].ptype == T::INT
                && nodes
                    .get(start..start + subtree_len(nodes, start))
                    .is_some_and(|d| column_ok || d.iter().all(|k| k.node_type != N::SLOT_REF))
        }
        _ => false,
    };
    x_ok && d_ok
}

/// `assert_true(BOOLEAN, msg)`, `msg` a string literal (C++ refuses any other at open).
fn assert_true_call(nodes: &[TExprNode], i: usize) -> bool {
    let Some((args, to)) = signature(&nodes[i]) else { return false };
    let msg = i + 1 + subtree_len(nodes, i + 1);
    args.len() == 2 && args[0].ptype == T::BOOLEAN && to.ptype == T::BOOLEAN && nodes.get(msg).is_some_and(|m| m.node_type == N::STRING_LITERAL)
}

fn describe(n: &TExprNode) -> String {
    match function_name(n) {
        Some(f) if matches!(n.node_type, N::FUNCTION_CALL | N::COMPUTE_FUNCTION_CALL | N::AGG_EXPR | N::ARITHMETIC_EXPR) => {
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
        (N::COMPUTE_FUNCTION_CALL, "COMPUTE_FUNCTION_CALL"),
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
    for (i, n) in e.nodes.iter().enumerate() {
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
            | N::IN_PRED
            | N::ARRAY_LITERAL => true,
            N::BINARY_PRED => comparison(n).is_some() || n.opcode == Some(TExprOpcode::EQ_FOR_NULL),
            N::COMPOUND_PRED => matches!(
                n.opcode,
                Some(
                    TExprOpcode::COMPOUND_AND
                        | TExprOpcode::COMPOUND_OR
                        | TExprOpcode::COMPOUND_NOT
                )
            ),
            N::ARITHMETIC_EXPR => arithmetic_call(n) || bit_call(n),
            // the FE sends the searched form; `has_case_expr` is ignored, as C++ ignores it
            N::CASE_EXPR => n.case_expr.is_some(),
            N::FUNCTION_CALL => match function_name(n).as_deref() {
                Some("substring" | "is_null_pred" | "is_not_null_pred") => true,
                Some("nullable" | "non_nullable") => signature(n).is_some_and(|(a, to)| a.len() == 1 && a[0].ptype == to.ptype),
                Some("abs") => arithmetic_call(n),
                Some("if" | "ifnull" | "coalesce") => conditional_call(n),
                Some("like" | "concat" | "length") => string_call(n),
                Some("round" | "truncate" | "floor" | "dfloor" | "ceil" | "dceil") => round_call(&e.nodes, i),
                Some("to_date") => signature(n).is_some_and(|(a, to)| a.len() == 1 && a[0].ptype == T::DATETIMEV2 && to.ptype == T::DATEV2),
                Some("concat_ws" | "regexp" | "rlike" | "split_by_string") => string_call(n),
                Some("assert_true") => assert_true_call(&e.nodes, i),
                Some("now" | "current_date" | "curdate") => clock_call(n),
                Some(f) if BIT_FUNCTIONS.contains(&f) => bit_call(n),
                Some(f) if crate::datetime::is_date_function(f) => date_call(n),
                Some(f) if crate::geo::is_geo_function(f) => geo_call(n),
                Some(f) if crate::math::is_math_function(f) => math_call(n),
                Some(f) if crate::strfn::is_string_function(f) => strfn_call(n),
                _ => false,
            },
            // `x ± INTERVAL n UNIT` (TimestampArithmeticExpr): C++ builds the VectorizedFnCall
            // its `fn` names, ignoring the node type and opcode
            N::COMPUTE_FUNCTION_CALL => function_name(n).is_some_and(|f| interval_function(&f)) && date_call(n),
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
            // an ARRAY is only carried: the expression itself, or a table function's argument;
            // no comparison, cast or other function takes one
            Some(c) if c.supported_array() && array_carried(&e.nodes, i) => {}
            Some(c) if c.ptype == T::ARRAY => {
                return Err(ExecError::Unsupported {
                    what: format!("an ARRAY value as an operand ({})", describe(&e.nodes[parent(&e.nodes, i).unwrap_or(i)])),
                })
            }
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
    // C++ opens every expression: its all-constant subtrees are evaluated now
    check_constants(e)
}

/// Evaluates every all-constant call subtree once, as C++ does at open
/// (`VExpr::get_const_col`, vexpr.cpp:819-845), so a coded error (a date out of range, an
/// overflow) fails the fragment at prepare. Other errors are left for the run; the clock
/// functions are skipped (no query clock exists yet).
pub fn check_constants(e: &TExpr) -> Result<(), ExecError> {
    let literal = |n: &TExprNode| {
        matches!(
            n.node_type,
            N::INT_LITERAL | N::LARGE_INT_LITERAL | N::STRING_LITERAL | N::BOOL_LITERAL | N::FLOAT_LITERAL | N::DECIMAL_LITERAL | N::DATE_LITERAL | N::NULL_LITERAL
        )
    };
    let call = |n: &TExprNode| matches!(n.node_type, N::FUNCTION_CALL | N::COMPUTE_FUNCTION_CALL | N::ARITHMETIC_EXPR | N::CAST_EXPR);
    let clock = |n: &TExprNode| matches!(function_name(n).as_deref(), Some("now" | "current_date" | "curdate"));
    // a regexp's constant pattern is compiled at open
    for (i, n) in e.nodes.iter().enumerate() {
        if n.node_type == N::FUNCTION_CALL && matches!(function_name(n).as_deref(), Some("regexp" | "rlike")) {
            let pattern = i + 1 + subtree_len(&e.nodes, i + 1);
            if let Some(p) = e.nodes.get(pattern).filter(|p| p.node_type == N::STRING_LITERAL).and_then(|p| p.string_literal.as_ref()) {
                crate::regexp::check_constant_pattern(p.value.as_bytes(), crate::regexp::options())?;
            }
        }
    }
    let mut i = 0;
    while i < e.nodes.len() {
        let len = subtree_len(&e.nodes, i);
        let Some(sub) = e.nodes.get(i..i + len) else { break };
        let constant = call(&sub[0]) && sub[1..].iter().all(|n| literal(n) || call(n)) && sub.iter().any(literal) && !sub.iter().any(clock);
        if !constant {
            i += 1;
            continue;
        }
        let one = Block { types: vec![], rows: vec![vec![]] };
        match eval_column(&TExpr { nodes: sub.to_vec() }, &one, &SlotMap::default()) {
            Err(err @ ExecError::Coded { .. }) => return Err(err),
            _ => i += len,
        }
    }
    Ok(())
}

/// The index of node `i`'s parent (pre-order, `num_children` each).
fn parent(nodes: &[TExprNode], i: usize) -> Option<usize> {
    // (node, children not yet seen) for each open node
    let mut stack: Vec<(usize, i32)> = Vec::new();
    for (j, n) in nodes.iter().enumerate() {
        while stack.last().is_some_and(|(_, left)| *left == 0) {
            stack.pop();
        }
        if j == i {
            return stack.last().map(|(p, _)| *p);
        }
        if let Some((_, left)) = stack.last_mut() {
            *left -= 1;
        }
        if n.num_children > 0 {
            stack.push((j, n.num_children));
        }
    }
    None
}

/// The table functions, whose arguments may be ARRAYs.
pub const TABLE_FUNCTIONS: &[&str] = &["explode", "explode_numbers"];

/// Whether an ARRAY at node `i` is carried, not operated on: the whole expression, or an
/// argument of a table function.
fn array_carried(nodes: &[TExprNode], i: usize) -> bool {
    match parent(nodes, i) {
        None => true,
        Some(p) => function_name(&nodes[p]).is_some_and(|f| TABLE_FUNCTIONS.contains(&f.as_str())),
    }
}

fn bad(msg: impl Into<String>) -> ExecError {
    ExecError::BadRequest(msg.into())
}

/// A stored value as the expression value M4's casts take (an ARRAY, which no cast takes, as
/// NULL: `check_nodes` refuses an array operand).
pub fn datum_value(d: &Datum, t: &ColType) -> Value {
    match d {
        Datum::Null | Datum::Array(_) => Value::Null,
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
        Datum::Null | Datum::Array(_) => None,
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
    // whether each argument is a constant expression (no slot read), as `is_col_constant`
    let mut constant = Vec::with_capacity(kids.capacity());
    for _ in 0..n.num_children.max(0) {
        let start = *pos;
        kids.push(eval_at(nodes, pos, block, slots)?);
        constant.push(nodes[start..*pos].iter().all(|k| k.node_type != N::SLOT_REF));
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
        // the elements, in the element type (`VArrayLiteral`)
        N::ARRAY_LITERAL => {
            let to = node_type(n)?;
            let elem = to.element().ok_or_else(|| bad("an ARRAY_LITERAL that is not an ARRAY"))?;
            let cols = kids
                .iter()
                .map(|(t, v)| v.iter().map(|d| coerce(d.clone(), t, &elem)).collect::<Result<Vec<_>, _>>())
                .collect::<Result<Vec<_>, _>>()?;
            let out = (0..rows).map(|r| Datum::Array(cols.iter().map(|c| c[r].clone()).collect())).collect();
            (to, out)
        }
        N::CASE_EXPR => {
            let c = n.case_expr.as_ref().ok_or_else(|| bad("CASE without its parameters"))?;
            let mut to = node_type(n)?;
            to.nullable = n.is_nullable != Some(false);
            let pairs = kids.len() - usize::from(c.has_else_expr);
            if pairs % 2 != 0 || (c.has_else_expr && kids.is_empty()) {
                return Err(bad("CASE with a WHEN and no THEN"));
            }
            let as_result = |(t, v): &(ColType, Vec<Datum>)| v.iter().map(|d| coerce(d.clone(), t, &to)).collect::<Result<Vec<_>, _>>();
            // the WHENs as they are, the THENs and ELSE in the result type
            let whens: Vec<&[Datum]> = (0..pairs / 2).map(|i| kids[2 * i].1.as_slice()).collect();
            let thens = (0..pairs / 2).map(|i| as_result(&kids[2 * i + 1])).collect::<Result<Vec<_>, _>>()?;
            let else_ = if c.has_else_expr { Some(as_result(&kids[pairs])?) } else { None };
            let float_sum = !to.nullable && matches!(to.ptype, T::FLOAT | T::DOUBLE);
            let thens: Vec<&[Datum]> = thens.iter().map(Vec::as_slice).collect();
            let out = crate::cond::case_(&whens, &thens, else_.as_deref(), float_sum);
            (to, out)
        }
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
                    if let Some(r) = float_to_number(d, &to) {
                        return Ok(r);
                    }
                    if let Some(r) = number_to_bool(d, &from, &to) {
                        return Ok(r);
                    }
                    if let Some(r) = date_to_number(d, &from, &to) {
                        return Ok(r);
                    }
                    // a FLOAT as text is the float's own shortest text, not its double's
                    if let (Datum::Float(_), T::CHAR | T::VARCHAR | T::STRING) = (d, to.ptype) {
                        return Ok(crate::row_text::cell_text(d, &from).map_or(Datum::Null, Datum::Bytes));
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
        // `a <=> b`: both NULL is true, one NULL false, never NULL (`eq_for_null`)
        N::BINARY_PRED if n.opcode == Some(TExprOpcode::EQ_FOR_NULL) => {
            let [(ta, a), (tb, b)] = <[_; 2]>::try_from(kids).map_err(|_| bad("<=> needs two arguments"))?;
            refuse_mixed(&ta, &tb)?;
            let out = a
                .iter()
                .zip(&b)
                .map(|(x, y)| match (x, y) {
                    (Datum::Null, Datum::Null) => Datum::Bool(true),
                    (Datum::Null, _) | (_, Datum::Null) => Datum::Bool(false),
                    _ => Datum::Bool(compare(x, &ta, y, &tb) == Some(Ordering::Equal)),
                })
                .collect();
            (ColType { nullable: false, ..boolean() }, out)
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
        N::FUNCTION_CALL | N::ARITHMETIC_EXPR if function_name(n).is_some_and(|f| BIT_FUNCTIONS.contains(&f.as_str())) => {
            let to = node_type(n)?;
            let types: Vec<ColType> = kids.iter().map(|(t, _)| t.clone()).collect();
            let args: Vec<&[Datum]> = kids.iter().map(|(_, v)| v.as_slice()).collect();
            let out = crate::bits::eval(&function_name(n).unwrap_or_default(), &types, &args, rows)?;
            (to, out)
        }
        N::FUNCTION_CALL if function_name(n).is_some_and(|f| crate::strfn::is_string_function(&f)) => {
            let args: Vec<&[Datum]> = kids.iter().map(|(_, v)| v.as_slice()).collect();
            (node_type(n)?, crate::strfn::eval(&function_name(n).unwrap_or_default(), &args, rows)?)
        }
        N::FUNCTION_CALL if function_name(n).is_some_and(|f| crate::math::is_math_function(&f)) => {
            let types: Vec<ColType> = kids.iter().map(|(t, _)| t.clone()).collect();
            let args: Vec<&[Datum]> = kids.iter().map(|(_, v)| v.as_slice()).collect();
            (node_type(n)?, crate::math::eval(&function_name(n).unwrap_or_default(), &types, &args, rows)?)
        }
        N::FUNCTION_CALL if function_name(n).is_some_and(|f| crate::geo::is_geo_function(&f)) => {
            let args: Vec<&[Datum]> = kids.iter().map(|(_, v)| v.as_slice()).collect();
            (node_type(n)?, crate::geo::eval(&function_name(n).unwrap_or_default(), &args, rows))
        }
        N::FUNCTION_CALL | N::COMPUTE_FUNCTION_CALL if function_name(n).is_some_and(|f| crate::datetime::is_date_function(&f)) => {
            let to = node_type(n)?;
            let types: Vec<ColType> = kids.iter().map(|(t, _)| t.clone()).collect();
            let args: Vec<&[Datum]> = kids.iter().map(|(_, v)| v.as_slice()).collect();
            let out = crate::datetime::eval(&function_name(n).unwrap_or_default(), &types, &to, &args, rows)?;
            (to, out)
        }
        N::ARITHMETIC_EXPR => {
            let to = node_type(n)?;
            let op = function_name(n).ok_or_else(|| ExecError::Unsupported { what: describe(n) })?;
            let [(ta, a), (tb, b)] = <[_; 2]>::try_from(kids).map_err(|_| bad(format!("{op} needs two arguments")))?;
            let out = crate::arith::arithmetic(&op, (&ta, &a), (&tb, &b), &to)?;
            (to, out)
        }
        N::FUNCTION_CALL => match function_name(n).as_deref() {
            Some("abs") => {
                let to = node_type(n)?;
                let (ta, a) = kids.first().ok_or_else(|| bad("abs without its argument"))?;
                let out = crate::arith::abs((ta, a), &to)?;
                (to, out)
            }
            Some(f @ ("now" | "current_date" | "curdate")) => {
                let to = node_type(n)?;
                let clock = crate::clock::current().ok_or_else(|| ExecError::Internal(format!("{f}() without the query's clock")))?;
                let out = if f == "now" {
                    // now(n) row by row; a NULL n is a NULL row (the default NULL handling)
                    (0..rows)
                        .map(|r| match kids.first().map(|(_, v)| &v[r]) {
                            None => Datum::Int(clock.now(0).into()),
                            Some(Datum::Int(s)) => Datum::Int(clock.now(*s as i32).into()),
                            Some(_) => Datum::Null,
                        })
                        .collect()
                } else {
                    vec![Datum::Int(clock.today().into()); rows]
                };
                (to, out)
            }
            Some(name) if crate::round::Mode::of(name).is_some() && !constant.get(1).copied().unwrap_or(true) => {
                // a column scale: each row's own, checked on every row first (round.h:519-534)
                let to = node_type(n)?;
                let mode = crate::round::Mode::of(name).unwrap_or(crate::round::Mode::Round);
                let (tx, x) = kids.first().ok_or_else(|| bad("truncate without its argument"))?;
                let (_, ds) = kids.get(1).ok_or_else(|| bad("truncate without its scale"))?;
                for d in ds {
                    if let Datum::Int(d) = d {
                        if *d > i128::from(i16::MAX) || *d < i128::from(i16::MIN) {
                            return Err(ExecError::Coded {
                                code: -218,
                                name: "E-218",
                                msg: format!("Scale argument for function is out of bound: {d}"),
                            });
                        }
                    }
                }
                let out = x
                    .iter()
                    .zip(ds)
                    .map(|(v, d)| match (v, d) {
                        (Datum::Null, _) | (_, Datum::Null) => Datum::Null,
                        (Datum::Double(f), Datum::Int(d)) => Datum::Double(crate::round::round_double_mode(*f, *d as i32, mode)),
                        (Datum::Int(r), Datum::Int(d)) => Datum::Int(crate::round::round_decimal_mode(*r, tx.scale, *d as i32, to.scale, mode)),
                        (other, _) => other.clone(),
                    })
                    .collect();
                (to, out)
            }
            Some(name) if crate::round::Mode::of(name).is_some() => {
                let mode = crate::round::Mode::of(name).unwrap_or(crate::round::Mode::Round);
                let to = node_type(n)?;
                let (tx, x) = kids.first().ok_or_else(|| bad("round without its argument"))?;
                let d = match kids.get(1).map(|(_, d)| d.first()) {
                    None | Some(None) => Some(0),
                    Some(Some(Datum::Int(d))) => Some(*d),
                    Some(Some(_)) => None,
                };
                match d {
                    _ if rows == 0 => (to, vec![]),
                    // a NULL digits argument: every row NULL
                    None => (to, vec![Datum::Null; rows]),
                    Some(d) if d > i128::from(i16::MAX) || d < i128::from(i16::MIN) => {
                        return Err(ExecError::Coded {
                            code: doris_thrift::gen::status::TStatusCode::INVALID_ARGUMENT.0,
                            name: "INVALID_ARGUMENT",
                            msg: format!("Scale argument for function {name} is out of bound: {d}"),
                        })
                    }
                    Some(d) => {
                        let d = d as i32;
                        let out = x
                            .iter()
                            .map(|v| match v {
                                Datum::Double(f) => Datum::Double(crate::round::round_double_mode(*f, d, mode)),
                                Datum::Int(r) => Datum::Int(crate::round::round_decimal_mode(*r, tx.scale, d, to.scale, mode)),
                                other => other.clone(),
                            })
                            .collect();
                        (to, out)
                    }
                }
            }
            Some("like") => {
                let (_, values) = kids.first().ok_or_else(|| bad("like without its value"))?;
                let (_, patterns) = kids.get(1).ok_or_else(|| bad("like without its pattern"))?;
                // a non-constant ESCAPE is ignored (`FunctionLike::open`)
                let escape = match (kids.get(2), constant.get(2)) {
                    _ if rows == 0 => None,
                    (Some((_, e)), Some(true)) => Some(crate::strings::escape_char(match e.first() {
                        Some(Datum::Bytes(b)) => b,
                        _ => &[],
                    })?),
                    _ => None,
                };
                let out = crate::strings::like_column(values, patterns, constant.get(1) == Some(&true), escape)?;
                (boolean(), out)
            }
            Some("concat") => {
                let t = node_type(n)?;
                let args: Vec<(&ColType, &[Datum])> = kids.iter().map(|(t, v)| (t, v.as_slice())).collect();
                (t, crate::strings::concat_column(&args))
            }
            // the date bits of a DATETIMEV2 (`v >> 37`)
            Some("to_date") => {
                let (_, v) = kids.first().ok_or_else(|| bad("to_date without its argument"))?;
                let out = v.iter().map(|d| if let Datum::Int(x) = d { Datum::Int(x >> 37) } else { Datum::Null }).collect();
                (node_type(n)?, out)
            }
            Some("regexp" | "rlike") => {
                let (_, values) = kids.first().ok_or_else(|| bad("regexp without its value"))?;
                let (_, patterns) = kids.get(1).ok_or_else(|| bad("regexp without its pattern"))?;
                let out = crate::regexp::regexp_column(values, patterns, constant.get(1) == Some(&true), crate::regexp::options())?;
                (boolean(), out)
            }
            Some("concat_ws") => {
                let t = node_type(n)?;
                let args: Vec<&[Datum]> = kids.iter().map(|(_, v)| v.as_slice()).collect();
                (t, crate::strings::concat_ws_column(&args))
            }
            // any NULL or false row fails the query (`function_assert_true.cpp`)
            Some("assert_true") => {
                let (_, c) = kids.first().ok_or_else(|| bad("assert_true without its condition"))?;
                if c.iter().any(|d| truth(d) != Some(true)) {
                    let msg = match kids.get(1).and_then(|(_, m)| m.first()) {
                        Some(Datum::Bytes(m)) => String::from_utf8_lossy(m).into_owned(),
                        _ => String::new(),
                    };
                    return Err(ExecError::Coded {
                        code: doris_thrift::gen::status::TStatusCode::INVALID_ARGUMENT.0,
                        name: "INVALID_ARGUMENT",
                        msg: format!("[E33] {msg}"),
                    });
                }
                (boolean(), vec![Datum::Bool(true); rows])
            }
            Some("split_by_string") => {
                let (_, v) = kids.first().ok_or_else(|| bad("split_by_string without its value"))?;
                let (_, s) = kids.get(1).ok_or_else(|| bad("split_by_string without its separator"))?;
                (node_type(n)?, crate::strings::split_column(v, s))
            }
            Some("length") => {
                let (_, v) = kids.first().ok_or_else(|| bad("length without its argument"))?;
                (node_type(n)?, crate::strings::length_column(v))
            }
            // `nullable(x)` only marks the type nullable (`function_nullables.cpp:36-66`)
            Some("nullable") => {
                let (t, v) = kids.into_iter().next().ok_or_else(|| bad("nullable without its argument"))?;
                (ColType { nullable: true, ..t }, v)
            }
            // `non_nullable(x)`: a NULL fails the query (`function_nullables.cpp:69-104`)
            Some("non_nullable") => {
                let (t, v) = kids.into_iter().next().ok_or_else(|| bad("non_nullable without its argument"))?;
                if v.contains(&Datum::Null) {
                    return Err(ExecError::Coded {
                        code: doris_thrift::gen::status::TStatusCode::INVALID_ARGUMENT.0,
                        name: "INVALID_ARGUMENT",
                        msg: format!("There's NULL value in column {} which is illegal for non_nullable", t.name),
                    });
                }
                (ColType { nullable: false, ..t }, v)
            }
            Some(f @ ("if" | "ifnull" | "coalesce")) => {
                let to = node_type(n)?;
                let values = if f == "if" { kids.get(1..).unwrap_or_default() } else { &kids[..] };
                let values = values
                    .iter()
                    .map(|(t, v)| v.iter().map(|d| coerce(d.clone(), t, &to)).collect::<Result<Vec<_>, _>>())
                    .collect::<Result<Vec<_>, _>>()?;
                let out = match (f, &values[..]) {
                    ("if", [then, else_]) => {
                        let (_, c) = kids.first().ok_or_else(|| bad("if without its condition"))?;
                        crate::cond::if_(c, then, else_)
                    }
                    ("if", _) => return Err(bad("if needs three arguments")),
                    _ => crate::cond::coalesce(&values.iter().map(Vec::as_slice).collect::<Vec<_>>()),
                };
                (to, out)
            }
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
        // an ARRAY never reaches a comparison: `check_nodes` refuses it
        Datum::Null | Datum::Array(_) => return None,
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

/// A DATEV2 or DATETIMEV2 cast to a number: `YYYYMMDD` or `YYYYMMDDhhmmss`, its `to_int64()`
/// (`cast_to_basic_number_common.h:358-362`).
fn date_to_number(d: &Datum, from: &ColType, to: &ColType) -> Option<Datum> {
    let datetime = match from.ptype {
        T::DATEV2 => false,
        T::DATETIMEV2 => true,
        _ => return None,
    };
    let Datum::Int(v) = d else { return None };
    let t = crate::datetime::Dt::unpack(*v, datetime);
    let mut n = (t.year * 100 + t.month) * 100 + t.day;
    if datetime {
        n = ((n * 100 + t.hour) * 100 + t.minute) * 100 + t.second;
    }
    Some(match to.ptype {
        T::DOUBLE => Datum::Double(n as f64),
        T::BIGINT | T::LARGEINT => Datum::Int(i128::from(n)),
        _ => return None,
    })
}

/// A number cast to BOOLEAN, as `cast_to_boolean.h` does it: true unless the value is zero (for a
/// decimal, its unscaled value).
fn number_to_bool(d: &Datum, from: &ColType, to: &ColType) -> Option<Datum> {
    if to.ptype != T::BOOLEAN {
        return None;
    }
    let numeric = matches!(
        from.ptype,
        T::TINYINT | T::SMALLINT | T::INT | T::BIGINT | T::LARGEINT | T::FLOAT | T::DOUBLE | T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I | T::DECIMAL256
    );
    if !numeric {
        return None;
    }
    Some(match d {
        Datum::Int(v) => Datum::Bool(*v != 0),
        Datum::Float(f) => Datum::Bool(*f != 0.0),
        Datum::Double(f) => Datum::Bool(*f != 0.0),
        _ => return None,
    })
}

/// A FLOAT or DOUBLE cast to an integer or a decimal, numerically (the non-strict cast; None
/// for any other cast):
/// - integers: truncated, NULL when not finite or outside the type
///   (`cast_to_basic_number_common.h:185-205`);
/// - decimals: `x * 10^scale` plus ±0.5, truncated; NULL when not finite or at or past the
///   precision's bound (`cast_to_decimal.h:238-262`).
fn float_to_number(d: &Datum, to: &ColType) -> Option<Datum> {
    let x = match d {
        Datum::Float(f) => f64::from(*f),
        Datum::Double(f) => *f,
        _ => return None,
    };
    let bits = match to.ptype {
        T::TINYINT => 8,
        T::SMALLINT => 16,
        T::INT => 32,
        T::BIGINT => 64,
        T::LARGEINT => 128,
        T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I => 0,
        _ => return None,
    };
    if !x.is_finite() {
        return Some(Datum::Null);
    }
    if bits > 0 {
        let t = x.trunc();
        let bound = 2f64.powi(bits - 1);
        return Some(if t < -bound || t >= bound { Datum::Null } else { Datum::Int(t as i128) });
    }
    let max = 10i128.checked_pow(to.precision as u32).map_or(f64::MAX, |m| (m - 1) as f64);
    // the integer multiplier as a double, as C++ converts it (powi can round differently)
    let scaled = x * 10i128.checked_pow(to.scale.max(0) as u32).map_or(f64::INFINITY, |m| m as f64);
    if scaled <= -max || scaled >= max {
        return Some(Datum::Null);
    }
    Some(Datum::Int((scaled + if x >= 0.0 { 0.5 } else { -0.5 }) as i128))
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
