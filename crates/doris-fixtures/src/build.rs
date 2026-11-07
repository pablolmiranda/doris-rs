//! Builders for hand-made plan pieces in tests: types, expressions and functions. The generated
//! Thrift structs have no `Default`, so each blank value is spelled out once here.
use doris_thrift::gen::exprs::{
    TAggregateExpr, TBoolLiteral, TDateLiteral, TDecimalLiteral, TExpr, TExprNode,
    TExprNodeType as N, TFloatLiteral, TInPredicate, TIntLiteral, TSlotRef, TStringLiteral,
};
use doris_thrift::gen::opcodes::TExprOpcode;
use doris_thrift::gen::types::{
    TFunction, TFunctionBinaryType, TFunctionName, TPrimitiveType, TScalarType, TTypeDesc,
    TTypeNode, TTypeNodeType,
};
use thrift::OrderedFloat;

/// A scalar type with its length, precision and scale (-1/0 when they do not apply).
pub fn scalar(p: TPrimitiveType, len: i32, precision: i32, scale: i32) -> TTypeDesc {
    let s = TScalarType::new(p, Some(len), Some(precision), Some(scale), None, None, None);
    TTypeDesc::new(
        Some(vec![TTypeNode::new(
            TTypeNodeType::SCALAR,
            Some(s),
            None,
            None,
            None,
        )]),
        None,
        None,
        None,
        None,
        None,
        None,
    )
}

pub fn ty(p: TPrimitiveType) -> TTypeDesc {
    scalar(p, -1, 0, 0)
}

pub fn blank(node_type: N, type_: TTypeDesc, num_children: i32) -> TExprNode {
    TExprNode {
        node_type,
        type_,
        opcode: None,
        num_children,
        agg_expr: None,
        bool_literal: None,
        case_expr: None,
        date_literal: None,
        float_literal: None,
        int_literal: None,
        in_predicate: None,
        is_null_pred: None,
        like_pred: None,
        literal_pred: None,
        slot_ref: None,
        string_literal: None,
        tuple_is_null_pred: None,
        info_func: None,
        decimal_literal: None,
        output_scale: -1,
        fn_call_expr: None,
        large_int_literal: None,
        output_column: None,
        output_type: None,
        vector_opcode: None,
        fn_: None,
        vararg_start_idx: None,
        child_type: None,
        is_nullable: None,
        json_literal: None,
        schema_change_expr: None,
        column_ref: None,
        match_predicate: None,
        ipv4_literal: None,
        ipv6_literal: None,
        label: None,
        timev2_literal: None,
        varbinary_literal: None,
        is_cast_nullable: None,
        search_param: None,
        short_circuit_evaluation: None,
        lambda_argument_names: None,
    }
}

pub fn function(name: &str, args: Vec<TTypeDesc>, ret: TTypeDesc) -> TFunction {
    TFunction::new(
        TFunctionName::new(None, name.to_string()),
        TFunctionBinaryType::BUILTIN,
        args,
        ret,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
}

fn leaf(n: TExprNode) -> TExpr {
    TExpr::new(vec![n])
}

/// A node over child expressions, in pre-order.
pub fn over(mut n: TExprNode, children: Vec<TExpr>) -> TExpr {
    n.num_children = children.len() as i32;
    let mut nodes = vec![n];
    for c in children {
        nodes.extend(c.nodes);
    }
    TExpr::new(nodes)
}

pub fn slot(id: i32, t: TTypeDesc) -> TExpr {
    let mut n = blank(N::SLOT_REF, t, 0);
    n.slot_ref = Some(TSlotRef::new(id, 0, None, None));
    leaf(n)
}

pub fn int(v: i64, t: TTypeDesc) -> TExpr {
    let mut n = blank(N::INT_LITERAL, t, 0);
    n.int_literal = Some(TIntLiteral::new(v));
    leaf(n)
}

pub fn string(s: &str) -> TExpr {
    let mut n = blank(N::STRING_LITERAL, ty(TPrimitiveType::STRING), 0);
    n.string_literal = Some(TStringLiteral::new(s.to_string()));
    leaf(n)
}

pub fn decimal(text: &str, t: TTypeDesc) -> TExpr {
    let mut n = blank(N::DECIMAL_LITERAL, t, 0);
    n.decimal_literal = Some(TDecimalLiteral::new(text.to_string()));
    leaf(n)
}

pub fn date(text: &str, t: TTypeDesc) -> TExpr {
    let mut n = blank(N::DATE_LITERAL, t, 0);
    n.date_literal = Some(TDateLiteral::new(text.to_string()));
    leaf(n)
}

pub fn float(v: f64, t: TTypeDesc) -> TExpr {
    let mut n = blank(N::FLOAT_LITERAL, t, 0);
    n.float_literal = Some(TFloatLiteral::new(OrderedFloat(v)));
    leaf(n)
}

pub fn boolean(v: bool) -> TExpr {
    let mut n = blank(N::BOOL_LITERAL, ty(TPrimitiveType::BOOLEAN), 0);
    n.bool_literal = Some(TBoolLiteral::new(v));
    leaf(n)
}

pub fn null(t: TTypeDesc) -> TExpr {
    leaf(blank(N::NULL_LITERAL, t, 0))
}

fn opcode(op: &str) -> TExprOpcode {
    match op {
        "eq" => TExprOpcode::EQ,
        "ne" => TExprOpcode::NE,
        "lt" => TExprOpcode::LT,
        "le" => TExprOpcode::LE,
        "gt" => TExprOpcode::GT,
        "ge" => TExprOpcode::GE,
        "and" => TExprOpcode::COMPOUND_AND,
        "or" => TExprOpcode::COMPOUND_OR,
        "not" => TExprOpcode::COMPOUND_NOT,
        other => panic!("no opcode {other}"),
    }
}

fn first_type(e: &TExpr) -> TTypeDesc {
    e.nodes[0].type_.clone()
}

/// `l <op> r`, a BINARY_PRED as the FE sends it (opcode and function).
pub fn binary(op: &str, l: TExpr, r: TExpr) -> TExpr {
    let boolean = ty(TPrimitiveType::BOOLEAN);
    let mut n = blank(N::BINARY_PRED, boolean.clone(), 2);
    n.opcode = Some(opcode(op));
    n.fn_ = Some(function(op, vec![first_type(&l), first_type(&r)], boolean));
    over(n, vec![l, r])
}

/// `and`, `or` (two children) or `not` (one).
pub fn compound(op: &str, children: Vec<TExpr>) -> TExpr {
    let mut n = blank(N::COMPOUND_PRED, ty(TPrimitiveType::BOOLEAN), 0);
    n.opcode = Some(opcode(op));
    over(n, children)
}

/// `value [NOT] IN (list…)`.
pub fn in_list(value: TExpr, list: Vec<TExpr>, not: bool) -> TExpr {
    let mut n = blank(N::IN_PRED, ty(TPrimitiveType::BOOLEAN), 0);
    n.opcode = Some(if not {
        TExprOpcode::FILTER_NOT_IN
    } else {
        TExprOpcode::FILTER_IN
    });
    n.in_predicate = Some(TInPredicate::new(not));
    let mut children = vec![value];
    children.extend(list);
    over(n, children)
}

/// A FUNCTION_CALL of a builtin.
pub fn call(name: &str, ret: TTypeDesc, args: Vec<TExpr>) -> TExpr {
    let mut n = blank(N::FUNCTION_CALL, ret.clone(), 0);
    n.fn_ = Some(function(name, args.iter().map(first_type).collect(), ret));
    over(n, args)
}

/// An AGG_EXPR call (`is_merge` for the second phase, whose argument is the serialized state).
pub fn aggregate(
    name: &str,
    ret: TTypeDesc,
    param_types: Vec<TTypeDesc>,
    is_merge: bool,
    args: Vec<TExpr>,
) -> TExpr {
    let mut n = blank(N::AGG_EXPR, ret.clone(), 0);
    n.fn_ = Some(function(name, param_types.clone(), ret));
    n.agg_expr = Some(TAggregateExpr::new(is_merge, Some(param_types)));
    over(n, args)
}
