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
/// `ARRAY<elem>`, as the FE describes it: an ARRAY node, then the element's.
pub fn array_of(elem: TTypeDesc) -> TTypeDesc {
    let mut nodes = vec![doris_thrift::gen::types::TTypeNode::new(doris_thrift::gen::types::TTypeNodeType::ARRAY, None, None, Some(true), None)];
    nodes.extend(elem.types.unwrap_or_default());
    TTypeDesc { types: Some(nodes), ..elem }
}

/// `MAP<key, value>`: a MAP node (`contains_nulls` for the key and the value), then the key's
/// nodes, then the value's.
pub fn map_of(key: TTypeDesc, value: TTypeDesc) -> TTypeDesc {
    let mut nodes = vec![TTypeNode::new(TTypeNodeType::MAP, None, None, None, Some(vec![true, true]))];
    nodes.extend(key.types.unwrap_or_default());
    nodes.extend(value.types.clone().unwrap_or_default());
    TTypeDesc { types: Some(nodes), ..value }
}

/// `STRUCT<name type, ..>`: a STRUCT node with its fields (name, nullable), then each field's nodes.
pub fn struct_of(fields: Vec<(&str, TTypeDesc, bool)>) -> TTypeDesc {
    let names = fields
        .iter()
        .map(|(n, _, null)| doris_thrift::gen::types::TStructField::new(n.to_string(), None, Some(*null)))
        .collect();
    let mut nodes = vec![TTypeNode::new(TTypeNodeType::STRUCT, None, Some(names), None, None)];
    for (_, t, _) in fields {
        nodes.extend(t.types.unwrap_or_default());
    }
    TTypeDesc { types: Some(nodes), ..ty(TPrimitiveType::INT) }
}

/// An ARRAY_LITERAL of `elems` (each already of the element type).
pub fn array_literal(elem: TTypeDesc, elems: Vec<TExpr>) -> TExpr {
    over(blank(N::ARRAY_LITERAL, array_of(elem), 0), elems)
}

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
        "eq_for_null" => TExprOpcode::EQ_FOR_NULL,
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

// ---- query plans ---------------------------------------------------------------------------

use doris_proto::doris::PExecPlanFragmentRequest;
use doris_thrift::gen::agent_service::{TCreateTabletReq, TTabletSchema};
use doris_thrift::gen::descriptors::{TColumn, TSlotDescriptor};
use doris_thrift::gen::palo_internal_service::{TPipelineFragmentParams, TPipelineFragmentParamsList};
use doris_thrift::gen::types::{TColumnType, TKeysType, TStorageType, TUniqueId};
use thrift::protocol::{TCompactOutputProtocol, TSerializable};

use crate::replays::{self, plan_list, shape, Step};

/// A column of a hand-made table: its name, type and nullability.
#[derive(Debug, Clone)]
pub struct Col {
    pub name: String,
    pub ty: TTypeDesc,
    pub nullable: bool,
}

pub fn col(name: &str, ty: TTypeDesc, nullable: bool) -> Col {
    Col { name: name.to_string(), ty, nullable }
}

/// The corpus query the scan plans are cloned from: a one-fragment OLAP scan into the result
/// sink (the FE's statistics read), with its exec request.
pub fn template() -> (PExecPlanFragmentRequest, TPipelineFragmentParams) {
    for r in replays::all() {
        for s in &r.steps {
            let Step::ExecPlanFragment { request, .. } = s else { continue };
            let Some(list) = plan_list(request) else { continue };
            if shape(&list) == "OLAP_SCAN->RESULT" {
                let p = list.params_list.unwrap().remove(0);
                return (request.clone(), p);
            }
        }
    }
    panic!("no OLAP_SCAN->RESULT query in the replays");
}

/// The thrift column of a hand-made table column; unique ids are positions.
pub fn tcolumn(template: &TColumn, i: usize, c: &Col) -> TColumn {
    let s = c.ty.types.as_ref().unwrap()[0].scalar_type.clone().unwrap();
    TColumn {
        column_name: c.name.clone(),
        column_type: TColumnType::new(s.type_, s.len, None, s.precision, s.scale, None, None),
        aggregation_type: None,
        is_key: Some(i == 0),
        is_allow_null: Some(c.nullable),
        default_value: None,
        col_unique_id: Some(i as i32),
        ..template.clone()
    }
}

/// CREATE for a duplicate-key tablet of these columns, the first one the key.
pub fn create_tablet(tablet_id: i64, columns: Vec<TColumn>, keys: TKeysType, version: i64) -> TCreateTabletReq {
    let short_keys = columns.iter().filter(|c| c.is_key == Some(true)).count().clamp(1, 3) as i16;
    let schema = TTabletSchema::new(
        short_keys,
        0,
        keys,
        TStorageType::COLUMN,
        columns,
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
        None,
        None,
        None,
        None,
    );
    TCreateTabletReq::new(
        tablet_id,
        schema,
        Some(version),
        None,
        None,
        None,
        None,
        None,
        None,
        Some(1),
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
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
}

/// `SELECT <columns> FROM <tablet>` at `version`, one fragment: an OLAP scan into the result
/// sink. Slots 0.. are the columns in order, in tuple 0; the instance id is the query id + 1.
pub fn scan_query(query_id: TUniqueId, tablet_id: i64, version: i64, columns: &[Col], batch_size: i32) -> TPipelineFragmentParams {
    let (_, mut p) = template();
    p.query_id = query_id.clone();
    p.fragment_id = Some(0);
    p.need_wait_execution_trigger = Some(false);
    if let Some(o) = p.query_options.as_mut() {
        o.batch_size = Some(batch_size);
    }
    let desc = p.desc_tbl.as_mut().unwrap();
    let slot0 = desc.slot_descriptors.as_ref().unwrap()[0].clone();
    desc.slot_descriptors = Some(
        columns
            .iter()
            .enumerate()
            .map(|(i, c)| TSlotDescriptor {
                id: i as i32,
                parent: 0,
                slot_type: c.ty.clone(),
                col_name: c.name.clone(),
                col_unique_id: Some(i as i32),
                null_indicator_bit: if c.nullable { 0 } else { -1 },
                primitive_type: c.ty.types.as_ref().and_then(|t| t[0].scalar_type.as_ref()).map(|s| s.type_),
                ..slot0.clone()
            })
            .collect(),
    );
    desc.tuple_descriptors.truncate(1);
    desc.tuple_descriptors[0].id = 0;
    let fragment = p.fragment.as_mut().unwrap();
    let plan = fragment.plan.as_mut().unwrap();
    plan.nodes.truncate(1);
    let node = &mut plan.nodes[0];
    node.node_id = 0;
    node.row_tuples = vec![0];
    node.conjuncts = None;
    node.projections = None;
    node.output_tuple_id = None;
    node.limit = -1;
    let scan = node.olap_scan_node.as_mut().unwrap();
    let col0 = scan.columns_desc.as_ref().unwrap()[0].clone();
    scan.tuple_id = 0;
    scan.key_type = Some(TKeysType::DUP_KEYS);
    scan.enable_unique_key_merge_on_write = Some(false);
    scan.columns_desc = Some(columns.iter().enumerate().map(|(i, c)| tcolumn(&col0, i, c)).collect());
    scan.output_column_unique_ids = Some((0..columns.len() as i32).collect());
    fragment.output_exprs = Some(columns.iter().enumerate().map(|(i, c)| slot(i as i32, c.ty.clone())).collect());
    let mut local = p.local_params.as_ref().unwrap()[0].clone();
    local.fragment_instance_id = TUniqueId { hi: query_id.hi, lo: query_id.lo + 1 };
    let mut range = local.per_node_scan_ranges.values().next().unwrap()[0].clone();
    let pr = range.scan_range.palo_scan_range.as_mut().unwrap();
    pr.tablet_id = tablet_id;
    pr.version = version.to_string();
    local.per_node_scan_ranges = [(0, vec![range])].into_iter().collect();
    p.local_params = Some(vec![local]);
    p
}

/// The exec request of a fragment list, as the FE sends it (compact Thrift, version 3).
pub fn exec_request(params: Vec<TPipelineFragmentParams>) -> PExecPlanFragmentRequest {
    let (mut req, _) = template();
    let list = TPipelineFragmentParamsList::new(params, None, None, None, None, None, None, None, None, None, None, None, None, None);
    let mut buf = Vec::new();
    list.write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut buf)).expect("write to a Vec");
    req.request = Some(buf);
    req.compact = Some(true);
    req
}
