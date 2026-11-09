//! Write fragments: the `exec_plan_fragment` request (`compact=true`, `version=3`, a compact
//! Thrift `TPipelineFragmentParamsList`), and the two plan shapes doris-rs runs:
//! `UNION_NODE` (constant rows) → projections → `output_exprs` → `OLAP_TABLE_SINK` for
//! `INSERT … VALUES`, and `FILE_SCAN_NODE` → `OLAP_TABLE_SINK` for stream load. Any other plan
//! with a table sink (`INSERT … SELECT`) runs in the query pipeline, its sink checked here.
use std::collections::HashMap;

use doris_proto::doris::PExecPlanFragmentRequest;
use doris_thrift::gen::data_sinks::{TDataSinkType, TOlapTableSink};
use doris_thrift::gen::descriptors::{TColumn, TSlotDescriptor};
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::palo_internal_service::{TPipelineFragmentParams, TPipelineFragmentParamsList};
use doris_thrift::gen::plan_nodes::{TPlanNode, TPlanNodeType};
use doris_thrift::gen::types::{TNetworkAddress, TUniqueId};
use thrift::protocol::{TCompactInputProtocol, TSerializable};

use crate::expr::{check, eval, Slots};
use crate::value::{ColType, Value};
use crate::{ExecError, RowError};

/// `PFragmentRequestVersion::VERSION_3`: a `TPipelineFragmentParamsList`.
const VERSION_3: i32 = 3;

pub fn decode_fragments(req: &PExecPlanFragmentRequest) -> Result<TPipelineFragmentParamsList, ExecError> {
    if req.version != Some(VERSION_3) {
        return Err(ExecError::Unsupported { what: format!("fragment request version {:?}", req.version) });
    }
    let bytes = req.request.as_deref().ok_or_else(|| ExecError::BadRequest("no request bytes".into()))?;
    let mut b = bytes;
    let list = if req.compact.unwrap_or(false) {
        TPipelineFragmentParamsList::read_from_in_protocol(&mut TCompactInputProtocol::new(&mut b))
    } else {
        TPipelineFragmentParamsList::read_from_in_protocol(&mut thrift::protocol::TBinaryInputProtocol::new(&mut b, true))
    };
    list.map_err(|e| ExecError::BadRequest(e.to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WritePlan {
    /// `INSERT … VALUES`: the rows are the union node's constant expressions.
    Values,
    /// Stream load: rows come from the request body through the file scan node.
    Stream,
    /// `INSERT … SELECT`: rows come from the fragment's plan, run by the query pipeline.
    Select,
}

/// A write fragment, ready to run.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteFragment {
    pub params: TPipelineFragmentParams,
    pub plan: WritePlan,
    pub sink: TOlapTableSink,
    pub query_id: TUniqueId,
    pub instance_id: TUniqueId,
    pub fragment_id: i32,
    pub coord: Option<TNetworkAddress>,
    pub backend_id: i64,
    /// `enable_insert_strict`: when false, over-long strings are cut instead of filtered. A
    /// stream load sets it from its strict mode.
    pub insert_strict: bool,
    /// `load_zero_tolerance` (`TQueryGlobals`): too many error rows stop the load.
    pub zero_tolerance: bool,
    dest: Vec<ColType>,
}

fn node_name(t: TPlanNodeType) -> String {
    let names = [
        (TPlanNodeType::OLAP_SCAN_NODE, "OLAP_SCAN_NODE"),
        (TPlanNodeType::EXCHANGE_NODE, "EXCHANGE_NODE"),
        (TPlanNodeType::SORT_NODE, "SORT_NODE"),
        (TPlanNodeType::AGGREGATION_NODE, "AGGREGATION_NODE"),
        (TPlanNodeType::HASH_JOIN_NODE, "HASH_JOIN_NODE"),
        (TPlanNodeType::UNION_NODE, "UNION_NODE"),
        (TPlanNodeType::FILE_SCAN_NODE, "FILE_SCAN_NODE"),
        (TPlanNodeType::SELECT_NODE, "SELECT_NODE"),
        (TPlanNodeType::CROSS_JOIN_NODE, "CROSS_JOIN_NODE"),
        (TPlanNodeType::EMPTY_SET_NODE, "EMPTY_SET_NODE"),
        (TPlanNodeType::SCHEMA_SCAN_NODE, "SCHEMA_SCAN_NODE"),
        (TPlanNodeType::ANALYTIC_EVAL_NODE, "ANALYTIC_EVAL_NODE"),
        (TPlanNodeType::TABLE_FUNCTION_NODE, "TABLE_FUNCTION_NODE"),
        (TPlanNodeType::DATA_GEN_SCAN_NODE, "DATA_GEN_SCAN_NODE"),
        (TPlanNodeType::REPEAT_NODE, "REPEAT_NODE"),
        (TPlanNodeType::INTERSECT_NODE, "INTERSECT_NODE"),
        (TPlanNodeType::EXCEPT_NODE, "EXCEPT_NODE"),
        (TPlanNodeType::GROUP_COMMIT_SCAN_NODE, "GROUP_COMMIT_SCAN_NODE"),
    ];
    names.iter().find(|(n, _)| *n == t).map_or_else(|| format!("plan node type {}", t.0), |(_, s)| s.to_string())
}

/// The slots of a tuple, in descriptor table order.
pub(crate) fn tuple_slots(params: &TPipelineFragmentParams, tuple: i32) -> Vec<TSlotDescriptor> {
    params
        .desc_tbl
        .as_ref()
        .and_then(|d| d.slot_descriptors.as_ref())
        .map(|s| s.iter().filter(|s| s.parent == tuple).cloned().collect())
        .unwrap_or_default()
}

/// The base index's columns (the one holding every column), by name.
pub(crate) fn base_columns(sink: &TOlapTableSink) -> HashMap<String, TColumn> {
    sink.schema
        .indexes
        .iter()
        .max_by_key(|i| i.columns_desc.as_ref().map_or(0, Vec::len))
        .and_then(|i| i.columns_desc.clone())
        .unwrap_or_default()
        .into_iter()
        .map(|c| (c.column_name.clone(), c))
        .collect()
}

pub fn plan_write(params: &TPipelineFragmentParams) -> Result<WriteFragment, ExecError> {
    let fragment = params.fragment.as_ref().ok_or_else(|| ExecError::BadRequest("no fragment".into()))?;
    let nodes = fragment.plan.as_ref().map(|p| p.nodes.as_slice()).unwrap_or_default();
    let plan = match nodes {
        [n] if n.node_type == TPlanNodeType::UNION_NODE => WritePlan::Values,
        [n] if n.node_type == TPlanNodeType::FILE_SCAN_NODE => WritePlan::Stream,
        [] => return Err(ExecError::BadRequest("an empty plan".into())),
        _ => {
            let other = nodes.iter().find(|n| !matches!(n.node_type, TPlanNodeType::UNION_NODE | TPlanNodeType::FILE_SCAN_NODE));
            return Err(ExecError::Unsupported {
                what: other.map_or_else(|| "a plan of several nodes".to_string(), |n| node_name(n.node_type)),
            });
        }
    };
    let wf = table_sink(params, plan.clone())?;
    if plan == WritePlan::Values {
        let n = &nodes[0];
        let union = n.union_node.as_ref().ok_or_else(|| ExecError::BadRequest("UNION_NODE without its params".into()))?;
        if !union.result_expr_lists.is_empty() {
            return Err(ExecError::Unsupported { what: "a union with children".into() });
        }
        for e in union.const_expr_lists.iter().flatten().chain(n.projections.iter().flatten()).chain(fragment.output_exprs.iter().flatten()) {
            check(e)?;
        }
    }
    if wf.plan == WritePlan::Stream {
        crate::file_scan::check_stream(&wf)?;
    }
    Ok(wf)
}

/// Whether a request is a write the load service runs: one fragment of one node, constant rows
/// (`INSERT … VALUES`) or a file scan, into an OLAP_TABLE_SINK. Every other plan with a table
/// sink (`INSERT … SELECT`) is a query's.
pub fn is_load_plan(list: &TPipelineFragmentParamsList) -> bool {
    let params = list.params_list.as_deref().unwrap_or_default();
    let [p] = params else { return false };
    let Some(f) = p.fragment.as_ref() else { return false };
    let nodes = f.plan.as_ref().map(|p| p.nodes.as_slice()).unwrap_or_default();
    f.output_sink.as_ref().is_some_and(|s| s.type_ == TDataSinkType::OLAP_TABLE_SINK)
        && matches!(nodes, [n] if matches!(n.node_type, TPlanNodeType::UNION_NODE | TPlanNodeType::FILE_SCAN_NODE))
}

/// A fragment's OLAP_TABLE_SINK, checked: what doris-rs refuses (partial update, group commit,
/// automatic partitions, a column of a type it does not write) is refused naming it.
pub fn table_sink(params: &TPipelineFragmentParams, plan: WritePlan) -> Result<WriteFragment, ExecError> {
    let fragment = params.fragment.as_ref().ok_or_else(|| ExecError::BadRequest("no fragment".into()))?;
    let sink = fragment.output_sink.as_ref().ok_or_else(|| ExecError::BadRequest("no sink".into()))?;
    if sink.type_ != TDataSinkType::OLAP_TABLE_SINK {
        return Err(ExecError::Unsupported { what: format!("data sink type {}", sink.type_.0) });
    }
    let sink = sink.olap_table_sink.clone().ok_or_else(|| ExecError::BadRequest("OLAP_TABLE_SINK without its params".into()))?;
    if sink.schema.is_partial_update == Some(true) {
        return Err(ExecError::Unsupported { what: "partial update".into() });
    }
    if sink.group_commit_mode.is_some() {
        return Err(ExecError::Unsupported { what: "group commit".into() });
    }
    if sink.partition.enable_automatic_partition == Some(true) {
        return Err(ExecError::Unsupported { what: "automatic partitions".into() });
    }
    let base = base_columns(&sink);
    let dest = sink
        .schema
        .slot_descs
        .iter()
        .map(|s| match base.get(&s.col_name) {
            Some(c) => Ok(ColType::from_column(c)),
            None => ColType::from_type_desc(&s.col_name, &s.slot_type, s.null_indicator_bit >= 0)
                .ok_or_else(|| ExecError::Unsupported { what: format!("column {} of a nested type", s.col_name) }),
        })
        .collect::<Result<Vec<_>, _>>()?;
    for c in base.values().map(ColType::from_column).chain(dest.iter().cloned()) {
        if !c.writable() {
            return Err(ExecError::Unsupported { what: format!("column {} of type {}", c.name, c.type_name()) });
        }
    }
    let instance_id = params
        .local_params
        .as_ref()
        .and_then(|l| l.first())
        .map(|l| l.fragment_instance_id.clone())
        .unwrap_or_else(|| params.query_id.clone());
    let wf = WriteFragment {
        params: params.clone(),
        plan,
        query_id: params.query_id.clone(),
        instance_id,
        fragment_id: params.fragment_id.unwrap_or(0),
        coord: params.coord.clone(),
        backend_id: params.backend_id.unwrap_or(0),
        insert_strict: params.query_options.as_ref().and_then(|q| q.enable_insert_strict).unwrap_or(true),
        zero_tolerance: params.query_globals.as_ref().and_then(|g| g.load_zero_tolerance).unwrap_or(false),
        sink,
        dest,
    };
    Ok(wf)
}

impl WriteFragment {
    fn node(&self) -> &TPlanNode {
        &self.params.fragment.as_ref().expect("planned").plan.as_ref().expect("planned").nodes[0]
    }

    fn output_exprs(&self) -> &[TExpr] {
        self.params.fragment.as_ref().and_then(|f| f.output_exprs.as_deref()).unwrap_or_default()
    }

    /// Whether each column of the block reaching the sink has a null map: the output
    /// expression's nullability (a column of literals that are never NULL has none), which
    /// decides the block's bytes.
    pub fn input_nullable(&self) -> Vec<bool> {
        let exprs = self.output_exprs();
        if exprs.is_empty() {
            return self.dest.iter().map(|c| c.nullable).collect();
        }
        exprs.iter().map(|e| e.nodes.first().and_then(|n| n.is_nullable).unwrap_or(true)).collect()
    }

    /// The sink's destination columns, in the order rows reach it.
    pub fn dest_columns(&self) -> &[ColType] {
        &self.dest
    }

    /// The destination rows of a VALUES insert: each constant row through the projections and
    /// the output expressions. A row an expression cannot evaluate is a row error.
    pub fn values_rows(&self) -> Result<Vec<Result<Vec<Value>, RowError>>, ExecError> {
        if self.plan != WritePlan::Values {
            return Err(ExecError::BadRequest("not a VALUES insert".into()));
        }
        let n = self.node();
        let union = n.union_node.as_ref().expect("checked");
        let union_slots: Vec<i32> = tuple_slots(&self.params, union.tuple_id).iter().map(|s| s.id).collect();
        let proj_slots: Vec<i32> = n.output_tuple_id.map(|t| tuple_slots(&self.params, t).iter().map(|s| s.id).collect()).unwrap_or_default();
        Ok(union
            .const_expr_lists
            .iter()
            .map(|row| {
                let mut slots = Slots::new();
                for (id, e) in union_slots.iter().zip(row) {
                    slots.insert(*id, eval(e, &Slots::new())?);
                }
                if let Some(projections) = &n.projections {
                    let mut out = Slots::new();
                    for (id, e) in proj_slots.iter().zip(projections) {
                        out.insert(*id, eval(e, &slots)?);
                    }
                    slots = out;
                }
                self.output_exprs().iter().map(|e| eval(e, &slots)).collect()
            })
            .collect())
    }
}
