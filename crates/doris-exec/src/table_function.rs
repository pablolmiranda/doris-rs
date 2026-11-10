//! TABLE_FUNCTION_NODE (lateral view), as `table_function_operator.cpp` builds its rows, with
//! `explode` (`vexplode_v2.cpp`) and `explode_numbers` (`vexplode_numbers.cpp`).
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::{TExpr, TExprNodeType as N};
use doris_thrift::gen::plan_nodes::TTableFunctionNode;

use crate::ExecError;

fn unsupported(what: impl Into<String>) -> ExecError {
    ExecError::Unsupported { what: what.into() }
}

/// The function a table function expression calls.
pub fn function_name(e: &TExpr) -> Option<String> {
    let root = e.nodes.first()?;
    (root.node_type == N::FUNCTION_CALL).then(|| root.fn_.as_ref().map(|f| f.name.function_name.to_ascii_lowercase()))?
}

/// The node's function calls, each `explode` or `explode_numbers` over one argument; the
/// `_outer` forms and `expand_conjuncts` (LEFT JOIN LATERAL) are refused.
pub fn check(n: &TTableFunctionNode) -> Result<(), ExecError> {
    if n.expand_conjuncts.as_ref().is_some_and(|c| !c.is_empty()) {
        return Err(unsupported("a TABLE_FUNCTION_NODE with expand_conjuncts"));
    }
    let fns = n.fn_call_expr_list.as_deref().unwrap_or_default();
    if fns.is_empty() {
        return Err(ExecError::BadRequest("a TABLE_FUNCTION_NODE without functions".into()));
    }
    for e in fns {
        let name = function_name(e).ok_or_else(|| ExecError::BadRequest("a table function that is not a call".into()))?;
        let args = e.nodes[0].num_children;
        match name.as_str() {
            "explode" | "explode_numbers" if args == 1 => {}
            "explode" => return Err(unsupported("explode of several arrays (a STRUCT)")),
            other => return Err(unsupported(format!("the table function {other}"))),
        }
        crate::eval::check_read(&TExpr {
            nodes: e.nodes[1..].to_vec(),
        })?;
    }
    Ok(())
}

/// One row's values of table function `name` over its argument `arg`: none for NULL or empty.
pub fn values(name: &str, arg: &Datum) -> Vec<Datum> {
    match (name, arg) {
        ("explode_numbers", Datum::Int(n)) => (0..(*n).max(0)).map(Datum::Int).collect(),
        ("explode", Datum::Array(a)) => a.clone(),
        _ => vec![],
    }
}

/// The node's rows: each child row (its slot copied when `keep`, else `defaults`) once per
/// combination of its functions' values, the first function outermost and the last fastest. A
/// child row with any function empty gives none. `lists[f][r]` is function f's values for row r.
pub fn expand(rows: &[Vec<Datum>], lists: &[Vec<Vec<Datum>>], keep: &[bool], defaults: &[Datum]) -> Vec<Vec<Datum>> {
    let mut out = Vec::new();
    for (r, row) in rows.iter().enumerate() {
        let per_fn: Vec<&Vec<Datum>> = lists.iter().map(|l| &l[r]).collect();
        if per_fn.iter().any(|v| v.is_empty()) {
            continue;
        }
        let base: Vec<Datum> = row
            .iter()
            .enumerate()
            .map(|(i, d)| {
                if keep.get(i).copied().unwrap_or(true) {
                    d.clone()
                } else {
                    defaults[i].clone()
                }
            })
            .collect();
        // an odometer over the functions' values, the last one fastest
        let mut at = vec![0usize; per_fn.len()];
        loop {
            let mut o = base.clone();
            o.extend(at.iter().zip(&per_fn).map(|(i, v)| v[*i].clone()));
            out.push(o);
            let mut f = per_fn.len();
            loop {
                if f == 0 {
                    break;
                }
                f -= 1;
                at[f] += 1;
                if at[f] < per_fn[f].len() {
                    break;
                }
                at[f] = 0;
                if f == 0 {
                    f = usize::MAX;
                    break;
                }
            }
            if f == usize::MAX || per_fn.is_empty() {
                break;
            }
        }
    }
    out
}
