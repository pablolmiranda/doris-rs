//! REPEAT_NODE (`exec/operator/repeat_operator.cpp`): the input once per repeat, its grouping
//! slots outside that repeat's kept set NULL, then one BIGINT per grouping list.
use doris_segment::writer::Datum;
use doris_thrift::gen::plan_nodes::TRepeatNode;

use crate::ExecError;

/// `RepeatOperatorX::init`: every grouping list covers every repeat.
pub fn check_repeat(r: &TRepeatNode) -> Result<(), ExecError> {
    let repeats = r.repeat_id_list.len();
    if r.slot_id_set_list.len() < repeats {
        return Err(ExecError::BadRequest(format!("a REPEAT_NODE with {} kept-slot sets for {repeats} repeats", r.slot_id_set_list.len())));
    }
    for g in &r.grouping_list {
        if g.len() < repeats {
            return Err(ExecError::Internal(format!(
                "grouping_list size {} is less than repeat_id_list size {repeats}",
                g.len()
            )));
        }
    }
    Ok(())
}

/// The output rows: `values` holds each input row's `exprs` values, in the order of `slots` (the
/// output tuple's first slots); `rows` is the input's row count, which a node without `exprs` still
/// emits.
pub fn repeat_rows(r: &TRepeatNode, slots: &[i32], values: &[Vec<Datum>], rows: usize) -> Result<Vec<Vec<Datum>>, ExecError> {
    check_repeat(r)?;
    let mut out = Vec::with_capacity(rows * r.repeat_id_list.len());
    for (k, kept) in r.slot_id_set_list.iter().enumerate().take(r.repeat_id_list.len()) {
        // a grouping slot outside this repeat's kept set is NULL (repeat_operator.cpp:126-153)
        let nulled: Vec<bool> = slots.iter().map(|s| r.all_slot_ids.contains(s) && !kept.contains(s)).collect();
        for row in 0..rows {
            let mut o: Vec<Datum> = match values.get(row) {
                Some(v) => v.iter().zip(&nulled).map(|(d, n)| if *n { Datum::Null } else { d.clone() }).collect(),
                None => vec![],
            };
            o.extend(r.grouping_list.iter().map(|g| Datum::Int(g[k].into())));
            out.push(o);
        }
    }
    Ok(out)
}
