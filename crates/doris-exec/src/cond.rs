//! `if`, `ifnull` and `coalesce`, row by row as the C++ BE selects them
//! (`be/src/exprs/vcondition_expr.cpp`):
//! - `if`: a NULL condition takes the else branch (:369-396);
//! - `ifnull`/`coalesce`: the first argument that is not NULL; NaN is a value (:495-547, :650+).
//!
//! The FE gives every branch the call's result type; `eval` coerces a branch that differs.
use doris_segment::writer::Datum;

/// `if(c, then, else)` per row.
pub fn if_(c: &[Datum], then: &[Datum], else_: &[Datum]) -> Vec<Datum> {
    c.iter()
        .zip(then.iter().zip(else_))
        .map(|(c, (t, e))| {
            if *c == Datum::Bool(true) || matches!(c, Datum::Int(v) if *v != 0) {
                t.clone()
            } else {
                e.clone()
            }
        })
        .collect()
}

/// `coalesce(args...)` per row; `ifnull(a, b)` is `coalesce(a, b)`.
pub fn coalesce(args: &[&[Datum]]) -> Vec<Datum> {
    let rows = args.first().map_or(0, |a| a.len());
    (0..rows)
        .map(|r| args.iter().map(|a| &a[r]).find(|d| **d != Datum::Null).cloned().unwrap_or(Datum::Null))
        .collect()
}
