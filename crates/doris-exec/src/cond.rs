//! `if`, `ifnull`, `coalesce` and CASE, row by row as the C++ BE selects them
//! (`be/src/exprs/vcondition_expr.cpp`, `vcase_expr.{h,cpp}`):
//! - `if`: a NULL condition takes the else branch (:369-396);
//! - `ifnull`/`coalesce`: the first argument that is not NULL; NaN is a value (:495-547, :650+);
//! - CASE: the first WHEN that is TRUE; NULL is no match; no match and no ELSE is NULL.
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

fn is_true(c: &Datum) -> bool {
    *c == Datum::Bool(true) || matches!(c, Datum::Int(v) if *v != 0)
}

/// `CASE WHEN w1 THEN t1 … [ELSE e] END` per row (the searched form: the FE sends no other).
/// With `float_sum`, a non-nullable FLOAT or DOUBLE result, C++ computes a row as the sum of
/// selected × value over every branch, ELSE included (`vcase_expr.h:215-266`): a NaN or ±Inf in a
/// branch not taken makes the row NaN, and a selected -0.0 comes out +0.0.
pub fn case_(whens: &[&[Datum]], thens: &[&[Datum]], else_: Option<&[Datum]>, float_sum: bool) -> Vec<Datum> {
    let rows = whens.first().map_or_else(|| else_.map_or(0, <[Datum]>::len), |w| w.len());
    (0..rows)
        .map(|r| {
            let taken = whens.iter().position(|w| is_true(&w[r]));
            if float_sum {
                let branches = thens.iter().map(|t| &t[r]).chain(else_.map(|e| &e[r]));
                let selected = |i: usize| if taken.map_or(i == thens.len(), |t| t == i) { 1.0 } else { 0.0 };
                let mut f32_sum: Option<f32> = None;
                let mut f64_sum: Option<f64> = None;
                for (i, v) in branches.enumerate() {
                    match v {
                        Datum::Float(x) => f32_sum = Some(f32_sum.unwrap_or(0.0) + selected(i) as f32 * x),
                        Datum::Double(x) => f64_sum = Some(f64_sum.unwrap_or(0.0) + selected(i) * x),
                        _ => {}
                    }
                }
                return match (f32_sum, f64_sum) {
                    (Some(x), _) => Datum::Float(x),
                    (_, Some(x)) => Datum::Double(x),
                    _ => Datum::Null,
                };
            }
            match taken {
                Some(i) => thens[i][r].clone(),
                None => else_.map_or(Datum::Null, |e| e[r].clone()),
            }
        })
        .collect()
}
