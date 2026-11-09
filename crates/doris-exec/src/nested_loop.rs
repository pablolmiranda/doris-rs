//! CROSS_JOIN_NODE (the nested-loop join) and ASSERT_NUM_ROWS_NODE, as the C++ BE runs them
//! (`exec/operator/nested_loop_join_probe_operator.cpp` "NLJP", `assert_num_rows_operator.cpp`):
//! - pairs come out probe-major: each probe row against every build row, in order (NLJP:110-144);
//! - LEFT/FULL OUTER append the probe rows left unmatched after the pairs, and RIGHT/FULL OUTER
//!   the build rows left unmatched after those, each in order (NLJP:153-248): C++'s order when
//!   the probe side is one block whose cross product is under `batch_size`;
//! - only `join_conjuncts` decide, two-valued: NULL fails, except for NULL_AWARE_LEFT_ANTI,
//!   where it matches (NLJP:101);
//! - a mark join emits every probe row with a mark that is never NULL: matched for SEMI, not
//!   matched for ANTI (NLJP:280-297).
use doris_segment::writer::Datum;
use doris_thrift::gen::plan_nodes::{TAssertNumRowsNode, TAssertion};
use doris_thrift::gen::status::TStatusCode;

use crate::hash_join::{Cond, JoinOp, Side};
use crate::ExecError;

fn joined(probe: Option<&Vec<Datum>>, pw: usize, build: Option<&Vec<Datum>>, bw: usize) -> Vec<Datum> {
    let mut row = Vec::with_capacity(pw + bw);
    match probe {
        Some(p) => row.extend(p.iter().cloned()),
        None => row.resize(pw, Datum::Null),
    }
    match build {
        Some(b) => row.extend(b.iter().cloned()),
        None => row.resize(pw + bw, Datum::Null),
    }
    row
}

/// The rows of a nested-loop join, probe row then build row (then the mark).
pub fn nested_loop(op: JoinOp, mark: bool, conj: Option<&Cond>, probe: Side, build: Side) -> Result<Vec<Vec<Datum>>, ExecError> {
    use JoinOp::*;
    if !matches!(
        op,
        Inner | Cross | LeftOuter | RightOuter | FullOuter | LeftSemi | LeftAnti | NullAwareLeftAnti
    ) {
        return Err(ExecError::Unsupported {
            what: format!("a CROSS_JOIN_NODE with op {op:?}"),
        });
    }
    if mark && !matches!(op, LeftSemi | LeftAnti | NullAwareLeftAnti) {
        return Err(ExecError::Unsupported {
            what: format!("a nested-loop mark join with op {op:?}"),
        });
    }
    let (pw, bw) = (probe.width, build.width);
    let pairs: Vec<Vec<Datum>> = probe
        .rows
        .iter()
        .flat_map(|p| build.rows.iter().map(move |b| joined(Some(p), pw, Some(b), bw)))
        .collect();
    let null_matches = op == NullAwareLeftAnti;
    let pass: Vec<bool> = match conj {
        Some(c) if !pairs.is_empty() => c(&pairs)?.into_iter().map(|v| v.unwrap_or(null_matches)).collect(),
        _ => vec![true; pairs.len()],
    };
    let n = build.rows.len();
    let mut probe_hit = vec![false; probe.rows.len()];
    let mut build_hit = vec![false; n];
    let mut out = Vec::new();
    for (i, (row, ok)) in pairs.into_iter().zip(&pass).enumerate() {
        if !*ok {
            continue;
        }
        probe_hit[i / n] = true;
        build_hit[i % n] = true;
        if matches!(op, Inner | Cross | LeftOuter | RightOuter | FullOuter) {
            out.push(row);
        }
    }
    for (p, hit) in probe_hit.iter().enumerate() {
        let row = Some(&probe.rows[p]);
        if mark {
            let m = *hit == (op == LeftSemi);
            let mut r = joined(row, pw, None, bw);
            r.push(Datum::Bool(m));
            out.push(r);
            continue;
        }
        let keep = match op {
            LeftOuter | FullOuter | LeftAnti | NullAwareLeftAnti => !hit,
            LeftSemi => *hit,
            _ => false,
        };
        if keep {
            out.push(joined(row, pw, None, bw));
        }
    }
    if matches!(op, RightOuter | FullOuter) {
        for (b, hit) in build_hit.iter().enumerate() {
            if !hit {
                out.push(joined(None, pw, Some(&build.rows[b]), bw));
            }
        }
    }
    Ok(out)
}

/// ASSERT_NUM_ROWS_NODE over its child's `rows` rows (`assert_num_rows_operator.cpp:44-117`):
/// true when the node emits one all-NULL row instead (EQ 1 over no rows, with
/// `should_convert_output_to_nullable`); a failed assertion is CANCELLED with C++'s message.
pub fn assert_rows(node_id: i32, a: &TAssertNumRowsNode, rows: usize) -> Result<bool, ExecError> {
    let d = a.desired_num_rows.unwrap_or(0);
    let n = rows as i64;
    let how = a.assertion.unwrap_or(TAssertion::LE);
    let (ok, name) = match how {
        TAssertion::EQ => (n == d, "EQ"),
        TAssertion::NE => (n != d, "NE"),
        TAssertion::LT => (n < d, "LT"),
        TAssertion::LE => (n <= d, "LE"),
        TAssertion::GT => (n > d, "GT"),
        TAssertion::GE => (n >= d, "GE"),
        other => {
            return Err(ExecError::Unsupported {
                what: format!("an ASSERT_NUM_ROWS_NODE assertion {other:?}"),
            })
        }
    };
    let null_row = how == TAssertion::EQ && n == 0 && d == 1 && a.should_convert_output_to_nullable == Some(true);
    if ok || null_row {
        return Ok(null_row);
    }
    Err(ExecError::Coded {
        code: TStatusCode::CANCELLED.0,
        name: "CANCELLED",
        msg: format!(
            "AssertOperator(node id: {node_id}) Expected {name} {d} to be returned by expression {}(actual rows: {n}) ",
            a.subquery_string.as_deref().unwrap_or_default()
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn side(keys: &[Option<i128>]) -> Side {
        Side {
            rows: keys
                .iter()
                .enumerate()
                .map(|(i, k)| vec![k.map_or(Datum::Null, Datum::Int), Datum::Int(i as i128)])
                .collect(),
            keys: Vec::new(),
            width: 2,
        }
    }

    fn idx(d: &Datum) -> Option<i128> {
        match d {
            Datum::Int(i) => Some(*i),
            _ => None,
        }
    }

    fn pairs(rows: &[Vec<Datum>]) -> Vec<(Option<i128>, Option<i128>)> {
        rows.iter().map(|r| (idx(&r[1]), idx(&r[3]))).collect()
    }

    /// `probe key = build key`, NULL when either is NULL.
    fn eq(rows: &[Vec<Datum>]) -> Result<Vec<Option<bool>>, ExecError> {
        Ok(rows
            .iter()
            .map(|r| match (&r[0], &r[2]) {
                (Datum::Int(a), Datum::Int(b)) => Some(a == b),
                _ => None,
            })
            .collect())
    }

    fn run(op: JoinOp, conj: Option<&Cond>, probe: &[Option<i128>], build: &[Option<i128>]) -> Vec<(Option<i128>, Option<i128>)> {
        pairs(&nested_loop(op, false, conj, side(probe), side(build)).unwrap())
    }

    #[test]
    fn cross_is_probe_major() {
        let got = run(JoinOp::Cross, None, &[Some(1), Some(2)], &[Some(3), Some(4)]);
        assert_eq!(got, [(0, 0), (0, 1), (1, 0), (1, 1)].map(|(p, b)| (Some(p), Some(b))));
    }

    #[test]
    fn left_outer_unmatched_after_matches() {
        let got = run(JoinOp::LeftOuter, Some(&eq), &[Some(1), Some(5), Some(2)], &[Some(1), Some(2)]);
        assert_eq!(got, [(Some(0), Some(0)), (Some(2), Some(1)), (Some(1), None)]);
    }

    #[test]
    fn right_and_full_outer_tail() {
        let got = run(JoinOp::RightOuter, Some(&eq), &[Some(2)], &[Some(1), Some(2), Some(3)]);
        assert_eq!(got, [(Some(0), Some(1)), (None, Some(0)), (None, Some(2))]);
        let got = run(JoinOp::FullOuter, Some(&eq), &[Some(9), Some(2)], &[Some(1), Some(2)]);
        assert_eq!(got, [(Some(1), Some(1)), (Some(0), None), (None, Some(0))]);
    }

    #[test]
    fn naaj_null_conjunct_counts_as_match() {
        assert_eq!(run(JoinOp::NullAwareLeftAnti, Some(&eq), &[Some(1), None], &[Some(2)]), [(Some(0), None)]);
        assert_eq!(
            run(JoinOp::NullAwareLeftAnti, Some(&eq), &[Some(1), None], &[]),
            [(Some(0), None), (Some(1), None)]
        );
    }

    #[test]
    fn nlj_mark_is_never_null() {
        let nulls = |rows: &[Vec<Datum>]| -> Result<Vec<Option<bool>>, ExecError> { Ok(vec![None; rows.len()]) };
        let got = nested_loop(JoinOp::LeftSemi, true, Some(&nulls), side(&[Some(1)]), side(&[Some(1)])).unwrap();
        assert_eq!(got[0].last(), Some(&Datum::Bool(false)));
        let got = nested_loop(JoinOp::LeftAnti, true, Some(&nulls), side(&[Some(1)]), side(&[Some(1)])).unwrap();
        assert_eq!(got[0].last(), Some(&Datum::Bool(true)));
    }

    fn eq_one() -> TAssertNumRowsNode {
        TAssertNumRowsNode {
            desired_num_rows: Some(1),
            subquery_string: Some("LogicalProject[31] ( distinct=false, projects=[last_updated_stamp#4] )".into()),
            assertion: Some(TAssertion::EQ),
            should_convert_output_to_nullable: Some(true),
        }
    }

    #[test]
    fn assert_rows_eq_one() {
        assert!(!assert_rows(7, &eq_one(), 1).unwrap());
        assert!(assert_rows(7, &eq_one(), 0).unwrap());
        let err = assert_rows(7, &eq_one(), 2).unwrap_err();
        let ExecError::Coded { code, msg, .. } = err else { panic!("{err:?}") };
        assert_eq!(code, TStatusCode::CANCELLED.0);
        assert_eq!(
            msg,
            "AssertOperator(node id: 7) Expected EQ 1 to be returned by expression LogicalProject[31] ( distinct=false, projects=[last_updated_stamp#4] )(actual rows: 2) "
        );
        let no_null = TAssertNumRowsNode {
            should_convert_output_to_nullable: Some(false),
            ..eq_one()
        };
        assert!(assert_rows(7, &no_null, 0).is_err());
    }
}
