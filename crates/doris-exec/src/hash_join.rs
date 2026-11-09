//! HASH_JOIN_NODE over rows, as the C++ BE gives them (`exec/operator/hashjoin_*`,
//! `exec/common/hash_table/join_hash_table.h` "HT", `exec/operator/join/process_hash_table_probe_impl.h` "PI"):
//! - build rows are inserted at the head of their key's chain, so a probe row's matches come out
//!   last build row first (HT:89-101); probe rows keep their order;
//! - keys compare bitwise: `-0.0` and `0.0` differ, a NaN matches only its own bits; an EQ key
//!   that is NULL matches nothing, an EQ_FOR_NULL key matches NULL to NULL (`hashjoin_build_sink.cpp:753-773`);
//! - other conjuncts are two-valued (NULL fails) and decide per pair (PI:905-988);
//! - RIGHT/FULL OUTER, RIGHT_SEMI and RIGHT_ANTI end with the build rows they left (or took), in
//!   build order (HT:200-225).
//!
//! A joined row is the probe row then the build row, either all NULL when absent.
use std::collections::HashMap;

use doris_segment::writer::Datum;
use doris_thrift::gen::plan_nodes::TJoinOp;

use crate::ExecError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOp {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    NullAwareLeftAnti,
    NullAwareLeftSemi,
    AsofLeftInner,
    AsofLeftOuter,
    Cross,
}

/// The join op of a Thrift op; None for those doris-rs refuses (MERGE_JOIN, ASOF_RIGHT_*).
pub fn join_op(op: TJoinOp) -> Option<JoinOp> {
    Some(match op {
        TJoinOp::INNER_JOIN => JoinOp::Inner,
        TJoinOp::LEFT_OUTER_JOIN => JoinOp::LeftOuter,
        TJoinOp::RIGHT_OUTER_JOIN => JoinOp::RightOuter,
        TJoinOp::FULL_OUTER_JOIN => JoinOp::FullOuter,
        TJoinOp::LEFT_SEMI_JOIN => JoinOp::LeftSemi,
        TJoinOp::RIGHT_SEMI_JOIN => JoinOp::RightSemi,
        TJoinOp::LEFT_ANTI_JOIN => JoinOp::LeftAnti,
        TJoinOp::RIGHT_ANTI_JOIN => JoinOp::RightAnti,
        TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN => JoinOp::NullAwareLeftAnti,
        TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN => JoinOp::NullAwareLeftSemi,
        TJoinOp::ASOF_LEFT_INNER_JOIN => JoinOp::AsofLeftInner,
        TJoinOp::ASOF_LEFT_OUTER_JOIN => JoinOp::AsofLeftOuter,
        TJoinOp::CROSS_JOIN => JoinOp::Cross,
        _ => return None,
    })
}

/// One side's rows (`width` columns each) and each row's key values.
pub struct Side {
    pub rows: Vec<Vec<Datum>>,
    pub keys: Vec<Vec<Datum>>,
    pub width: usize,
}

/// A conjunct over joined rows: Some(true/false), or None for NULL.
pub type Cond<'a> = dyn Fn(&[Vec<Datum>]) -> Result<Vec<Option<bool>>, ExecError> + 'a;

pub struct HashJoin<'a> {
    pub op: JoinOp,
    /// Per eq key: EQ_FOR_NULL (`<=>`).
    pub null_safe: Vec<bool>,
    pub mark: bool,
    pub other: Option<&'a Cond<'a>>,
    pub mark_conj: Option<&'a Cond<'a>>,
    /// ASOF joins: the `match_condition` op, and the index of the ASOF value in the keys (after
    /// the eq keys).
    pub asof: Option<(AsofOp, usize)>,
}

/// An ASOF `match_condition`: `probe OP build`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsofOp {
    Ge,
    Gt,
    Le,
    Lt,
}

/// A key value as C++ compares it: by its bits.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Bits {
    Null,
    Bool(bool),
    Int(i128),
    F32(u32),
    F64(u64),
    Bytes(Vec<u8>),
}

fn bits(d: &Datum) -> Bits {
    match d {
        Datum::Null => Bits::Null,
        Datum::Bool(b) => Bits::Bool(*b),
        Datum::Int(i) => Bits::Int(*i),
        Datum::Float(f) => Bits::F32(f.to_bits()),
        Datum::Double(f) => Bits::F64(f.to_bits()),
        Datum::Bytes(b) => Bits::Bytes(b.clone()),
    }
}

/// A row's key, or None when an EQ (not `<=>`) key is NULL: such a row matches nothing.
fn key(values: &[Datum], null_safe: &[bool]) -> Option<Vec<Bits>> {
    values
        .iter()
        .enumerate()
        .map(|(i, v)| {
            if matches!(v, Datum::Null) && !null_safe.get(i).copied().unwrap_or(false) {
                None
            } else {
                Some(bits(v))
            }
        })
        .collect()
}

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

/// The rows of a hash join, probe row then build row.
pub fn hash_join(j: &HashJoin, probe: Side, build: Side) -> Result<Vec<Vec<Datum>>, ExecError> {
    use JoinOp::*;
    let unsupported = |what: &str| Err(ExecError::Unsupported { what: what.to_string() });
    match j.op {
        Cross => return unsupported("a HASH_JOIN_NODE with the CROSS_JOIN op"),
        AsofLeftInner | AsofLeftOuter => return asof_join(j, probe, build),
        _ if j.mark => return mark_join(j, probe, build),
        NullAwareLeftSemi => return unsupported("a NULL_AWARE_LEFT_SEMI_JOIN that is not a mark join"),
        _ => {}
    }
    let (pw, bw) = (probe.width, build.width);
    let mut chains: HashMap<Vec<Bits>, Vec<usize>> = HashMap::new();
    let mut build_null = false;
    for (i, k) in build.keys.iter().enumerate() {
        match key(k, &j.null_safe) {
            Some(k) => chains.entry(k).or_default().push(i),
            None => build_null = true,
        }
    }
    // NULL_AWARE_LEFT_ANTI: a NULL build key leaves no row (`hashjoin_build_sink.cpp:323-331`)
    if j.op == NullAwareLeftAnti && build_null {
        return Ok(Vec::new());
    }
    // each probe row's candidates (last build row first) and whether its key was NULL
    let mut candidates: Vec<Vec<usize>> = Vec::with_capacity(probe.rows.len());
    let mut probe_null = Vec::with_capacity(probe.rows.len());
    for k in &probe.keys {
        let k = key(k, &j.null_safe);
        probe_null.push(k.is_none());
        let mut c: Vec<usize> = k
            .and_then(|k| chains.get(&k))
            .map(|c| c.iter().rev().copied().collect())
            .unwrap_or_default();
        // NULL_AWARE_LEFT_ANTI with other conjuncts pairs a NULL probe key with every build row (PI:631-652)
        if c.is_empty() && *probe_null.last().unwrap_or(&false) && j.op == NullAwareLeftAnti && j.other.is_some() {
            c = (0..build.rows.len()).collect();
        }
        candidates.push(c);
    }
    // whether each candidate pair passes the other conjuncts
    let passes: Vec<Vec<bool>> = match j.other {
        None => candidates.iter().map(|c| vec![true; c.len()]).collect(),
        Some(cond) => {
            let pairs: Vec<Vec<Datum>> = candidates
                .iter()
                .enumerate()
                .flat_map(|(p, c)| c.iter().map(move |b| (p, *b)))
                .map(|(p, b)| joined(Some(&probe.rows[p]), pw, Some(&build.rows[b]), bw))
                .collect();
            let mut verdicts = cond(&pairs)?.into_iter();
            candidates
                .iter()
                .map(|c| c.iter().map(|_| verdicts.next().flatten() == Some(true)).collect())
                .collect()
        }
    };
    let mut visited = vec![false; build.rows.len()];
    let mut out = Vec::new();
    for (p, (c, pass)) in candidates.iter().zip(&passes).enumerate() {
        let row = Some(&probe.rows[p]);
        let any = pass.iter().any(|x| *x);
        for (b, ok) in c.iter().zip(pass) {
            if *ok {
                visited[*b] = true;
            }
        }
        match j.op {
            Inner | LeftOuter | RightOuter | FullOuter => {
                for (b, ok) in c.iter().zip(pass) {
                    if *ok {
                        out.push(joined(row, pw, Some(&build.rows[*b]), bw));
                    }
                }
                if !any && matches!(j.op, LeftOuter | FullOuter) {
                    out.push(joined(row, pw, None, bw));
                }
            }
            LeftSemi if any => out.push(joined(row, pw, None, bw)),
            LeftAnti if !any => out.push(joined(row, pw, None, bw)),
            NullAwareLeftAnti => {
                // an empty build side keeps every row; otherwise a NULL probe key (with no other
                // conjunct to try) is dropped (HT:109-114, 245-265, 302-307)
                let keep = build.rows.is_empty() || !(any || (probe_null[p] && j.other.is_none()));
                if keep {
                    out.push(joined(row, pw, None, bw));
                }
            }
            _ => {}
        }
    }
    if matches!(j.op, RightOuter | FullOuter | RightSemi | RightAnti) {
        let take = j.op == RightSemi;
        for (b, v) in visited.iter().enumerate() {
            if *v == take {
                out.push(joined(None, pw, Some(&build.rows[b]), bw));
            }
        }
    }
    Ok(out)
}

/// An ASOF join's `match_condition` (`probe OP build`): its op and its two sides.
pub fn asof_condition(e: &doris_thrift::gen::exprs::TExpr) -> Result<(AsofOp, TExprPair), ExecError> {
    let root = e
        .nodes
        .first()
        .ok_or_else(|| ExecError::BadRequest("an empty ASOF match condition".into()))?;
    let name = root.fn_.as_ref().map(|f| f.name.function_name.to_ascii_lowercase()).unwrap_or_default();
    let how = match name.as_str() {
        "ge" => AsofOp::Ge,
        "gt" => AsofOp::Gt,
        "le" => AsofOp::Le,
        "lt" => AsofOp::Lt,
        _ => {
            return Err(ExecError::Unsupported {
                what: format!("an ASOF match condition {name:?}"),
            })
        }
    };
    if root.num_children != 2 {
        return Err(ExecError::BadRequest("an ASOF match condition without two sides".into()));
    }
    let mid = crate::aggregate::subtree_end(&e.nodes, 1)?;
    let end = crate::aggregate::subtree_end(&e.nodes, mid)?;
    let side = |a: usize, b: usize| doris_thrift::gen::exprs::TExpr {
        nodes: e.nodes[a..b].to_vec(),
    };
    Ok((how, (side(1, mid), side(mid, end))))
}

/// The probe and build sides of a comparison.
pub type TExprPair = (doris_thrift::gen::exprs::TExpr, doris_thrift::gen::exprs::TExpr);

/// A mark join (`process_hash_table_probe_impl.h:741-892`): every probe row once, with its mark
/// last. The mark is TRUE when some candidate passes; else NULL when the join is null-aware and
/// the probe key or a build key is NULL, or a candidate's conjuncts were NULL; else FALSE. An empty
/// build side gives FALSE. Anti joins flip TRUE and FALSE.
fn mark_join(j: &HashJoin, probe: Side, build: Side) -> Result<Vec<Vec<Datum>>, ExecError> {
    use JoinOp::*;
    let null_aware = matches!(j.op, NullAwareLeftAnti | NullAwareLeftSemi);
    let anti = match j.op {
        LeftSemi | NullAwareLeftSemi => false,
        LeftAnti | NullAwareLeftAnti => true,
        other => {
            return Err(ExecError::Unsupported {
                what: format!("a mark join with op {other:?}"),
            })
        }
    };
    let (pw, bw) = (probe.width, build.width);
    let mut chains: HashMap<Vec<Bits>, Vec<usize>> = HashMap::new();
    let mut null_keyed = Vec::new();
    for (i, k) in build.keys.iter().enumerate() {
        match key(k, &j.null_safe) {
            Some(k) => chains.entry(k).or_default().push(i),
            None => null_keyed.push(i),
        }
    }
    let build_null = !null_keyed.is_empty();
    // each probe row's candidate pairs: (build row, whether the pair came from a NULL key)
    let mut candidates: Vec<Vec<(usize, bool)>> = Vec::with_capacity(probe.rows.len());
    let mut probe_null = Vec::with_capacity(probe.rows.len());
    for k in &probe.keys {
        let k = key(k, &j.null_safe);
        probe_null.push(k.is_none());
        let mut c: Vec<(usize, bool)> = match &k {
            Some(k) => chains.get(k).map(|c| c.iter().rev().map(|b| (*b, false)).collect()).unwrap_or_default(),
            None => Vec::new(),
        };
        // null-aware with other conjuncts also tries the NULL-keyed build rows, and a NULL probe
        // key every build row, each as a NULL comparison (PI:631-652, 783-786)
        if null_aware && j.other.is_some() {
            match k {
                Some(_) => c.extend(null_keyed.iter().map(|b| (*b, true))),
                None => c = (0..build.rows.len()).map(|b| (b, true)).collect(),
            }
        }
        candidates.push(c);
    }
    let pairs: Vec<Vec<Datum>> = candidates
        .iter()
        .enumerate()
        .flat_map(|(p, c)| c.iter().map(move |(b, _)| (p, *b)))
        .map(|(p, b)| joined(Some(&probe.rows[p]), pw, Some(&build.rows[b]), bw))
        .collect();
    let eval = |cond: Option<&Cond>| -> Result<Vec<Option<bool>>, ExecError> {
        match cond {
            Some(c) if !pairs.is_empty() => c(&pairs),
            _ => Ok(vec![Some(true); pairs.len()]),
        }
    };
    let (other, mark_conj) = (eval(j.other)?, eval(j.mark_conj)?);
    let mut at = 0;
    let mut out = Vec::with_capacity(probe.rows.len());
    for (p, c) in candidates.iter().enumerate() {
        let mut found = false;
        let mut null = false;
        for (_, from_null) in c {
            let v = and3(other[at], mark_conj[at]);
            at += 1;
            if *from_null {
                // a NULL comparison: NULL unless the other conjuncts are false
                null |= v != Some(false);
            } else if v == Some(true) {
                found = true;
            } else if v.is_none() {
                null = true;
            }
        }
        let mark = if build.rows.is_empty() {
            Some(false)
        } else if found {
            Some(true)
        } else if null || (null_aware && j.other.is_none() && (probe_null[p] || build_null)) {
            None
        } else {
            Some(false)
        };
        let mark = mark.map(|m| m != anti);
        let mut row = joined(Some(&probe.rows[p]), pw, None, bw);
        row.push(mark.map_or(Datum::Null, Datum::Bool));
        out.push(row);
    }
    Ok(out)
}

/// Three-valued AND.
fn and3(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

/// An ASOF join (`exec/common/join_utils.h:255-320`): per probe row, of the build rows with its
/// eq keys, the one nearest its ASOF value on the `match_condition`'s side. A NULL key or value
/// matches nothing. C++ breaks a tie on the ASOF value with an unstable sort; doris-rs refuses
/// a tie between rows that differ.
fn asof_join(j: &HashJoin, probe: Side, build: Side) -> Result<Vec<Vec<Datum>>, ExecError> {
    let (how, at) = j
        .asof
        .ok_or_else(|| ExecError::BadRequest("an ASOF join without its match condition".into()))?;
    let (pw, bw) = (probe.width, build.width);
    let value = |k: &[Datum]| match k.get(at) {
        Some(Datum::Int(v)) => Some(*v),
        _ => None,
    };
    let mut chains: HashMap<Vec<Bits>, Vec<(i128, usize)>> = HashMap::new();
    for (i, k) in build.keys.iter().enumerate() {
        if let (Some(eq), Some(v)) = (key(&k[..at.min(k.len())], &j.null_safe), value(k)) {
            chains.entry(eq).or_default().push((v, i));
        }
    }
    let mut out = Vec::new();
    for (p, k) in probe.keys.iter().enumerate() {
        let row = Some(&probe.rows[p]);
        let found = match (key(&k[..at.min(k.len())], &j.null_safe), value(k)) {
            (Some(eq), Some(v)) => chains.get(&eq).and_then(|c| {
                let fits = |b: i128| match how {
                    AsofOp::Ge => b <= v,
                    AsofOp::Gt => b < v,
                    AsofOp::Le => b >= v,
                    AsofOp::Lt => b > v,
                };
                let better = |a: i128, b: i128| if matches!(how, AsofOp::Ge | AsofOp::Gt) { a > b } else { a < b };
                let best = c
                    .iter()
                    .filter(|(b, _)| fits(*b))
                    .map(|(b, _)| *b)
                    .reduce(|x, y| if better(y, x) { y } else { x })?;
                Some(c.iter().filter(|(b, _)| *b == best).map(|(_, i)| *i).collect::<Vec<_>>())
            }),
            _ => None,
        };
        match found {
            Some(rows) => {
                let first = rows[0];
                if rows.iter().any(|r| build.rows[*r] != build.rows[first]) {
                    return Err(ExecError::Unsupported {
                        what: "an ASOF match among equal values".into(),
                    });
                }
                out.push(joined(row, pw, Some(&build.rows[first]), bw));
            }
            None if j.op == JoinOp::AsofLeftOuter => out.push(joined(row, pw, None, bw)),
            None => {}
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A side whose rows are `[key, index]`, keyed on the first column.
    fn side(keys: &[Option<i128>]) -> Side {
        Side {
            rows: keys
                .iter()
                .enumerate()
                .map(|(i, k)| vec![k.map_or(Datum::Null, Datum::Int), Datum::Int(i as i128)])
                .collect(),
            keys: keys.iter().map(|k| vec![k.map_or(Datum::Null, Datum::Int)]).collect(),
            width: 2,
        }
    }

    fn idx(d: &Datum) -> Option<i128> {
        match d {
            Datum::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Each output row as (probe index, build index).
    fn pairs(rows: &[Vec<Datum>]) -> Vec<(Option<i128>, Option<i128>)> {
        rows.iter().map(|r| (idx(&r[1]), idx(&r[3]))).collect()
    }

    fn join(op: JoinOp) -> HashJoin<'static> {
        HashJoin {
            op,
            null_safe: vec![false],
            mark: false,
            other: None,
            mark_conj: None,
            asof: None,
        }
    }

    fn run(j: &HashJoin, probe: &[Option<i128>], build: &[Option<i128>]) -> Vec<(Option<i128>, Option<i128>)> {
        pairs(&hash_join(j, side(probe), side(build)).unwrap())
    }

    #[test]
    fn inner_matches_in_reverse_build_order() {
        let got = run(&join(JoinOp::Inner), &[Some(1), Some(3), Some(1)], &[Some(1), Some(1), Some(2), Some(1)]);
        let want = [(0, 3), (0, 1), (0, 0), (2, 3), (2, 1), (2, 0)].map(|(p, b)| (Some(p), Some(b)));
        assert_eq!(got, want);
    }

    #[test]
    fn left_outer_null_extends_in_place() {
        let got = run(&join(JoinOp::LeftOuter), &[Some(5), Some(1), Some(6)], &[Some(1)]);
        assert_eq!(got, [(Some(0), None), (Some(1), Some(0)), (Some(2), None)]);
    }

    #[test]
    fn right_and_full_outer_tail_in_build_order() {
        let got = run(&join(JoinOp::RightOuter), &[Some(1)], &[Some(9), Some(1), Some(8)]);
        assert_eq!(got, [(Some(0), Some(1)), (None, Some(0)), (None, Some(2))]);
        let got = run(&join(JoinOp::FullOuter), &[Some(1), Some(7)], &[Some(9), Some(1), Some(8)]);
        assert_eq!(got, [(Some(0), Some(1)), (Some(1), None), (None, Some(0)), (None, Some(2))]);
    }

    #[test]
    fn right_semi_and_anti_emit_only_at_the_end() {
        let build = [Some(9), Some(1), Some(8), None];
        let got = run(&join(JoinOp::RightSemi), &[Some(8), Some(1), Some(1)], &build);
        assert_eq!(got, [(None, Some(1)), (None, Some(2))]);
        let got = run(&join(JoinOp::RightAnti), &[Some(8), Some(1)], &build);
        assert_eq!(got, [(None, Some(0)), (None, Some(3))]);
    }

    #[test]
    fn null_keys() {
        // EQ: a NULL key matches nothing, but LEFT_ANTI keeps the probe row
        assert_eq!(run(&join(JoinOp::Inner), &[None, Some(1)], &[Some(1), None]), [(Some(1), Some(0))]);
        assert_eq!(
            run(&join(JoinOp::LeftAnti), &[None, Some(2), Some(1)], &[Some(1)]),
            [(Some(0), None), (Some(1), None)]
        );
        // two keys, one NULL: never a match
        let two = |k: &[(i128, Option<i128>)]| Side {
            rows: k
                .iter()
                .enumerate()
                .map(|(i, (a, _))| vec![Datum::Int(*a), Datum::Int(i as i128)])
                .collect(),
            keys: k.iter().map(|(a, b)| vec![Datum::Int(*a), b.map_or(Datum::Null, Datum::Int)]).collect(),
            width: 2,
        };
        let j = HashJoin {
            null_safe: vec![false, false],
            ..join(JoinOp::Inner)
        };
        assert!(hash_join(&j, two(&[(1, None)]), two(&[(1, None)])).unwrap().is_empty());
        // EQ_FOR_NULL: NULL matches NULL
        let j = HashJoin {
            null_safe: vec![true],
            ..join(JoinOp::Inner)
        };
        assert_eq!(run(&j, &[None, Some(2)], &[Some(2), None]), [(Some(0), Some(1)), (Some(1), Some(0))]);
    }

    #[test]
    fn float_keys_are_bitwise() {
        let nan = f64::from_bits(0x7ff8_0000_0000_0001);
        let doubles = |v: &[f64]| Side {
            rows: v
                .iter()
                .enumerate()
                .map(|(i, d)| vec![Datum::Double(*d), Datum::Int(i as i128)])
                .collect(),
            keys: v.iter().map(|d| vec![Datum::Double(*d)]).collect(),
            width: 2,
        };
        let got = hash_join(&join(JoinOp::Inner), doubles(&[0.0, nan]), doubles(&[-0.0, nan])).unwrap();
        assert_eq!(pairs(&got), [(Some(1), Some(1))]);
    }

    /// `probe index < build index`, NULL when the build index is 2.
    fn lt(rows: &[Vec<Datum>]) -> Result<Vec<Option<bool>>, ExecError> {
        Ok(rows
            .iter()
            .map(|r| match (idx(&r[1]), idx(&r[3])) {
                (_, Some(2)) => None,
                (Some(p), Some(b)) => Some(p < b),
                _ => None,
            })
            .collect())
    }

    #[test]
    fn other_conjunct() {
        let with = |op| HashJoin {
            other: Some(&lt),
            ..join(op)
        };
        // p0 passes with b1 only; p1 passes with none
        assert_eq!(
            run(&with(JoinOp::LeftOuter), &[Some(1), Some(1)], &[Some(1), Some(1)]),
            [(Some(0), Some(1)), (Some(1), None)]
        );
        // p0 passes with b1 and b3 (b2 is NULL): emitted once
        let build = [Some(1), Some(1), Some(1), Some(1)];
        assert_eq!(
            run(&with(JoinOp::LeftSemi), &[Some(1), Some(1), Some(1)], &build),
            [(Some(0), None), (Some(1), None), (Some(2), None)]
        );
        assert_eq!(
            run(&with(JoinOp::LeftAnti), &[Some(1), Some(3), Some(5)], &[Some(1), Some(1), Some(1)]),
            [(Some(1), None), (Some(2), None)]
        );
        // only (p0, b1) passes; b0 stays unvisited
        assert_eq!(
            run(&with(JoinOp::RightOuter), &[Some(1), Some(1)], &[Some(1), Some(1)]),
            [(Some(0), Some(1)), (None, Some(0))]
        );
    }

    #[test]
    fn null_aware_left_anti() {
        let j = join(JoinOp::NullAwareLeftAnti);
        assert!(run(&j, &[Some(2), None], &[Some(1), None]).is_empty());
        assert_eq!(run(&j, &[None, Some(1)], &[]), [(Some(0), None), (Some(1), None)]);
        assert_eq!(run(&j, &[None, Some(2), Some(1)], &[Some(1)]), [(Some(1), None)]);
    }

    /// The mark (the last column) of each output row.
    fn marks(j: &HashJoin, probe: &[Option<i128>], build: &[Option<i128>]) -> Vec<Option<bool>> {
        hash_join(j, side(probe), side(build))
            .unwrap()
            .iter()
            .map(|r| match r.last() {
                Some(Datum::Bool(b)) => Some(*b),
                _ => None,
            })
            .collect()
    }

    fn marked(op: JoinOp) -> HashJoin<'static> {
        HashJoin { mark: true, ..join(op) }
    }

    #[test]
    fn mark_semi_three_valued() {
        let j = marked(JoinOp::NullAwareLeftSemi);
        assert_eq!(marks(&j, &[Some(1), Some(2), None], &[Some(1), None]), [Some(true), None, None]);
        assert_eq!(marks(&j, &[Some(2)], &[Some(1)]), [Some(false)]);
        assert_eq!(marks(&j, &[None], &[]), [Some(false)]);
    }

    #[test]
    fn mark_anti_flips() {
        let j = marked(JoinOp::NullAwareLeftAnti);
        assert_eq!(marks(&j, &[Some(1), Some(2), None], &[Some(1), None]), [Some(false), None, None]);
        assert_eq!(marks(&j, &[Some(2)], &[Some(1)]), [Some(true)]);
        assert_eq!(marks(&j, &[None], &[]), [Some(true)]);
    }

    fn null_cond(rows: &[Vec<Datum>]) -> Result<Vec<Option<bool>>, ExecError> {
        Ok(vec![None; rows.len()])
    }

    #[test]
    fn mark_with_conjunct_null_is_null() {
        let j = HashJoin {
            mark_conj: Some(&null_cond),
            ..marked(JoinOp::LeftSemi)
        };
        assert_eq!(marks(&j, &[Some(1), Some(2)], &[Some(1)]), [None, Some(false)]);
    }

    #[test]
    fn mark_emits_every_probe_row_once() {
        let got = hash_join(&marked(JoinOp::LeftSemi), side(&[Some(3), Some(1), None]), side(&[Some(1), Some(1)])).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got.iter().map(|r| idx(&r[1])).collect::<Vec<_>>(), [Some(0), Some(1), Some(2)]);
        assert_eq!(
            got.iter().map(|r| r.last().cloned()).collect::<Vec<_>>(),
            [Some(Datum::Bool(false)), Some(Datum::Bool(true)), Some(Datum::Bool(false))]
        );
    }

    /// Rows `[k, v, x]` keyed on k, with v the ASOF value as the last key.
    fn asof_side(rows: &[(i128, Option<i128>, &str)]) -> Side {
        Side {
            rows: rows
                .iter()
                .map(|(k, v, x)| vec![Datum::Int(*k), v.map_or(Datum::Null, Datum::Int), Datum::Bytes(x.as_bytes().to_vec())])
                .collect(),
            keys: rows
                .iter()
                .map(|(k, v, _)| vec![Datum::Int(*k), v.map_or(Datum::Null, Datum::Int)])
                .collect(),
            width: 3,
        }
    }

    fn asof(op: JoinOp, how: AsofOp) -> HashJoin<'static> {
        HashJoin {
            asof: Some((how, 1)),
            ..join(op)
        }
    }

    /// The build x of each output row.
    fn picked(j: &HashJoin, probe: &[(i128, Option<i128>, &str)], build: &[(i128, Option<i128>, &str)]) -> Vec<Option<String>> {
        hash_join(j, asof_side(probe), asof_side(build))
            .unwrap()
            .iter()
            .map(|r| match &r[5] {
                Datum::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn asof_picks_the_nearest() {
        let build = [(1, Some(5), "b5"), (1, Some(9), "b9"), (1, Some(12), "b12"), (2, Some(10), "other")];
        let probe = [(1, Some(10), "p")];
        let one = |how, p: &[(i128, Option<i128>, &str)]| picked(&asof(JoinOp::AsofLeftInner, how), p, &build);
        assert_eq!(one(AsofOp::Ge, &probe), [Some("b9".to_string())]);
        assert_eq!(one(AsofOp::Gt, &probe), [Some("b9".to_string())]);
        assert_eq!(one(AsofOp::Le, &probe), [Some("b12".to_string())]);
        assert_eq!(one(AsofOp::Lt, &probe), [Some("b12".to_string())]);
        assert_eq!(one(AsofOp::Gt, &[(1, Some(9), "p")]), [Some("b5".to_string())]);
        assert_eq!(one(AsofOp::Ge, &[(1, Some(9), "p")]), [Some("b9".to_string())]);
        // no match: dropped by INNER, NULL-extended by OUTER; a NULL value never matches
        assert!(one(AsofOp::Lt, &[(1, Some(12), "p")]).is_empty());
        let outer = picked(&asof(JoinOp::AsofLeftOuter, AsofOp::Lt), &[(1, Some(12), "p"), (1, None, "q")], &build);
        assert_eq!(outer, [None, None]);
    }

    #[test]
    fn asof_tie_is_refused() {
        let j = asof(JoinOp::AsofLeftInner, AsofOp::Ge);
        let err = hash_join(&j, asof_side(&[(1, Some(10), "p")]), asof_side(&[(1, Some(9), "a"), (1, Some(9), "b")])).unwrap_err();
        assert!(
            matches!(&err, ExecError::Unsupported { what } if what.contains("an ASOF match among equal values")),
            "{err:?}"
        );
        let same = picked(&j, &[(1, Some(10), "p")], &[(1, Some(9), "a"), (1, Some(9), "a")]);
        assert_eq!(same, [Some("a".to_string())]);
    }
}
