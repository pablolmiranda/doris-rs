//! `retention`, `linear_histogram`, `exponential_moving_average` and `group_bit_*` as the C++ BE
//! computes them (`be/src/exprs/aggregate/`): see
//! `docs/superpowers/research/2026-11-10-m6-s7b3-aggregates.md`. Expected values are the
//! corpus's unless worked by hand.
use doris_exec::aggregate::{AggSpec, Aggregation};
use doris_exec::block::Block;
use doris_exec::exchange::Chunk;
use doris_exec::value::ColType;
use doris_exec::ExecError;
use doris_fixtures::build::{aggregate, array_of, float, scalar, slot, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn ct(t: &TTypeDesc) -> ColType {
    ColType::from_type_desc("", t, true).unwrap()
}

/// One call: `cols` are slots 1.., `lits` its constant arguments before (`lead`) or after them.
struct Call<'a> {
    name: &'a str,
    ret: TTypeDesc,
    cols: Vec<TTypeDesc>,
    lead: Vec<f64>,
    tail: Vec<f64>,
}

impl Call<'_> {
    fn spec(&self, merge: bool) -> AggSpec {
        let d = ty(T::DOUBLE);
        let mut params = vec![d.clone(); self.lead.len()];
        params.extend(self.cols.iter().cloned());
        params.extend(vec![d.clone(); self.tail.len()]);
        let args: Vec<TExpr> = if merge {
            vec![slot(99, scalar(T::VARCHAR, 65533, 0, 0))]
        } else {
            let mut a: Vec<TExpr> = self.lead.iter().map(|v| float(*v, d.clone())).collect();
            a.extend(self.cols.iter().enumerate().map(|(i, t)| slot(i as i32 + 1, t.clone())));
            a.extend(self.tail.iter().map(|v| float(*v, d.clone())));
            a
        };
        AggSpec::from_expr(&aggregate(self.name, self.ret.clone(), params, merge, args)).unwrap()
    }

    fn chunk(&self, rows: &[Vec<Datum>]) -> Chunk {
        Chunk {
            block: Block {
                types: self.cols.iter().map(ct).collect(),
                rows: rows.to_vec(),
            },
            slots: (1..=self.cols.len() as i32).collect(),
        }
    }

    fn one(&self, rows: &[Vec<Datum>]) -> Result<Datum, ExecError> {
        let a = Aggregation {
            group_by: vec![],
            functions: vec![self.spec(false)],
            finalize: true,
        };
        Ok(a.run(&self.chunk(rows))?.rows.remove(0).remove(0))
    }

    fn two(&self, parts: &[&[Vec<Datum>]]) -> Datum {
        let mut partials = Vec::new();
        for p in parts {
            let a = Aggregation {
                group_by: vec![],
                functions: vec![self.spec(false)],
                finalize: false,
            };
            partials.extend(a.run(&self.chunk(p)).unwrap().rows);
        }
        let m = Aggregation {
            group_by: vec![],
            functions: vec![self.spec(true)],
            finalize: true,
        };
        let chunk = Chunk {
            block: Block {
                types: vec![ct(&scalar(T::VARCHAR, 65533, 0, 0))],
                rows: partials,
            },
            slots: vec![99],
        };
        m.run(&chunk).unwrap().rows.remove(0).remove(0)
    }
}

fn b(v: bool) -> Datum {
    Datum::Bool(v)
}

fn bools(v: &[bool]) -> Datum {
    Datum::Array(v.iter().map(|x| b(*x)).collect())
}

fn retention(n: usize) -> Call<'static> {
    Call {
        name: "retention",
        ret: array_of(ty(T::BOOLEAN)),
        cols: vec![ty(T::BOOLEAN); n],
        lead: vec![],
        tail: vec![],
    }
}

#[test]
fn retention_needs_the_first_condition() {
    let r = retention(3);
    let rows = vec![vec![b(true), b(false), b(false)], vec![b(false), b(true), b(false)]];
    assert_eq!(r.one(&rows).unwrap(), bools(&[true, true, false]));
    // an event without the first condition counts for nothing
    assert_eq!(r.one(&[vec![b(false), b(true), b(true)]]).unwrap(), bools(&[false, false, false]));
    // a row with a NULL condition is skipped; none accepted is NULL
    assert_eq!(r.one(&[vec![b(true), Datum::Null, b(true)]]).unwrap(), Datum::Null);
    assert_eq!(r.two(&[&rows[..1], &rows[1..]]), bools(&[true, true, false]));
}

fn histogram(tail: Vec<f64>) -> Call<'static> {
    Call {
        name: "linear_histogram",
        ret: scalar(T::VARCHAR, 65533, 0, 0),
        cols: vec![ty(T::BOOLEAN)],
        lead: vec![],
        tail,
    }
}

fn json(d: Datum) -> String {
    match d {
        Datum::Bytes(v) => String::from_utf8(v).unwrap(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn linear_histogram_json_as_recorded() {
    let rows = vec![vec![b(false)], vec![b(true)], vec![Datum::Null], vec![b(true)]];
    assert_eq!(
        json(histogram(vec![1.0, 0.5]).one(&rows).unwrap()),
        r#"{"num_buckets":2,"buckets":[{"lower":-0.5,"upper":0.5,"count":1,"acc_count":1},{"lower":0.5,"upper":1.5,"count":2,"acc_count":3}]}"#
    );
    // missing buckets between the keys are emitted with 0; bounds accumulate; doubles keep a fraction
    assert_eq!(
        json(histogram(vec![0.5]).one(&rows[..2]).unwrap()),
        r#"{"num_buckets":3,"buckets":[{"lower":0.0,"upper":0.5,"count":1,"acc_count":1},{"lower":0.5,"upper":1.0,"count":0,"acc_count":1},{"lower":1.0,"upper":1.5,"count":1,"acc_count":2}]}"#
    );
    // an empty or all-NULL input is the empty histogram, never NULL
    assert_eq!(
        json(histogram(vec![0.5]).one(&[vec![Datum::Null]]).unwrap()),
        r#"{"num_buckets":0,"buckets":[]}"#
    );
    assert_eq!(
        json(histogram(vec![1.0, 0.5]).two(&[&rows[..1], &[], &rows[1..]])),
        json(histogram(vec![1.0, 0.5]).one(&rows).unwrap())
    );
}

#[test]
fn linear_histogram_errors() {
    let err = |tail: Vec<f64>| match histogram(tail).one(&[vec![b(true)]]) {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "INVALID_ARGUMENT");
            msg
        }
        other => panic!("{other:?}"),
    };
    assert_eq!(err(vec![0.0]), "Invalid interval 0, row_num 0, interval should be larger than 0");
    assert_eq!(err(vec![1.0, 1.0]), "Invalid offset 1, row_num 0, offset should be in [0, interval)");
}

fn ema(hd: f64) -> Call<'static> {
    Call {
        name: "exponential_moving_average",
        ret: ty(T::DOUBLE),
        cols: vec![ty(T::DOUBLE), ty(T::DOUBLE)],
        lead: vec![hd],
        tail: vec![],
    }
}

fn points(v: &[(f64, f64)]) -> Vec<Vec<Datum>> {
    v.iter().map(|(x, t)| vec![Datum::Double(*x), Datum::Double(*t)]).collect()
}

#[test]
fn ema_matches_reference_order() {
    let basic: Vec<(f64, f64)> = [95, 95, 95, 96, 96, 96, 96, 97, 97, 97, 97, 98, 98, 98, 98, 99, 99, 99, 100, 100]
        .iter()
        .enumerate()
        .map(|(i, v)| (f64::from(*v), i as f64 + 1.0))
        .collect();
    assert_eq!(ema(5.0).one(&points(&basic)).unwrap(), Datum::Double(92.25779635374204));
    assert_eq!(ema(1.0).one(&points(&basic[..1])).unwrap(), Datum::Double(47.5));
    assert_eq!(ema(1.0).one(&points(&[(10.0, 1.0), (20.0, 3.0)])).unwrap(), Datum::Double(11.25));
    assert_eq!(
        ema(2.0).one(&points(&[(-10.0, 1.0), (10.0, 5.0)])).unwrap(),
        Datum::Double(2.196699141100893)
    );
    assert_eq!(
        ema(1.0).one(&points(&[(10.0, 5.0), (20.0, 5.0), (30.0, 10.0)])).unwrap(),
        Datum::Double(15.46875)
    );
    // a half decay of 0 is 0, not NULL; no row is NULL
    assert_eq!(ema(0.0).one(&points(&basic[..1])).unwrap(), Datum::Double(0.0));
    assert_eq!(ema(1.0).one(&[vec![Datum::Null, Datum::Double(1.0)]]).unwrap(), Datum::Null);
    // one tablet's rows in one state, merged into the empty one
    assert_eq!(ema(5.0).two(&[&[], &points(&basic)]), Datum::Double(92.25779635374204));
}

fn bit(name: &'static str) -> Call<'static> {
    Call {
        name,
        ret: ty(T::INT),
        cols: vec![ty(T::INT)],
        lead: vec![],
        tail: vec![],
    }
}

#[test]
fn group_bit_ops_match_reference() {
    let rows: Vec<Vec<Datum>> = [Some(12), Some(5), None, Some(-44)]
        .iter()
        .map(|v| vec![v.map_or(Datum::Null, Datum::Int)])
        .collect();
    // worked by hand: 12 & 5 & -44 = 4; 12 | 5 | -44 = -35; 12 ^ 5 ^ -44 = -35
    assert_eq!(bit("group_bit_and").one(&rows).unwrap(), Datum::Int(4));
    assert_eq!(bit("group_bit_or").one(&rows).unwrap(), Datum::Int(-35));
    assert_eq!(bit("group_bit_xor").one(&rows).unwrap(), Datum::Int(-35));
    assert_eq!(bit("group_bit_and").one(&rows[2..3]).unwrap(), Datum::Null);
    for f in ["group_bit_and", "group_bit_or", "group_bit_xor"] {
        assert_eq!(bit(f).two(&[&rows[..2], &rows[2..]]), bit(f).one(&rows).unwrap(), "{f}");
    }
}
