//! The moment aggregates as the C++ BE computes them (`be/src/exprs/aggregate/`): see
//! `docs/superpowers/research/2026-11-10-m6-s7b2-moments.md`. Expected values are the corpus's.
use doris_exec::aggregate::{AggSpec, Aggregation};
use doris_exec::block::Block;
use doris_exec::exchange::Chunk;
use doris_exec::value::ColType;
use doris_fixtures::build::{aggregate, scalar, slot, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn d() -> TTypeDesc {
    ty(T::DOUBLE)
}

fn ct(t: &TTypeDesc) -> ColType {
    ColType::from_type_desc("", t, true).unwrap()
}

/// Rows of `width` DOUBLE columns, in slots 1 and 2.
fn input(rows: &[Vec<Option<f64>>], width: usize) -> Chunk {
    Chunk {
        block: Block {
            types: vec![ct(&d()); width],
            rows: rows
                .iter()
                .map(|r| r.iter().map(|v| v.map_or(Datum::Null, Datum::Double)).collect())
                .collect(),
        },
        slots: (1..=width as i32).collect(),
    }
}

fn spec(name: &str, width: usize, merge: bool) -> AggSpec {
    let args = if merge {
        vec![slot(10, scalar(T::VARCHAR, 65533, 0, 0))]
    } else {
        (1..=width as i32).map(|s| slot(s, d())).collect()
    };
    AggSpec::from_expr(&aggregate(name, d(), vec![d(); width], merge, args)).unwrap()
}

fn one_phase(name: &str, rows: &[Vec<Option<f64>>]) -> Datum {
    let width = rows.first().map_or(1, Vec::len);
    let a = Aggregation {
        group_by: vec![],
        functions: vec![spec(name, width, false)],
        finalize: true,
    };
    a.run(&input(rows, width)).unwrap().rows.remove(0).remove(0)
}

/// One first-phase state per part, merged in order.
fn two_phases(name: &str, parts: &[Vec<Vec<Option<f64>>>]) -> Datum {
    let width = parts.iter().flatten().next().map_or(1, Vec::len);
    let mut partials = Vec::new();
    for p in parts {
        let a = Aggregation {
            group_by: vec![],
            functions: vec![spec(name, width, false)],
            finalize: false,
        };
        partials.extend(a.run(&input(p, width)).unwrap().rows);
    }
    let state = scalar(T::VARCHAR, 65533, 0, 0);
    let merge = Aggregation {
        group_by: vec![],
        functions: vec![spec(name, width, true)],
        finalize: true,
    };
    let chunk = Chunk {
        block: Block {
            types: vec![ct(&state)],
            rows: partials,
        },
        slots: vec![10],
    };
    merge.run(&chunk).unwrap().rows.remove(0).remove(0)
}

fn col(v: &[f64]) -> Vec<Vec<Option<f64>>> {
    v.iter().map(|x| vec![Some(*x)]).collect()
}

fn pairs(v: &[(f64, f64)]) -> Vec<Vec<Option<f64>>> {
    v.iter().map(|(a, b)| vec![Some(*a), Some(*b)]).collect()
}

const FIVE: [f64; 5] = [-10.0, -20.0, 100.0, 100.0, 1000.0];

#[test]
fn skew_and_kurt_match_the_reference() {
    assert_eq!(one_phase("skew", &col(&FIVE)), Datum::Double(1.4337199628825619));
    assert_eq!(one_phase("kurt", &col(&FIVE)), Datum::Double(0.16212458373485106));
    assert_eq!(one_phase("kurt", &col(&[-10.0, 100.0, 1000.0])), Datum::Double(-1.4999999999999993));
    // merged power sums give the same bits on these exact values
    assert_eq!(two_phases("skew", &[col(&FIVE[..2]), col(&FIVE[2..])]), Datum::Double(1.4337199628825619));
}

#[test]
fn kurt_of_one_row_is_null() {
    assert_eq!(one_phase("kurt", &col(&[5.0])), Datum::Null);
    assert_eq!(one_phase("skew", &col(&[5.0, 5.0])), Datum::Null);
    assert_eq!(one_phase("skew", &[]), Datum::Null);
    assert_eq!(one_phase("kurt", &[vec![None]]), Datum::Null);
}

/// The `support_type/regr_*` rows: y a FLOAT cast to DOUBLE, x a DOUBLE (the suite's data,
/// not approximations of π or e).
#[allow(clippy::approx_constant)]
fn regr_rows() -> Vec<Vec<Option<f64>>> {
    [(3.14f32, 2.718281828), (6.28f32, 3.141592653), (9.42f32, 1.618033988)]
        .iter()
        .map(|(y, x)| vec![Some(f64::from(*y)), Some(*x)])
        .collect()
}

#[test]
fn regr_merges_per_tablet_states_as_the_reference() {
    let r = regr_rows();
    // C++ holds k1=2 in one tablet's state and k1=1,3 in another's, then merges them
    let parts = vec![vec![r[1].clone()], vec![r[0].clone(), r[2].clone()]];
    assert_eq!(two_phases("regr_slope", &parts), Datum::Double(-2.792892135154929));
    // one state over the scan order gives the last digit differently
    assert_eq!(
        one_phase("regr_slope", &[r[1].clone(), r[0].clone(), r[2].clone()]),
        Datum::Double(-2.792892135154927)
    );
}

#[test]
fn regr_slope_skips_null_pairs() {
    let rows = vec![
        vec![Some(1.0), Some(1.0)],
        vec![None, Some(5.0)],
        vec![Some(3.0), Some(2.0)],
        vec![Some(9.0), None],
    ];
    assert_eq!(one_phase("regr_slope", &rows), Datum::Double(2.0));
    assert_eq!(one_phase("regr_intercept", &rows), Datum::Double(-1.0));
    // a constant x has no slope
    assert_eq!(one_phase("regr_slope", &pairs(&[(1.0, 2.0), (3.0, 2.0)])), Datum::Null);
}

#[test]
fn covar_samp_of_one_row_is_zero() {
    let three = pairs(&[(1.0, 2.0), (2.0, 3.0), (3.0, 4.0)]);
    assert_eq!(one_phase("covar", &three), Datum::Double(0.666666666666667));
    assert_eq!(one_phase("covar_samp", &three), Datum::Double(1.0));
    assert_eq!(one_phase("covar_samp", &pairs(&[(1.0, 2.0)])), Datum::Double(0.0));
    assert_eq!(one_phase("covar", &pairs(&[(1.0, 2.0)])), Datum::Double(0.0));
    // a row with a NULL is skipped; none accepted is NULL
    assert_eq!(one_phase("covar", &[vec![Some(1.0), None]]), Datum::Null);
}

#[test]
fn corr_of_constant_is_zero() {
    let g2 = pairs(&[(36.0, 21.0), (30.0, 22.0), (25.0, 20.0)]);
    assert_eq!(one_phase("corr", &g2), Datum::Double(0.4539206495016019));
    assert_eq!(one_phase("corr", &pairs(&[(25.0, 21.0), (25.0, 22.0)])), Datum::Double(0.0));
    // the stored order (k1 sorted) gives the reference's last digit
    assert_eq!(
        one_phase("corr_welford", &pairs(&[(30.0, 22.0), (25.0, 20.0), (36.0, 21.0)])),
        Datum::Double(0.4539206495016017)
    );
    assert_eq!(one_phase("corr_welford", &pairs(&[(25.0, 21.0), (25.0, 22.0)])), Datum::Double(0.0));
    assert_eq!(one_phase("corr", &[vec![None, Some(1.0)]]), Datum::Null);
}

#[test]
fn avg_weighted_zero_weights_is_nan() {
    let Datum::Double(v) = one_phase("avg_weighted", &pairs(&[(1.0, 1.0), (2.0, -1.0)])) else {
        panic!()
    };
    assert!(v.is_nan());
    assert_eq!(one_phase("avg_weighted", &pairs(&[(4.0, 1.0), (1.0, 3.0)])), Datum::Double(1.75));
    assert_eq!(one_phase("avg_weighted", &[vec![Some(1.0), None]]), Datum::Null);
}

#[test]
fn every_moment_merges_to_one_state_on_exact_inputs() {
    let rows = pairs(&[(1.0, 2.0), (2.0, 5.0), (4.0, 4.0), (8.0, 3.0)]);
    // Welford's running means divide: a merged state differs from one state in the last digit
    // (the value of a port of the C++ merge formula)
    assert_eq!(
        two_phases("corr_welford", &[rows[..1].to_vec(), rows[1..].to_vec()]),
        Datum::Double(-0.04170288281141495)
    );
    assert_eq!(one_phase("corr_welford", &rows), Datum::Double(-0.041702882811414876));
    for f in ["regr_slope", "regr_intercept", "corr", "covar", "covar_samp", "avg_weighted"] {
        assert_eq!(two_phases(f, &[rows[..1].to_vec(), rows[1..].to_vec()]), one_phase(f, &rows), "{f}");
        // an empty part sends no state
        assert_eq!(two_phases(f, &[vec![], rows.clone()]), one_phase(f, &rows), "{f}");
    }
    let one: Vec<_> = rows.iter().map(|r| vec![r[0]]).collect();
    for f in ["kurt", "skew"] {
        assert_eq!(two_phases(f, &[one[..2].to_vec(), one[2..].to_vec()]), one_phase(f, &one), "{f}");
    }
}

/// `non_nullable(x)` (`function_nullables.cpp`): the value, typed not nullable; a NULL fails.
#[test]
fn non_nullable_fails_on_a_null() {
    use doris_exec::eval::{check_read, eval_column, SlotMap};
    use doris_exec::ExecError;
    use doris_fixtures::build::call;
    let mut t = ct(&ty(T::INT));
    t.name = "k1".into();
    let block = Block {
        types: vec![t],
        rows: vec![vec![Datum::Int(3)], vec![Datum::Null]],
    };
    let slots = SlotMap([(1, 0)].into_iter().collect());
    let e = call("non_nullable", ty(T::INT), vec![slot(1, ty(T::INT))]);
    check_read(&e).unwrap();
    let one = Block {
        types: block.types.clone(),
        rows: vec![vec![Datum::Int(3)]],
    };
    let (to, v) = eval_column(&e, &one, &slots).unwrap();
    assert_eq!((to.nullable, v), (false, vec![Datum::Int(3)]));
    match eval_column(&e, &block, &slots) {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "INVALID_ARGUMENT");
            assert_eq!(msg, "There's NULL value in column k1 which is illegal for non_nullable");
        }
        other => panic!("{other:?}"),
    }
}
