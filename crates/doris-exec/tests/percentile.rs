//! `percentile` and `percentile_array` as the C++ BE computes them under
//! `new_version_percentile` (`PercentileExactState`, `aggregate_function_percentile.h:434-619`),
//! and `element_at`: see `docs/superpowers/research/2026-11-10-m6-s7b3-aggregates.md`.
use doris_exec::aggregate::{AggSpec, Aggregation};
use doris_exec::analytic::{window_columns, Frame, Window, WindowFn, WindowKind};
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::exchange::Chunk;
use doris_exec::value::ColType;
use doris_exec::ExecError;
use doris_fixtures::build::{aggregate, array_literal, array_of, call, float, int, scalar, slot, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn ct(t: &TTypeDesc) -> ColType {
    ColType::from_type_desc("", t, true).unwrap()
}

fn d() -> TTypeDesc {
    ty(T::DOUBLE)
}

fn levels(qs: &[f64]) -> TExpr {
    array_literal(d(), qs.iter().map(|q| float(*q, d())).collect())
}

/// `percentile(x, q)` or `percentile_array(x, [q…])` over slot 1 of type `t`.
fn spec(name: &str, t: &TTypeDesc, qs: &[f64], merge: bool) -> AggSpec {
    let (ret, level_type, level) = if name == "percentile" {
        (d(), d(), float(qs[0], d()))
    } else {
        (array_of(d()), array_of(d()), levels(qs))
    };
    let args = if merge {
        vec![slot(10, scalar(T::VARCHAR, 65533, 0, 0))]
    } else {
        vec![slot(1, t.clone()), level]
    };
    AggSpec::from_expr(&aggregate(name, ret, vec![t.clone(), level_type], merge, args)).unwrap()
}

fn input(t: &TTypeDesc, values: &[Datum]) -> Chunk {
    Chunk {
        block: Block {
            types: vec![ct(t)],
            rows: values.iter().map(|v| vec![v.clone()]).collect(),
        },
        slots: vec![1],
    }
}

fn one_phase(name: &str, t: &TTypeDesc, qs: &[f64], values: &[Datum]) -> Result<Datum, ExecError> {
    let a = Aggregation {
        group_by: vec![],
        functions: vec![spec(name, t, qs, false)],
        finalize: true,
    };
    Ok(a.run(&input(t, values))?.rows.remove(0).remove(0))
}

fn two_phases(name: &str, t: &TTypeDesc, qs: &[f64], parts: &[&[Datum]]) -> Datum {
    let mut partials = Vec::new();
    for p in parts {
        let a = Aggregation {
            group_by: vec![],
            functions: vec![spec(name, t, qs, false)],
            finalize: false,
        };
        partials.extend(a.run(&input(t, p)).unwrap().rows);
    }
    let merge = Aggregation {
        group_by: vec![],
        functions: vec![spec(name, t, qs, true)],
        finalize: true,
    };
    let chunk = Chunk {
        block: Block {
            types: vec![ct(&scalar(T::VARCHAR, 65533, 0, 0))],
            rows: partials,
        },
        slots: vec![10],
    };
    merge.run(&chunk).unwrap().rows.remove(0).remove(0)
}

fn ints(v: &[i128]) -> Vec<Datum> {
    v.iter().map(|x| Datum::Int(*x)).collect()
}

fn dbl(v: &[f64]) -> Datum {
    Datum::Array(v.iter().map(|x| Datum::Double(*x)).collect())
}

/// The recorded FLOAT column, not approximations of π.
#[allow(clippy::approx_constant)]
#[test]
fn percentile_interpolates() {
    let t = ty(T::INT);
    let p = |q: f64, v: &[i128]| one_phase("percentile", &t, &[q], &ints(v)).unwrap();
    // u = (n-1)q; s[i] + (u-i)(s[i+1]-s[i]); past the last pair the maximum
    assert_eq!(p(0.5, &[4, 1, 3, 2]), Datum::Double(2.5));
    assert_eq!(p(1.0, &[4, 1, 3, 2]), Datum::Double(4.0));
    assert_eq!(p(0.0, &[4, 1, 3, 2]), Datum::Double(1.0));
    assert_eq!(p(0.9, &[7]), Datum::Double(7.0));
    // NULL rows are skipped; none accepted is NULL
    let with_null = vec![Datum::Null, Datum::Int(5), Datum::Int(1)];
    assert_eq!(one_phase("percentile", &t, &[0.5], &with_null).unwrap(), Datum::Double(3.0));
    assert_eq!(one_phase("percentile", &t, &[0.5], &[Datum::Null]).unwrap(), Datum::Null);
    // the recorded FLOAT column (3.14, 6.28, 9.42) at [0.5, 0.8]
    let f = ty(T::FLOAT);
    let floats: Vec<Datum> = [3.14f32, 6.28, 9.42].iter().map(|x| Datum::Float(*x)).collect();
    assert_eq!(
        one_phase("percentile_array", &f, &[0.5, 0.8], &floats).unwrap(),
        dbl(&[6.28000020980835, 8.164000129699707])
    );
}

#[test]
fn percentile_level_out_of_range_errors() {
    match one_phase("percentile", &ty(T::INT), &[1.5], &ints(&[1])) {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "INVALID_ARGUMENT");
            assert_eq!(msg, "quantile in func percentile should in [0, 1], but real data is:1.500000");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn percentile_merges_to_one_state() {
    let t = ty(T::BIGINT);
    let all = ints(&[10, 40, 20, 30, 50]);
    let one = one_phase("percentile", &t, &[0.3], &all).unwrap();
    assert_eq!(two_phases("percentile", &t, &[0.3], &[&all[..2], &all[2..]]), one);
    assert_eq!(two_phases("percentile", &t, &[0.3], &[&[], &all]), one);
    assert_eq!(
        two_phases("percentile_array", &t, &[0.3, 0.9], &[&all[..1], &all[1..]]),
        one_phase("percentile_array", &t, &[0.3, 0.9], &all).unwrap()
    );
}

#[test]
fn percentile_array_levels_in_order() {
    let t = ty(T::INT);
    // levels are answered at their own positions, whatever their order
    assert_eq!(
        one_phase("percentile_array", &t, &[0.9, 0.0, 0.5], &ints(&[1, 2, 3])).unwrap(),
        dbl(&[2.8, 1.0, 2.0])
    );
    // an empty input is an empty array, never NULL
    assert_eq!(one_phase("percentile_array", &t, &[0.5], &[Datum::Null]).unwrap(), dbl(&[]));
}

#[test]
fn element_at_indexes() {
    let arr = || slot(1, array_of(d()));
    let at = |i: i64| call("element_at", d(), vec![arr(), int(i, ty(T::INT))]);
    let block = Block {
        types: vec![ct(&array_of(d()))],
        rows: vec![vec![dbl(&[1.5, 2.5, 3.5])], vec![Datum::Null]],
    };
    let slots = SlotMap([(1, 0)].into_iter().collect());
    let eval = |e: TExpr| {
        check_read(&e).unwrap();
        eval_column(&e, &block, &slots).unwrap().1
    };
    assert_eq!(eval(at(1)), vec![Datum::Double(1.5), Datum::Null]);
    assert_eq!(eval(at(3)), vec![Datum::Double(3.5), Datum::Null]);
    assert_eq!(eval(at(-1)), vec![Datum::Double(3.5), Datum::Null]);
    assert_eq!(eval(at(0)), vec![Datum::Null, Datum::Null]);
    assert_eq!(eval(at(4)), vec![Datum::Null, Datum::Null]);
}

#[test]
fn percentile_over_a_window() {
    let t = ty(T::TINYINT);
    let mut s = spec("percentile", &t, &[0.5], false);
    s.args = vec![];
    let f = WindowFn {
        kind: WindowKind::Agg(s),
        ret: ct(&d()),
        args: vec![],
    };
    let args = vec![(ct(&t), ints(&[3, 1, 2])), (ct(&d()), vec![Datum::Double(0.5); 3])];
    let frame = Frame::Rows { start: None, end: Some(0) };
    let rows = vec![vec![]; 3];
    let out = window_columns(
        &rows,
        &[],
        &rows,
        &[],
        &Window {
            frame: &frame,
            f: &f,
            args: &args,
            batch_size: 4064,
        },
    )
    .unwrap();
    // re-finalized after each row: [3], [3,1], [3,1,2]
    assert_eq!(out, vec![Datum::Double(3.0), Datum::Double(2.0), Datum::Double(2.0)]);
}

// review fixes

/// Only percentile_array's levels carry an array into an aggregate: `max` of one would keep the
/// first row's array, as arrays have no order here.
#[test]
fn an_array_argument_of_another_aggregate_is_refused() {
    use doris_fixtures::build::string;
    let vc = scalar(T::VARCHAR, 65533, 0, 0);
    let arr = array_of(vc.clone());
    let split = call("split_by_string", arr.clone(), vec![string("a,b"), string(",")]);
    let e = aggregate("max", arr.clone(), vec![arr], false, vec![split]);
    let err = AggSpec::from_expr(&e).unwrap_err().to_string();
    assert!(err.contains("ARRAY"), "{err}");
    assert!(doris_exec::eval::check_nodes(&e, true).is_err());
    // percentile_array's own levels still are
    let ok = aggregate(
        "percentile_array",
        array_of(d()),
        vec![ty(T::INT), array_of(d())],
        false,
        vec![slot(1, ty(T::INT)), levels(&[0.5])],
    );
    doris_exec::eval::check_nodes(&ok, true).unwrap();
}

#[test]
fn percentile_of_a_decimal_is_refused() {
    let dec = scalar(T::DECIMAL64, -1, 10, 2);
    let e = aggregate("percentile", d(), vec![dec.clone(), d()], false, vec![slot(1, dec), float(0.5, d())]);
    let a = Aggregation {
        group_by: vec![],
        functions: vec![AggSpec::from_expr(&e).unwrap()],
        finalize: true,
    };
    assert!(a.run(&input(&scalar(T::DECIMAL64, -1, 10, 2), &[Datum::Int(150)])).is_err());
}

#[test]
fn a_nan_level_is_accepted() {
    // check_quantile is `q < 0 || q > 1`, which NaN passes
    let Datum::Double(v) = one_phase("percentile", &ty(T::INT), &[f64::NAN], &ints(&[1, 2])).unwrap() else {
        panic!()
    };
    assert!(v.is_nan());
}

/// The x86 BE is built with GCC's default `-ffp-contract=fast`, so the interpolation
/// `s[i] + (u-i)·(s[i+1]-s[i])` is one fused multiply-add: test_select_stddev_variance_window
/// recorded -6349.399999999999 for SMALLINT (-32767, 255) at 0.8, where two roundings give
/// -6349.399999999998.
#[test]
fn percentile_interpolates_with_one_rounding() {
    let t = ty(T::SMALLINT);
    assert_eq!(
        one_phase("percentile", &t, &[0.8], &ints(&[-32767, 255])).unwrap(),
        Datum::Double(-6349.399999999999)
    );
    // the same rows' other recorded values are unchanged
    assert_eq!(one_phase("percentile", &t, &[0.8], &ints(&[1989, 1991])).unwrap(), Datum::Double(1990.6));
}
