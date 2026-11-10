//! Window functions over sorted rows, as the C++ BE's ANALYTIC_EVAL_NODE computes them: frames
//! clamped to the partition, peers by value (NULL = NULL, -0.0 = 0.0), values added in row order.
use doris_exec::aggregate::AggSpec;
use doris_exec::analytic::{check_frame, split_float_keys, window_columns, Frame, Window, WindowFn, WindowKind};
use doris_exec::value::ColType;
use doris_fixtures::build;
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

const N: Datum = Datum::Null;

fn ty(ptype: T) -> ColType {
    ColType {
        name: String::new(),
        ptype,
        len: 0,
        precision: 0,
        scale: 0,
        nullable: true,
    }
}

fn i(v: i128) -> Datum {
    Datum::Int(v)
}

fn d(v: f64) -> Datum {
    Datum::Double(v)
}

fn keys(values: &[Datum]) -> Vec<Vec<Datum>> {
    values.iter().map(|v| vec![v.clone()]).collect()
}

fn win(kind: WindowKind, ret: T) -> WindowFn {
    WindowFn {
        kind,
        ret: ty(ret),
        args: vec![],
    }
}

fn agg(function: &str, arg: T, ret: T) -> WindowFn {
    let spec = AggSpec {
        function: function.into(),
        merge: false,
        ret: ty(ret),
        param_types: vec![ty(arg)],
        args: vec![],
    };
    win(WindowKind::Agg(spec), ret)
}

/// One function over rows with the given partition and order keys and argument columns.
fn run(part: &[Datum], order: &[Datum], frame: Frame, f: &WindowFn, args: &[(T, Vec<Datum>)]) -> Vec<Datum> {
    let args: Vec<(ColType, Vec<Datum>)> = args.iter().map(|(t, v)| (ty(*t), v.clone())).collect();
    let (p, o) = (keys(part), keys(order));
    let pt = if part.is_empty() { vec![] } else { vec![ty(T::VARCHAR)] };
    let ot = if order.is_empty() { vec![] } else { vec![ty(T::DOUBLE)] };
    let p: Vec<Vec<Datum>> = if part.is_empty() { vec![vec![]; rows(&args, order)] } else { p };
    let o: Vec<Vec<Datum>> = if order.is_empty() { vec![vec![]; p.len()] } else { o };
    window_columns(&p, &pt, &o, &ot, &Window { frame: &frame, f, args: &args, batch_size: 4064 }).unwrap()
}

fn rows(args: &[(ColType, Vec<Datum>)], order: &[Datum]) -> usize {
    args.first().map_or(order.len(), |a| a.1.len())
}

fn s(v: &str) -> Datum {
    Datum::Bytes(v.as_bytes().to_vec())
}

const TO_CURRENT: Frame = Frame::Rows { start: None, end: Some(0) };

#[test]
fn row_number_rank_and_dense_rank_restart_per_partition() {
    let part = [s("a"), s("a"), s("a"), s("b")];
    let order = [d(1.0), d(1.0), d(2.0), d(5.0)];
    assert_eq!(
        run(&part, &order, TO_CURRENT, &win(WindowKind::RowNumber, T::BIGINT), &[]),
        [i(1), i(2), i(3), i(1)]
    );
    assert_eq!(
        run(&part, &order, Frame::RangeToCurrent, &win(WindowKind::Rank, T::BIGINT), &[]),
        [i(1), i(1), i(3), i(1)]
    );
    assert_eq!(
        run(&part, &order, Frame::RangeToCurrent, &win(WindowKind::DenseRank, T::BIGINT), &[]),
        [i(1), i(1), i(2), i(1)]
    );
}

#[test]
fn null_partition_keys_are_one_partition_and_zero_signs_are_peers() {
    assert_eq!(
        run(&[N, N], &[d(1.0), d(2.0)], TO_CURRENT, &win(WindowKind::RowNumber, T::BIGINT), &[]),
        [i(1), i(2)]
    );
    assert_eq!(
        run(
            &[s("a"), s("a")],
            &[d(-0.0), d(0.0)],
            Frame::RangeToCurrent,
            &win(WindowKind::Rank, T::BIGINT),
            &[]
        ),
        [i(1), i(1)]
    );
    // NaN is its own peer group, after the numbers
    assert_eq!(
        run(
            &[s("a"), s("a"), s("a")],
            &[d(1.0), d(f64::NAN), d(f64::NAN)],
            Frame::RangeToCurrent,
            &win(WindowKind::Rank, T::BIGINT),
            &[]
        ),
        [i(1), i(2), i(2)]
    );
}

#[test]
fn range_to_current_includes_peers() {
    let part = vec![s("a"); 3];
    let order = [d(1.0), d(1.0), d(2.0)];
    let x = vec![i(5), i(3), i(4)];
    assert_eq!(
        run(&part, &order, Frame::RangeToCurrent, &agg("min", T::INT, T::INT), &[(T::INT, x.clone())]),
        [i(3), i(3), i(3)]
    );
    assert_eq!(
        run(&part, &order, Frame::RangeToCurrent, &agg("sum", T::INT, T::BIGINT), &[(T::INT, x)]),
        [i(8), i(8), i(12)]
    );
}

#[test]
fn rows_frames_clamp_to_the_partition() {
    let part = vec![s("a"); 5];
    let order = [d(1.0), d(2.0), d(3.0), d(4.0), d(5.0)];
    let x = vec![i(4), i(2), i(9), i(1), i(7)];
    let min = agg("min", T::INT, T::INT);
    let f = Frame::Rows {
        start: Some(0),
        end: Some(3),
    };
    assert_eq!(run(&part, &order, f, &min, &[(T::INT, x.clone())]), [i(1), i(1), i(1), i(1), i(7)]);
    let f = Frame::Rows {
        start: Some(1),
        end: Some(2),
    };
    assert_eq!(run(&part, &order, f, &min, &[(T::INT, x.clone())]), [i(2), i(1), i(1), i(7), N]);
    let count = agg("count", T::INT, T::BIGINT);
    assert_eq!(run(&part, &order, f, &count, &[(T::INT, x)]), [i(2), i(2), i(2), i(1), i(0)]);
}

#[test]
fn ties_keep_input_order_in_a_rows_frame() {
    // test_min_max_window [8]: two rows tie on the order keys; the frame sees them as they came
    let part = vec![s("a"); 3];
    let order = [d(22.0), d(22.0), d(23.0)];
    let f = Frame::Rows {
        start: Some(0),
        end: Some(3),
    };
    let x = vec![i(0), i(1), i(5)];
    assert_eq!(run(&part, &order, f, &agg("min", T::INT, T::INT), &[(T::INT, x)]), [i(0), i(1), i(5)]);
}

#[test]
fn lead_and_lag_use_the_default_at_the_current_row() {
    let part = vec![s("a"); 3];
    let order = [d(1.0), d(2.0), d(3.0)];
    let args = [
        (T::INT, vec![i(1), i(2), i(3)]),
        (T::BIGINT, vec![i(1); 3]),
        (T::INT, vec![i(10), i(20), i(30)]),
    ];
    let lead = win(WindowKind::Lead(1), T::INT);
    let lag = win(WindowKind::Lag(1), T::INT);
    assert_eq!(
        run(&part, &order, Frame::Rows { start: None, end: Some(1) }, &lead, &args),
        [i(2), i(3), i(30)]
    );
    assert_eq!(
        run(&part, &order, Frame::Rows { start: None, end: Some(-1) }, &lag, &args),
        [i(10), i(1), i(2)]
    );
    // a lead does not cross into the next partition
    let part = [s("a"), s("b"), s("b")];
    assert_eq!(
        run(&part, &order, Frame::Rows { start: None, end: Some(1) }, &lead, &args),
        [i(10), i(3), i(30)]
    );
}

#[test]
fn first_and_last_value_with_and_without_nulls() {
    let part = vec![s("a"); 4];
    let order = [d(1.0), d(2.0), d(3.0), d(4.0)];
    let x = [(T::INT, vec![N, i(2), N, i(4)])];
    let first = |ignore_nulls| win(WindowKind::First { ignore_nulls }, T::INT);
    let last = |ignore_nulls| win(WindowKind::Last { ignore_nulls }, T::INT);
    assert_eq!(run(&part, &order, Frame::Partition, &first(false), &x), [N, N, N, N]);
    assert_eq!(run(&part, &order, Frame::Partition, &first(true), &x), [i(2), i(2), i(2), i(2)]);
    assert_eq!(run(&part, &order, Frame::Partition, &last(false), &x), [i(4), i(4), i(4), i(4)]);
    assert_eq!(run(&part, &order, TO_CURRENT, &last(false), &x), [N, i(2), N, i(4)]);
    assert_eq!(run(&part, &order, TO_CURRENT, &last(true), &x), [N, i(2), i(2), i(4)]);
    // a frame wholly past the partition gives NULL
    assert_eq!(
        run(
            &part,
            &order,
            Frame::Rows {
                start: Some(5),
                end: Some(6)
            },
            &first(false),
            &x
        ),
        [N, N, N, N]
    );
}

#[test]
fn float_sums_add_in_row_order() {
    let part = vec![s("a"); 3];
    let order = [d(1.0), d(2.0), d(3.0)];
    let got = run(
        &part,
        &order,
        TO_CURRENT,
        &agg("sum", T::DOUBLE, T::DOUBLE),
        &[(T::DOUBLE, vec![d(0.1), d(0.2), d(0.3)])],
    );
    let bits = |v: &Datum| match v {
        Datum::Double(f) => f.to_bits(),
        other => panic!("{other:?}"),
    };
    assert_eq!(
        got.iter().map(bits).collect::<Vec<_>>(),
        [0.1f64.to_bits(), (0.1f64 + 0.2).to_bits(), (0.1f64 + 0.2 + 0.3).to_bits()]
    );
}

#[test]
fn a_whole_partition_frame_without_order_keys() {
    let part = [s("a"), s("a"), s("b")];
    let x = vec![i(3), i(1), i(7)];
    assert_eq!(
        run(
            &part,
            &[],
            Frame::RangeToCurrent,
            &win(WindowKind::Rank, T::BIGINT),
            &[(T::INT, x.clone())]
        ),
        [i(1), i(1), i(1)]
    );
    assert_eq!(
        run(&part, &[], Frame::RangeToCurrent, &agg("min", T::INT, T::INT), &[(T::INT, x)]),
        [i(1), i(1), i(7)]
    );
}

#[test]
fn two_arguments_mean_ignore_nulls_whatever_the_flag() {
    // C++ picks the ignore-nulls first_value/last_value whenever there are 2 arguments
    // (aggregate_function_window.cpp); the FE rewrites (x, false) to (x)
    for flag in [true, false] {
        let e = build::aggregate(
            "first_value",
            build::ty(T::INT),
            vec![build::ty(T::INT), build::ty(T::BOOLEAN)],
            false,
            vec![build::slot(1, build::ty(T::INT)), build::boolean(flag)],
        );
        assert_eq!(
            WindowFn::from_expr(&e).unwrap().kind,
            WindowKind::First { ignore_nulls: true },
            "flag {flag}"
        );
    }
    let e = build::aggregate(
        "last_value",
        build::ty(T::INT),
        vec![build::ty(T::INT)],
        false,
        vec![build::slot(1, build::ty(T::INT))],
    );
    assert_eq!(WindowFn::from_expr(&e).unwrap().kind, WindowKind::Last { ignore_nulls: false });
}

#[test]
fn multi_distinct_count_is_not_a_window_aggregate() {
    let e = build::aggregate(
        "multi_distinct_count",
        build::ty(T::BIGINT),
        vec![build::ty(T::INT)],
        false,
        vec![build::slot(1, build::ty(T::INT))],
    );
    let err = WindowFn::from_expr(&e).unwrap_err().to_string();
    assert!(err.contains("multi_distinct_count over a window"), "{err}");
}

#[test]
fn lead_and_lag_need_the_frame_the_fe_forces() {
    let lead = win(WindowKind::Lead(1), T::INT);
    let lag = win(WindowKind::Lag(1), T::INT);
    assert!(check_frame(&lead, &Frame::Rows { start: None, end: Some(1) }).is_ok());
    assert!(check_frame(&lag, &Frame::Rows { start: None, end: Some(-1) }).is_ok());
    for (f, frame) in [
        (&lead, Frame::Rows { start: None, end: Some(2) }),
        (&lead, Frame::Partition),
        (&lag, Frame::Rows { start: None, end: Some(0) }),
        (
            &lag,
            Frame::Rows {
                start: Some(-1),
                end: Some(-1),
            },
        ),
    ] {
        let err = check_frame(f, &frame).unwrap_err().to_string();
        assert!(err.contains(&format!("{} over a window other than", f.name())), "{frame:?}: {err}");
    }
}

#[test]
fn a_sum_over_an_empty_frame_with_a_non_nullable_type_is_zero() {
    let mut f = agg("sum", T::INT, T::BIGINT);
    if let WindowKind::Agg(spec) = &mut f.kind {
        spec.ret.nullable = false;
    }
    let got = run(
        &[s("a"), s("a")],
        &[d(1.0), d(2.0)],
        Frame::Rows {
            start: Some(1),
            end: Some(1),
        },
        &f,
        &[(T::INT, vec![i(4), i(5)])],
    );
    assert_eq!(got, [i(5), i(0)]);
}

#[test]
fn ignore_nulls_last_value_past_the_partition_keeps_the_last_non_null() {
    let x = [(T::INT, vec![i(1), N, N, i(9)])];
    let f = win(WindowKind::Last { ignore_nulls: true }, T::INT);
    let part = [s("a"), s("a"), s("a"), s("b")];
    let got = run(
        &part,
        &[d(1.0), d(2.0), d(3.0), d(4.0)],
        Frame::Rows { start: None, end: Some(1) },
        &f,
        &x,
    );
    assert_eq!(got, [i(1), i(1), i(1), i(9)]);
}

#[test]
fn float_keys_equal_in_value_but_not_in_bits_are_found() {
    // C++ hashes partition keys by their bytes, so -0.0 and 0.0 (or two NaNs) can reach
    // different instances, where doris-rs's exchange sends them to one
    let t = [ty(T::DOUBLE)];
    assert!(split_float_keys(&keys(&[d(-0.0), d(0.0)]), &t));
    assert!(split_float_keys(&keys(&[d(f64::NAN), d(-f64::NAN)]), &t));
    assert!(!split_float_keys(&keys(&[d(0.0), d(0.0), d(1.0)]), &t));
    assert!(!split_float_keys(&keys(&[N, N]), &t));
    assert!(split_float_keys(&keys(&[Datum::Float(-0.0), Datum::Float(0.0)]), &[ty(T::FLOAT)]));
}

#[test]
fn batch1_aggregates_are_not_window_aggregates() {
    for (f, t) in [("sum0", T::BIGINT), ("stddev", T::DOUBLE), ("group_concat", T::VARCHAR), ("ndv", T::INT), ("window_funnel_v2", T::INT)] {
        let e = build::aggregate(f, build::ty(t), vec![build::ty(t)], false, vec![build::slot(1, build::ty(t))]);
        let err = WindowFn::from_expr(&e).unwrap_err().to_string();
        assert!(err.contains(&format!("{f} over a window")), "{err}");
    }
}

#[test]
fn ntile_splits_like_cpp() {
    // 7 rows into 3: the first 7 % 3 buckets take one more row
    let order: Vec<Datum> = (1..=7).map(|v| d(v as f64)).collect();
    let got = run(&[], &order, TO_CURRENT, &win(WindowKind::Ntile(3), T::BIGINT), &[]);
    assert_eq!(got, [1, 1, 1, 2, 2, 3, 3].map(i).to_vec());
    // more buckets than rows: row i is bucket i + 1; restarts per partition
    let part = [s("a"), s("a"), s("b")];
    let got = run(&part, &[d(1.0), d(2.0), d(1.0)], TO_CURRENT, &win(WindowKind::Ntile(5), T::BIGINT), &[]);
    assert_eq!(got, vec![i(1), i(2), i(1)]);
}

#[test]
fn ntile_parses_its_literal() {
    let e = build::aggregate("ntile", build::ty(T::BIGINT), vec![build::ty(T::TINYINT)], false, vec![build::int(3, build::ty(T::TINYINT))]);
    assert_eq!(WindowFn::from_expr(&e).unwrap().kind, WindowKind::Ntile(3));
}

#[test]
fn ntile_refuses_a_partition_past_one_block() {
    let order: Vec<Datum> = (0..5000).map(|v| d(v as f64)).collect();
    let (p, o) = (vec![vec![]; order.len()], keys(&order));
    let r = window_columns(&p, &[], &o, &[ty(T::DOUBLE)], &Window { frame: &TO_CURRENT, f: &win(WindowKind::Ntile(3), T::BIGINT), args: &[], batch_size: 4064 });
    assert!(r.is_err());
}

#[test]
fn ntile_refuses_past_the_querys_batch_size() {
    let order: Vec<Datum> = (0..2000).map(|v| d(v as f64)).collect();
    let (p, o) = (vec![vec![]; order.len()], keys(&order));
    let f = win(WindowKind::Ntile(3), T::BIGINT);
    assert!(window_columns(&p, &[], &o, &[ty(T::DOUBLE)], &Window { frame: &TO_CURRENT, f: &f, args: &[], batch_size: 1024 }).is_err());
    assert!(window_columns(&p, &[], &o, &[ty(T::DOUBLE)], &Window { frame: &TO_CURRENT, f: &f, args: &[], batch_size: 4064 }).is_ok());
}
