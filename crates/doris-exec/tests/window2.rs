//! Batch 6's window functions as the C++ BE computes them: the stddev family's finalizers,
//! `percentile_approx` below the size where its t-digest merges points, `percent_rank`,
//! `cume_dist` and the distinct combinator (`multi_distinct_sum`/`group_concat`). See
//! `docs/superpowers/research/2026-11-12-m6-s7b6-aggregates.md`; expected values are the
//! recorded rows or follow from the rules there.
use doris_exec::aggregate::AggSpec;
use doris_exec::analytic::{check_frame, check_window, window_columns, Frame, Window, WindowFn, WindowKind};
use doris_exec::value::ColType;
use doris_exec::ExecError;
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

fn s(v: &str) -> Datum {
    Datum::Bytes(v.as_bytes().to_vec())
}

fn win(kind: WindowKind, ret: T) -> WindowFn {
    WindowFn {
        kind,
        ret: ty(ret),
        args: vec![],
    }
}

fn agg(function: &str, args: &[T], ret: T) -> WindowFn {
    let spec = AggSpec {
        function: function.into(),
        merge: false,
        ret: ty(ret),
        param_types: args.iter().map(|t| ty(*t)).collect(),
        args: vec![],
    };
    win(WindowKind::Agg(spec), ret)
}

/// One function over rows with the given partition and order keys and argument columns.
fn try_run(part: &[Datum], order: &[Datum], frame: Frame, f: &WindowFn, args: &[(T, Vec<Datum>)]) -> Result<Vec<Datum>, ExecError> {
    let args: Vec<(ColType, Vec<Datum>)> = args.iter().map(|(t, v)| (ty(*t), v.clone())).collect();
    let n = args.first().map_or(part.len().max(order.len()), |a| a.1.len());
    let p: Vec<Vec<Datum>> = if part.is_empty() {
        vec![vec![]; n]
    } else {
        part.iter().map(|v| vec![v.clone()]).collect()
    };
    let o: Vec<Vec<Datum>> = if order.is_empty() {
        vec![vec![]; n]
    } else {
        order.iter().map(|v| vec![v.clone()]).collect()
    };
    let pt = if part.is_empty() { vec![] } else { vec![ty(T::VARCHAR)] };
    let ot = if order.is_empty() { vec![] } else { vec![ty(T::DOUBLE)] };
    window_columns(
        &p,
        &pt,
        &o,
        &ot,
        &Window {
            frame: &frame,
            f,
            args: &args,
            batch_size: 4064,
        },
    )
}

fn run(part: &[Datum], order: &[Datum], frame: Frame, f: &WindowFn, args: &[(T, Vec<Datum>)]) -> Vec<Datum> {
    try_run(part, order, frame, f, args).unwrap()
}

const SLIDE_PREV: Frame = Frame::Rows {
    start: Some(-1),
    end: Some(-1),
};
const TO_CURRENT: Frame = Frame::Rows { start: None, end: Some(0) };
const ONLY_CURRENT: Frame = Frame::Rows {
    start: Some(0),
    end: Some(0),
};

#[test]
fn the_stddev_family_runs_over_windows() {
    for f in ["stddev", "stddev_samp", "variance", "variance_samp"] {
        let dt = build::ty(T::DOUBLE);
        let e = build::aggregate(f, dt.clone(), vec![dt.clone()], false, vec![build::slot(1, dt.clone())]);
        WindowFn::from_expr(&e).unwrap_or_else(|e| panic!("{f}: {e}"));
    }
}

/// Welford's state over {1989, 1991}: m2 = 2, so the population variance is 1 and the sample
/// variance 2 (recorded: `variance_samp` of two rows {1989, 1991} is `2`).
#[test]
fn sample_and_population_finalizers() {
    let x = vec![d(1989.0), d(1991.0)];
    let whole = |f: &str| run(&[], &[], Frame::Partition, &agg(f, &[T::DOUBLE], T::DOUBLE), &[(T::DOUBLE, x.clone())]);
    assert_eq!(whole("variance"), vec![d(1.0), d(1.0)]);
    assert_eq!(whole("stddev"), vec![d(1.0), d(1.0)]);
    assert_eq!(whole("variance_samp"), vec![d(2.0), d(2.0)]);
    assert_eq!(whole("stddev_samp"), vec![d(std::f64::consts::SQRT_2), d(std::f64::consts::SQRT_2)]);
    for f in ["stddev", "stddev_samp", "variance", "variance_samp"] {
        let g = agg(f, &[T::DOUBLE], T::DOUBLE);
        // one row: 0 for all four (recorded: `current row and current row` gives 0)
        assert_eq!(run(&[], &[], ONLY_CURRENT, &g, &[(T::DOUBLE, x.clone())]), vec![d(0.0), d(0.0)], "{f}");
        // an empty frame, or only NULL rows: NULL
        assert_eq!(run(&[], &[], SLIDE_PREV, &g, &[(T::DOUBLE, x.clone())]), vec![N, d(0.0)], "{f}");
        assert_eq!(run(&[], &[], Frame::Partition, &g, &[(T::DOUBLE, vec![N, N])]), vec![N, N], "{f}");
    }
    // a cumulative frame adds rows in order
    let g = agg("variance_samp", &[T::DOUBLE], T::DOUBLE);
    assert_eq!(
        run(&[], &[], TO_CURRENT, &g, &[(T::DOUBLE, vec![d(1.0), d(3.0), d(5.0)])]),
        vec![d(0.0), d(2.0), d(4.0)]
    );
}

fn approx(x: Vec<Datum>, q: f64, frame: Frame) -> Result<Vec<Datum>, ExecError> {
    let n = x.len();
    let f = agg("percentile_approx", &[T::DOUBLE, T::DOUBLE, T::DOUBLE], T::DOUBLE);
    try_run(
        &[],
        &[],
        frame,
        &f,
        &[(T::DOUBLE, x), (T::DOUBLE, vec![d(q); n]), (T::DOUBLE, vec![d(4096.0); n])],
    )
}

#[test]
fn percentile_approx_below_the_merge_size() {
    let v = |xs: &[f64]| xs.iter().map(|x| d(*x)).collect::<Vec<_>>();
    // the median: the middle value, or the f32 mean of the middle two
    assert_eq!(approx(v(&[1991.0, 1989.0]), 0.5, Frame::Partition).unwrap()[0], d(1990.0));
    assert_eq!(approx(v(&[3.0, 1.0, 2.0]), 0.5, Frame::Partition).unwrap()[0], d(2.0));
    assert_eq!(approx(v(&[7.0]), 0.5, Frame::Partition).unwrap()[0], d(7.0));
    // values are kept as f32
    assert_eq!(approx(v(&[0.1]), 0.5, Frame::Partition).unwrap()[0], d(f64::from(0.1f32)));
    let ten = v(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);
    assert_eq!(approx(ten.clone(), 0.9, Frame::Partition).unwrap()[0], d(9.5));
    assert_eq!(approx(ten.clone(), 0.99, Frame::Partition).unwrap()[0], d(10.0));
    assert_eq!(approx(ten, 0.0, Frame::Partition).unwrap()[0], d(1.0));
    // NaN is dropped: a state of only NaN gives 0, not NULL; only NULL rows give NULL
    assert_eq!(approx(v(&[f64::NAN]), 0.5, Frame::Partition).unwrap()[0], d(0.0));
    assert_eq!(approx(vec![N, N], 0.5, Frame::Partition).unwrap(), vec![N, N]);
    // an empty frame is NULL
    assert_eq!(approx(v(&[1.0, 2.0]), 0.5, SLIDE_PREV).unwrap(), vec![N, d(1.0)]);
    // a quantile outside [0, 1]
    match approx(v(&[1.0]), 1.5, Frame::Partition) {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "INVALID_ARGUMENT");
            assert_eq!(msg, "quantile in func percentile should in [0, 1], but real data is:1.500000");
        }
        other => panic!("{other:?}"),
    }
    // from 2048 values the digest may merge points: refused
    let big: Vec<Datum> = (0..2048).map(|x| d(x as f64)).collect();
    let r = approx(big, 0.5, Frame::Partition);
    assert!(matches!(r, Err(ExecError::Unsupported { .. })), "{r:?}");
}

/// The recorded `PARTITION BY four ORDER BY ten` partitions.
#[test]
fn percent_rank_and_cume_dist() {
    let part = [s("a"), s("a"), s("a"), s("b"), s("b"), s("b"), s("b"), s("c"), s("d"), s("d")];
    let order = [d(0.0), d(0.0), d(4.0), d(1.0), d(1.0), d(3.0), d(7.0), d(2.0), d(5.0), d(9.0)];
    let pr = run(&part, &order, Frame::RangeToCurrent, &win(WindowKind::PercentRank, T::DOUBLE), &[]);
    let two_thirds = 0.6666666666666666;
    assert_eq!(
        pr,
        vec![d(0.0), d(0.0), d(1.0), d(0.0), d(0.0), d(two_thirds), d(1.0), d(0.0), d(0.0), d(1.0)]
    );
    let cd = run(&part, &order, Frame::RangeToCurrent, &win(WindowKind::CumeDist, T::DOUBLE), &[]);
    assert_eq!(
        cd,
        vec![
            d(two_thirds),
            d(two_thirds),
            d(1.0),
            d(0.5),
            d(0.5),
            d(0.75),
            d(1.0),
            d(1.0),
            d(0.5),
            d(1.0)
        ]
    );
    // no ORDER BY: one peer group
    assert_eq!(
        run(
            &[],
            &[],
            Frame::RangeToCurrent,
            &win(WindowKind::PercentRank, T::DOUBLE),
            &[(T::INT, vec![i(1), i(2)])]
        ),
        vec![d(0.0), d(0.0)]
    );
    assert_eq!(
        run(
            &[],
            &[],
            Frame::RangeToCurrent,
            &win(WindowKind::CumeDist, T::DOUBLE),
            &[(T::INT, vec![i(1), i(2)])]
        ),
        vec![d(1.0), d(1.0)]
    );
    // the frame the FE forces, and their names
    for (name, kind) in [("percent_rank", WindowKind::PercentRank), ("cume_dist", WindowKind::CumeDist)] {
        let e = build::aggregate(name, build::ty(T::DOUBLE), vec![], false, vec![]);
        assert_eq!(WindowFn::from_expr(&e).unwrap().kind, kind);
        assert!(check_frame(&win(kind.clone(), T::DOUBLE), &TO_CURRENT).is_err());
        check_window(&win(kind, T::DOUBLE), &Frame::RangeToCurrent, true).unwrap();
    }
}

/// `multi_distinct_window`: the whole partition, and `ROWS 1 PRECEDING AND 1 PRECEDING` over
/// (1, 10, 'a'), (2, 20, 'b').
#[test]
fn multi_distinct_in_frames() {
    let sum = agg("multi_distinct_sum", &[T::INT], T::BIGINT);
    let concat = agg("multi_distinct_group_concat", &[T::VARCHAR], T::VARCHAR);
    let count = agg("multi_distinct_count", &[T::INT], T::BIGINT);
    assert_eq!(run(&[], &[], Frame::Partition, &sum, &[(T::INT, vec![i(2); 4])]), vec![i(2); 4]);
    assert_eq!(
        run(&[], &[], Frame::Partition, &concat, &[(T::VARCHAR, vec![s("a"), s("a")])]),
        vec![s("a"); 2]
    );
    assert_eq!(run(&[], &[], SLIDE_PREV, &sum, &[(T::INT, vec![i(10), i(20)])]), vec![N, i(10)]);
    assert_eq!(run(&[], &[], SLIDE_PREV, &concat, &[(T::VARCHAR, vec![s("a"), s("b")])]), vec![N, s("a")]);
    assert_eq!(run(&[], &[], SLIDE_PREV, &count, &[(T::INT, vec![i(10), i(20)])]), vec![i(0), i(1)]);
    // distinct values, NULL skipped
    assert_eq!(
        run(&[], &[], Frame::Partition, &sum, &[(T::INT, vec![i(1), i(2), i(1), N])]),
        vec![i(3); 4]
    );
    // count runs over every frame (a plain set); sum and group_concat not over cumulative ones,
    // where C++ adds the values each step brings again
    check_frame(&count, &SLIDE_PREV).unwrap();
    check_frame(&count, &TO_CURRENT).unwrap();
    for f in [&sum, &concat] {
        check_frame(f, &SLIDE_PREV).unwrap();
        check_frame(f, &Frame::Partition).unwrap();
        assert!(check_frame(f, &TO_CURRENT).is_err());
        assert!(check_window(f, &Frame::RangeToCurrent, true).is_err());
        check_window(f, &Frame::RangeToCurrent, false).unwrap();
    }
    // two distinct strings come out in C++'s hash-set order: refused
    assert!(matches!(
        try_run(&[], &[], Frame::Partition, &concat, &[(T::VARCHAR, vec![s("a"), s("b")])]),
        Err(ExecError::Unsupported { .. })
    ));
    // a DOUBLE sum adds in hash-set order: refused
    let e = build::aggregate(
        "multi_distinct_sum",
        build::ty(T::DOUBLE),
        vec![build::ty(T::DOUBLE)],
        false,
        vec![build::slot(1, build::ty(T::DOUBLE))],
    );
    assert!(WindowFn::from_expr(&e).is_err());
}

// final review fixes

/// Two partial states merged past the limit: the digest may have merged points (I1).
#[test]
fn merged_percentile_approx_states_past_the_limit_are_refused() {
    use doris_exec::stats_agg::Stats;
    let params = [ty(T::DOUBLE), ty(T::DOUBLE)];
    let state = |from: usize| {
        let mut s = Stats::new("percentile_approx", &params).unwrap();
        for x in from..from + 1500 {
            s.add(&[&d(x as f64), &d(0.5)]).unwrap();
        }
        s
    };
    let mut a = state(0);
    assert!(matches!(a.merge(&state(1500)), Err(ExecError::Unsupported { .. })));
    // under the limit the merge stands
    let mut small = Stats::new("percentile_approx", &params).unwrap();
    small.add(&[&d(1.0), &d(0.5)]).unwrap();
    let mut other = Stats::new("percentile_approx", &params).unwrap();
    other.add(&[&d(3.0), &d(0.5)]).unwrap();
    small.merge(&other).unwrap();
    assert_eq!(small.finish(true, &ty(T::DOUBLE)), d(2.0));
}

/// Infinite values follow C++'s `std::min`/`std::max` and the `_min` formula (I2).
#[test]
fn percentile_approx_with_infinities_as_cpp() {
    let v = |xs: &[f64]| xs.iter().map(|x| d(*x)).collect::<Vec<_>>();
    // weight 0 on -inf: x is NaN; std::min(NaN, x2) is NaN, std::max(x1, NaN) is x1
    assert_eq!(
        approx(v(&[f64::NEG_INFINITY, 5.0]), 0.75, Frame::Partition).unwrap()[0],
        d(f64::NEG_INFINITY)
    );
    // the first half-centroid: _min + 2·index/w0·(mean0 - _min) is NaN with _min -inf, written as 0
    assert_eq!(approx(v(&[f64::NEG_INFINITY, 5.0]), 0.1, Frame::Partition).unwrap()[0], d(0.0));
    // finite values are unchanged by the literal formula
    assert_eq!(approx(v(&[2.0, 5.0]), 0.1, Frame::Partition).unwrap()[0], d(2.0));
}
