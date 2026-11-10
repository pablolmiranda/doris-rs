//! Aggregation: one phase, and two phases with the partial states handed over in process.
use doris_exec::aggregate::{AggSpec, Aggregation};
use doris_exec::block::Block;
use doris_exec::exchange::Chunk;
use doris_exec::value::ColType;
use doris_fixtures::build::{aggregate, scalar, slot, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn ct(t: &TTypeDesc) -> ColType {
    ColType::from_type_desc("", t, true).unwrap()
}

/// One column of values in slot 1.
fn input(t: &TTypeDesc, values: &[Datum]) -> Chunk {
    Chunk {
        block: Block {
            types: vec![ct(t)],
            rows: values.iter().map(|v| vec![v.clone()]).collect(),
        },
        slots: vec![1],
    }
}

fn spec(name: &str, ret: TTypeDesc, params: Vec<TTypeDesc>, merge: bool, args: Vec<TExpr>) -> AggSpec {
    AggSpec::from_expr(&aggregate(name, ret, params, merge, args)).unwrap()
}

fn one_phase(functions: Vec<AggSpec>, chunk: &Chunk) -> Vec<Vec<Datum>> {
    Aggregation {
        group_by: vec![],
        functions,
        finalize: true,
    }
    .run(chunk)
    .unwrap()
    .rows
}

fn ints(v: &[Option<i128>]) -> Vec<Datum> {
    v.iter().map(|x| x.map_or(Datum::Null, Datum::Int)).collect()
}

#[test]
fn count_star_over_two_phases() {
    let int = ty(T::INT);
    let count = |merge: bool, args: Vec<TExpr>| spec("count", ty(T::BIGINT), vec![], merge, args);
    // three first-phase instances, one of them empty
    let mut partials = Vec::new();
    for values in [ints(&[Some(1), Some(2)]), ints(&[]), ints(&[Some(3), None, Some(5)])] {
        let phase1 = Aggregation {
            group_by: vec![],
            functions: vec![count(false, vec![])],
            finalize: false,
        };
        partials.extend(phase1.run(&input(&int, &values)).unwrap().rows);
    }
    // an empty first phase sends no partial row
    assert_eq!(partials.len(), 2);
    let state = scalar(T::VARCHAR, 65533, 0, 0);
    let merge_in = Chunk {
        block: Block {
            types: vec![ct(&state)],
            rows: partials,
        },
        slots: vec![10],
    };
    let phase2 = Aggregation {
        group_by: vec![],
        functions: vec![count(true, vec![slot(10, state)])],
        finalize: true,
    };
    assert_eq!(phase2.run(&merge_in).unwrap().rows, vec![vec![Datum::Int(5)]]);
}

#[test]
fn aggregates_skip_nulls() {
    let int = ty(T::INT);
    let x = || vec![slot(1, int.clone())];
    let fns = || {
        vec![
            spec("count", ty(T::BIGINT), vec![int.clone()], false, x()),
            spec("count", ty(T::BIGINT), vec![], false, vec![]),
            spec("sum", ty(T::BIGINT), vec![int.clone()], false, x()),
            spec("min", int.clone(), vec![int.clone()], false, x()),
            spec("max", int.clone(), vec![int.clone()], false, x()),
            spec("avg", ty(T::DOUBLE), vec![int.clone()], false, x()),
        ]
    };
    let got = one_phase(fns(), &input(&int, &ints(&[Some(1), None, Some(3)])));
    assert_eq!(
        got,
        vec![vec![
            Datum::Int(2),
            Datum::Int(3),
            Datum::Int(4),
            Datum::Int(1),
            Datum::Int(3),
            Datum::Double(2.0)
        ]]
    );
    let got = one_phase(fns(), &input(&int, &ints(&[None])));
    assert_eq!(
        got,
        vec![vec![Datum::Int(0), Datum::Int(1), Datum::Null, Datum::Null, Datum::Null, Datum::Null]]
    );
}

#[test]
fn empty_input() {
    let int = ty(T::INT);
    let fns = || {
        vec![
            spec("count", ty(T::BIGINT), vec![], false, vec![]),
            spec("sum", ty(T::BIGINT), vec![int.clone()], false, vec![slot(1, int.clone())]),
        ]
    };
    assert_eq!(one_phase(fns(), &input(&int, &[])), vec![vec![Datum::Int(0), Datum::Null]]);
    let grouped = Aggregation {
        group_by: vec![slot(1, int.clone())],
        functions: fns(),
        finalize: true,
    };
    assert!(grouped.run(&input(&int, &[])).unwrap().rows.is_empty());
}

#[test]
fn group_by_keys() {
    let int = ty(T::INT);
    let chunk = Chunk {
        block: Block {
            types: vec![ct(&int), ct(&int)],
            rows: vec![
                ints(&[Some(1), Some(10)]),
                ints(&[None, Some(5)]),
                ints(&[Some(1), Some(7)]),
                ints(&[None, None]),
            ],
        },
        slots: vec![1, 2],
    };
    let sum = spec("sum", ty(T::BIGINT), vec![int.clone()], false, vec![slot(2, int.clone())]);
    let mut rows = Aggregation {
        group_by: vec![slot(1, int.clone())],
        functions: vec![sum],
        finalize: true,
    }
    .run(&chunk)
    .unwrap()
    .rows;
    rows.sort_by_key(|r| format!("{r:?}"));
    // NULL keys form one group
    assert_eq!(rows, vec![ints(&[Some(1), Some(17)]), ints(&[None, Some(5)])]);
}

#[test]
fn avg_of_decimal_truncates() {
    let d = scalar(T::DECIMAL32, -1, 9, 2);
    let ret = scalar(T::DECIMAL128I, -1, 38, 4);
    let got = one_phase(
        vec![spec("avg", ret, vec![d.clone()], false, vec![slot(1, d.clone())])],
        &input(&d, &ints(&[Some(100), Some(200), Some(200)])),
    );
    // 5.00 / 3 = 1.6666, cut, not rounded
    assert_eq!(got, vec![vec![Datum::Int(16666)]]);
}

#[test]
fn count_distinct() {
    let int = ty(T::INT);
    let f = |merge: bool, args: Vec<TExpr>| spec("multi_distinct_count", ty(T::BIGINT), vec![int.clone()], merge, args);
    let got = one_phase(
        vec![f(false, vec![slot(1, int.clone())])],
        &input(&int, &ints(&[Some(1), Some(1), None, Some(2)])),
    );
    assert_eq!(got, vec![vec![Datum::Int(2)]]);
    // two first phases sharing a value
    let mut partials = Vec::new();
    for values in [ints(&[Some(1), Some(2)]), ints(&[Some(2), Some(3)])] {
        let p1 = Aggregation {
            group_by: vec![],
            functions: vec![f(false, vec![slot(1, int.clone())])],
            finalize: false,
        };
        partials.extend(p1.run(&input(&int, &values)).unwrap().rows);
    }
    let state = scalar(T::VARCHAR, 65533, 0, 0);
    let merged = Aggregation {
        group_by: vec![],
        functions: vec![f(true, vec![slot(10, state.clone())])],
        finalize: true,
    }
    .run(&Chunk {
        block: Block {
            types: vec![ct(&state)],
            rows: partials,
        },
        slots: vec![10],
    })
    .unwrap();
    assert_eq!(merged.rows, vec![vec![Datum::Int(3)]]);
}

#[test]
fn sum_overflow_is_an_error() {
    let big = ty(T::BIGINT);
    let s = spec("sum", ty(T::BIGINT), vec![big.clone()], false, vec![slot(1, big.clone())]);
    let err = Aggregation {
        group_by: vec![],
        functions: vec![s],
        finalize: true,
    }
    .run(&input(&big, &ints(&[Some(i64::MAX.into()), Some(1)])))
    .expect_err("BIGINT overflow");
    assert!(err.to_string().contains("overflow"), "{err}");
}

#[test]
fn an_unknown_aggregate_is_refused() {
    let int = ty(T::INT);
    let e = aggregate("percentile", ty(T::DOUBLE), vec![int.clone()], false, vec![slot(1, int)]);
    let err = AggSpec::from_expr(&e).expect_err("percentile is not supported").to_string();
    assert!(err.contains("function percentile"), "{err}");
}

#[test]
fn a_limit_with_a_group_key_order_keeps_the_top_groups() {
    // SELECT count(*), k ... GROUP BY k ORDER BY k DESC LIMIT 2: the FE pushes the limit into the
    // aggregation with agg_sort_info_by_group_key; the groups kept are the top keys, not any two
    use doris_exec::aggregate::top_groups;
    use doris_thrift::gen::plan_nodes::TSortInfo;
    let int = ty(T::INT);
    let rows: Vec<Vec<Datum>> = [1, 3, 0, 4, 2].iter().map(|k| vec![Datum::Int(*k), Datum::Int(5)]).collect();
    let block = Block { types: vec![ct(&int), ct(&ty(T::BIGINT))], rows };
    let info = TSortInfo::new(vec![slot(7, int.clone())], vec![false], vec![false], None, None);
    let kept = top_groups(block, &info, 2);
    assert_eq!(kept.rows, vec![vec![Datum::Int(4), Datum::Int(5)], vec![Datum::Int(3), Datum::Int(5)]]);
}

#[test]
fn a_sum_to_a_lower_scale_is_refused() {
    // Doris sums a decimal at its own scale; a lower result scale would need rounding doris-rs does not do
    let d = scalar(T::DECIMAL32, -1, 9, 2);
    let err = Aggregation {
        group_by: vec![],
        functions: vec![spec("sum", scalar(T::DECIMAL128I, -1, 38, 1), vec![d.clone()], false, vec![slot(1, d.clone())])],
        finalize: true,
    }
    .run(&input(&d, &ints(&[Some(155)])))
    .expect_err("scale 2 to 1")
    .to_string();
    assert!(err.contains("lower scale"), "{err}");
}

fn not_null(mut e: TExpr) -> TExpr {
    e.nodes[0].is_nullable = Some(false);
    e
}

fn doubles(v: &[f64]) -> Vec<Datum> {
    v.iter().map(|x| Datum::Double(*x)).collect()
}

/// Runs `name` in two phases: one first-phase instance per input, then a merge.
fn two_phases(name: &str, ret: TTypeDesc, param: TTypeDesc, inputs: &[Vec<Datum>]) -> Datum {
    let mut partials = Vec::new();
    for values in inputs {
        let phase1 = Aggregation {
            group_by: vec![],
            functions: vec![spec(name, ret.clone(), vec![param.clone()], false, vec![slot(1, param.clone())])],
            finalize: false,
        };
        partials.extend(phase1.run(&input(&param, values)).unwrap().rows);
    }
    let state = scalar(T::VARCHAR, 65533, 0, 0);
    let phase2 = Aggregation {
        group_by: vec![],
        functions: vec![spec(name, ret, vec![param], true, vec![slot(10, state.clone())])],
        finalize: true,
    };
    let merge_in = Chunk { block: Block { types: vec![ct(&state)], rows: partials }, slots: vec![10] };
    phase2.run(&merge_in).unwrap().rows.remove(0).remove(0)
}

#[test]
fn sum0_of_no_rows_is_zero() {
    let big = ty(T::BIGINT);
    let f = AggSpec::from_expr(&not_null(aggregate("sum0", big.clone(), vec![big.clone()], false, vec![slot(1, big.clone())]))).unwrap();
    assert_eq!(one_phase(vec![f.clone()], &input(&big, &[])), vec![vec![Datum::Int(0)]]);
    assert_eq!(one_phase(vec![f.clone()], &input(&big, &ints(&[None]))), vec![vec![Datum::Int(0)]]);
    assert_eq!(one_phase(vec![f], &input(&big, &ints(&[Some(2), None, Some(5)]))), vec![vec![Datum::Int(7)]]);
}

#[test]
fn stddev_is_welford_pop() {
    let d = ty(T::DOUBLE);
    let f = || vec![spec("stddev", d.clone(), vec![d.clone()], false, vec![slot(1, d.clone())])];
    assert_eq!(one_phase(f(), &input(&d, &doubles(&[1.0, 2.0, 3.0, 4.0]))), vec![vec![Datum::Double(1.25f64.sqrt())]]);
    // one row: 0, even for an infinity
    assert_eq!(one_phase(f(), &input(&d, &doubles(&[f64::INFINITY]))), vec![vec![Datum::Double(0.0)]]);
    assert_eq!(one_phase(f(), &input(&d, &[])), vec![vec![Datum::Null]]);
    // an infinite deviation is NaN
    let Datum::Double(v) = one_phase(f(), &input(&d, &doubles(&[f64::INFINITY, 1.0])))[0][0] else { panic!() };
    assert!(v.is_nan());
}

#[test]
fn stddev_merges_through_the_formula_even_into_an_empty_state() {
    let d = ty(T::DOUBLE);
    assert_eq!(two_phases("stddev", d.clone(), d.clone(), &[doubles(&[1.0, 2.0]), doubles(&[3.0, 4.0])]), Datum::Double(1.25f64.sqrt()));
    // one phase over two equal huge values: 0; two phases: the first merge's (delta²)·0 is NaN
    let big = f64::MAX;
    let d1 = one_phase(vec![spec("stddev", d.clone(), vec![d.clone()], false, vec![slot(1, d.clone())])], &input(&d, &doubles(&[big, big])));
    assert_eq!(d1, vec![vec![Datum::Double(0.0)]]);
    let Datum::Double(v) = two_phases("stddev", d.clone(), d.clone(), &[doubles(&[big]), doubles(&[big])]) else { panic!() };
    assert!(v.is_nan(), "{v}");
}

#[test]
fn group_concat_skips_nulls_and_keeps_input_order() {
    let v = ty(T::VARCHAR);
    let b = |s: &str| Datum::Bytes(s.as_bytes().to_vec());
    let f = vec![spec("group_concat", v.clone(), vec![v.clone()], false, vec![slot(1, v.clone())])];
    assert_eq!(one_phase(f, &input(&v, &[b("a"), Datum::Null, b("c"), b("b")])), vec![vec![b("a,c,b")]]);
    let sep = vec![spec("group_concat", v.clone(), vec![v.clone(), v.clone()], false, vec![slot(1, v.clone()), doris_fixtures::build::string("-")])];
    assert_eq!(one_phase(sep, &input(&v, &[b("a"), b("b")])), vec![vec![b("a-b")]]);
}

#[test]
fn group_concat_of_no_rows_is_null() {
    let v = ty(T::VARCHAR);
    let f = || vec![spec("group_concat", v.clone(), vec![v.clone()], false, vec![slot(1, v.clone())])];
    assert_eq!(one_phase(f(), &input(&v, &[Datum::Null, Datum::Null])), vec![vec![Datum::Null]]);
    assert_eq!(one_phase(f(), &input(&v, &[])), vec![vec![Datum::Null]]);
}

#[test]
fn sum0_of_a_double_is_refused() {
    let d = ty(T::DOUBLE);
    let e = not_null(aggregate("sum0", d.clone(), vec![d.clone()], false, vec![slot(1, d)]));
    let spec = AggSpec::from_expr(&e).unwrap();
    let r = Aggregation { group_by: vec![], functions: vec![spec], finalize: true }.run(&input(&ty(T::DOUBLE), &[]));
    assert!(r.is_err());
}

#[test]
fn ndv_counts_distinct_values_skipping_nulls() {
    let int = ty(T::INT);
    let f = AggSpec::from_expr(&not_null(aggregate("ndv", ty(T::BIGINT), vec![int.clone()], false, vec![slot(1, int.clone())]))).unwrap();
    let got = one_phase(vec![f.clone()], &input(&int, &ints(&[Some(1), Some(2), None, Some(1), Some(3)])));
    assert_eq!(got, vec![vec![Datum::Int(3)]]);
    assert_eq!(one_phase(vec![f], &input(&int, &[])), vec![vec![Datum::Int(0)]]);
    let v = ty(T::VARCHAR);
    let b = |s: &str| Datum::Bytes(s.as_bytes().to_vec());
    let parts = [vec![b("a"), b("b")], vec![b("b"), b("c"), Datum::Null]];
    assert_eq!(two_phases("ndv", ty(T::BIGINT), v, &parts), Datum::Int(3));
}

#[test]
fn ndv_of_an_unlisted_type_is_refused() {
    let t = ty(T::DECIMALV2);
    let f = AggSpec::from_expr(&aggregate("ndv", ty(T::BIGINT), vec![t.clone()], false, vec![slot(1, t)]));
    let r = f.and_then(|f| Aggregation { group_by: vec![], functions: vec![f], finalize: true }.run(&input(&ty(T::INT), &[])));
    assert!(r.is_err());
}

/// window_funnel_v2(window, mode, ts, c1..cn) over rows of (ts text, conditions), one phase.
fn funnel(window: i64, mode: &str, rows: &[(&str, &[bool])]) -> Result<Datum, doris_exec::ExecError> {
    let n = rows.first().map_or(2, |r| r.1.len());
    let ts = scalar(T::DATETIMEV2, -1, 0, 0);
    let b = ty(T::BOOLEAN);
    let mut types = vec![ct(&ts)];
    types.extend((0..n).map(|_| ct(&b)));
    let data = rows
        .iter()
        .map(|(t, c)| {
            let mut r = vec![Datum::Int(doris_exec::value::parse_datetimev2(t, 0).unwrap().into())];
            r.extend(c.iter().map(|x| Datum::Bool(*x)));
            r
        })
        .collect();
    let chunk = Chunk { block: Block { types, rows: data }, slots: (1..=n as i32 + 1).collect() };
    let mut params = vec![ty(T::BIGINT), ty(T::VARCHAR), ts.clone()];
    params.extend((0..n).map(|_| b.clone()));
    let mut args = vec![doris_fixtures::build::int(window, ty(T::BIGINT)), doris_fixtures::build::string(mode), slot(1, ts)];
    args.extend((0..n).map(|i| slot(i as i32 + 2, b.clone())));
    let f = spec("window_funnel_v2", ty(T::INT), params, false, args);
    Ok(Aggregation { group_by: vec![], functions: vec![f], finalize: true }.run(&chunk)?.rows.remove(0).remove(0))
}

const T0: &str = "2024-01-01 10:00:00";
const T1: &str = "2024-01-01 10:00:01";
const T2: &str = "2024-01-01 10:00:02";

#[test]
fn window_funnel_v2_default_chains_rows_within_the_window() {
    let rows: &[(&str, &[bool])] = &[(T0, &[true, false, false]), (T1, &[false, true, false]), (T2, &[false, false, true])];
    assert_eq!(funnel(10, "default", rows).unwrap(), Datum::Int(3));
    assert_eq!(funnel(1, "default", rows).unwrap(), Datum::Int(2));
    // two conditions of one row never chain
    assert_eq!(funnel(10, "default", &[(T0, &[true, true])]).unwrap(), Datum::Int(1));
    // no matching row: 0; no row at all: NULL
    assert_eq!(funnel(10, "default", &[(T0, &[false, true])]).unwrap(), Datum::Int(0));
    assert_eq!(funnel(10, "default", &[]).unwrap(), Datum::Null);
}

#[test]
fn window_funnel_v2_increase_needs_a_later_timestamp() {
    let rows: &[(&str, &[bool])] = &[(T0, &[true, false]), (T0, &[false, true])];
    assert_eq!(funnel(10, "default", rows).unwrap(), Datum::Int(2));
    assert_eq!(funnel(10, "increase", rows).unwrap(), Datum::Int(1));
}

#[test]
fn window_funnel_v2_deduplication_breaks_on_a_repeated_level() {
    let rows: &[(&str, &[bool])] = &[(T0, &[true, false, false]), (T1, &[true, false, false]), (T2, &[false, true, false])];
    // from the first c1 the gap holds another c1; from the second the chain reaches c2
    assert_eq!(funnel(10, "deduplication", rows).unwrap(), Datum::Int(2));
    let rows: &[(&str, &[bool])] = &[(T0, &[true, false, false]), (T1, &[false, true, false]), (T1, &[false, true, false]), (T2, &[false, false, true])];
    // the second c2 sits in the gap before c3: the chain stops at c2
    assert_eq!(funnel(10, "deduplication", rows).unwrap(), Datum::Int(2));
    assert_eq!(funnel(10, "default", rows).unwrap(), Datum::Int(3));
}

#[test]
fn window_funnel_v2_fixed_breaks_on_a_level_jump() {
    let rows: &[(&str, &[bool])] = &[(T0, &[true, false, false]), (T1, &[false, false, true]), (T2, &[false, true, false])];
    assert_eq!(funnel(10, "default", rows).unwrap(), Datum::Int(2));
    assert_eq!(funnel(10, "fixed", rows).unwrap(), Datum::Int(1));
}

#[test]
fn window_funnel_v2_errors_like_cpp() {
    let rows: &[(&str, &[bool])] = &[(T0, &[true, false])];
    let e = funnel(-1, "default", rows).unwrap_err().to_string();
    assert_eq!(e, "the sliding time window must be a positive integer, but got: -1");
    let e = funnel(10, "Default", rows).unwrap_err().to_string();
    assert_eq!(e, "Invalid window_funnel mode");
}

#[test]
fn window_funnel_v2_window_past_9999_is_zero() {
    let end = "9999-12-31 23:59:59";
    let rows: &[(&str, &[bool])] = &[("9999-12-31 23:59:58", &[true, false]), (end, &[false, true])];
    assert_eq!(funnel(10, "default", rows).unwrap(), Datum::Int(1));
}
