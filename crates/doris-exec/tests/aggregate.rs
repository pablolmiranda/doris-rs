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
    let e = aggregate("ndv", ty(T::BIGINT), vec![int.clone()], false, vec![slot(1, int)]);
    let err = AggSpec::from_expr(&e).expect_err("ndv is not supported").to_string();
    assert!(err.contains("function ndv"), "{err}");
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
