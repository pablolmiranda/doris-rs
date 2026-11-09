//! PARTITION_SORT_NODE's top-N per partition, as the C++ BE's partition sorter keeps it:
//! partitions keyed as a hash table keys them (NULL = NULL, floats by their bits), first-seen
//! order, each partition sorted, ties at the limit kept by RANK and DENSE_RANK.
use doris_exec::partition_sort::partition_top_n;
use doris_segment::writer::Datum;
use doris_thrift::gen::plan_nodes::TopNAlgorithm as A;

fn i(v: i128) -> Datum {
    Datum::Int(v)
}

/// Rows of (partition, order) values, sorted by the order value.
fn top(rows: &[(Datum, i128)], algo: A, n: usize) -> Vec<(Datum, i128)> {
    let data: Vec<Vec<Datum>> = rows.iter().map(|(p, o)| vec![p.clone(), i(*o)]).collect();
    let part: Vec<Vec<Datum>> = rows.iter().map(|(p, _)| vec![p.clone()]).collect();
    let order: Vec<i128> = rows.iter().map(|(_, o)| *o).collect();
    partition_top_n(data, &part, |a, b| order[a].cmp(&order[b]), algo, n)
        .into_iter()
        .map(|r| match (&r[0], &r[1]) {
            (p, Datum::Int(o)) => (p.clone(), *o),
            other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn row_number_keeps_the_first_n_per_partition() {
    let rows = [(i(1), 3), (i(1), 1), (i(1), 2), (i(2), 9), (i(2), 8)];
    assert_eq!(top(&rows, A::ROW_NUMBER, 2), [(i(1), 1), (i(1), 2), (i(2), 8), (i(2), 9)]);
}

#[test]
fn rank_keeps_ties_at_the_limit() {
    let rows = [(i(1), 2), (i(1), 1), (i(1), 1), (i(1), 3)];
    assert_eq!(top(&rows, A::RANK, 1), [(i(1), 1), (i(1), 1)]);
    // rank 1, 1, 3: a limit of 2 still keeps only the two 1s
    assert_eq!(top(&rows, A::RANK, 2), [(i(1), 1), (i(1), 1)]);
}

#[test]
fn dense_rank_keeps_n_distinct_values() {
    let rows = [(i(1), 2), (i(1), 1), (i(1), 1), (i(1), 3)];
    assert_eq!(top(&rows, A::DENSE_RANK, 2), [(i(1), 1), (i(1), 1), (i(1), 2)]);
}

#[test]
fn partitions_come_in_first_seen_order_sorted() {
    let rows = [(i(5), 2), (i(3), 7), (i(5), 1), (i(3), 6)];
    assert_eq!(top(&rows, A::ROW_NUMBER, 5), [(i(5), 1), (i(5), 2), (i(3), 6), (i(3), 7)]);
}

#[test]
fn zero_signs_are_different_partitions_and_nulls_one() {
    let rows = [(Datum::Double(-0.0), 1), (Datum::Double(0.0), 2), (Datum::Null, 3), (Datum::Null, 4)];
    let got = top(&rows, A::ROW_NUMBER, 1);
    assert_eq!(got.len(), 3, "{got:?}");
    assert_eq!(got[2], (Datum::Null, 3));
}
