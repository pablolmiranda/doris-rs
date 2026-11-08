//! PARTITION_SORT_NODE: the top N rows of each partition, as the C++ BE's partition sorter
//! (`partition_sort_sink_operator.cpp`, `partition_sorter.cpp`) keeps them:
//! - rows are grouped as a hash table groups them: NULL equals NULL, floats by their bits;
//! - each partition is sorted, and keeps its first N rows (ROW_NUMBER), the rows of rank at most
//!   N (RANK), or of dense rank at most N (DENSE_RANK); ties are kept with their peers;
//! - partitions come out in first-seen order, each one sorted.
//!
//! Outside its global phase, C++ stops filtering past 1024 small partitions and sends the rest
//! through, a superset. The window above re-sorts and re-filters, so an exact filter answers
//! the same.
use std::cmp::Ordering;
use std::collections::HashMap;

use doris_segment::writer::Datum;
use doris_thrift::gen::plan_nodes::TopNAlgorithm;

use crate::set_op::row_key;

/// The rows kept, partitions in first-seen order and each sorted (stably) by `cmp`, which
/// compares two rows by their index in `rows`. `part` holds each row's partition values.
pub fn partition_top_n(
    rows: Vec<Vec<Datum>>,
    part: &[Vec<Datum>],
    cmp: impl Fn(usize, usize) -> Ordering,
    algo: TopNAlgorithm,
    n: usize,
) -> Vec<Vec<Datum>> {
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
    for (r, key) in part.iter().enumerate() {
        let g = *index.entry(row_key(key)).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[g].push(r);
    }
    let mut keep = Vec::new();
    for mut g in groups {
        g.sort_by(|a, b| cmp(*a, *b));
        // each row's rank and dense rank among its partition's sorted rows
        let (mut rank, mut dense) = (0, 0);
        for (at, r) in g.iter().enumerate() {
            if at == 0 || cmp(g[at - 1], *r) != Ordering::Equal {
                rank = at + 1;
                dense += 1;
            }
            let kept = match algo {
                TopNAlgorithm::RANK => rank <= n,
                TopNAlgorithm::DENSE_RANK => dense <= n,
                _ => at < n,
            };
            if !kept {
                break;
            }
            keep.push(*r);
        }
    }
    let mut rows: Vec<Option<Vec<Datum>>> = rows.into_iter().map(Some).collect();
    keep.into_iter().filter_map(|r| rows[r].take()).collect()
}
