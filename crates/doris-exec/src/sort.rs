//! Sorting rows by keys, each with its own direction and NULL placement (`nulls_first` is
//! independent of the direction, [wire] §5.1), top-n with an offset, and merging sorted inputs.
use std::cmp::Ordering;

use doris_segment::writer::Datum;
use doris_thrift::gen::plan_nodes::TSortInfo;

use crate::block::Block;
use crate::eval::compare;
use crate::value::ColType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortKey {
    pub column: usize,
    pub asc: bool,
    pub nulls_first: bool,
}

/// The keys of a sort info whose ordering expressions are the given block columns.
pub fn keys(info: &TSortInfo, columns: &[usize]) -> Vec<SortKey> {
    columns
        .iter()
        .enumerate()
        .map(|(i, c)| SortKey {
            column: *c,
            asc: info.is_asc_order.get(i).copied().unwrap_or(true),
            nulls_first: info.nulls_first.get(i).copied().unwrap_or(true),
        })
        .collect()
}

fn cmp_value(a: &Datum, b: &Datum, t: &ColType, k: &SortKey) -> Ordering {
    match (a == &Datum::Null, b == &Datum::Null) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if k.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            if k.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => {
            let o = compare(a, t, b, t).unwrap_or(Ordering::Equal);
            if k.asc {
                o
            } else {
                o.reverse()
            }
        }
    }
}

pub fn cmp_rows(a: &[Datum], b: &[Datum], types: &[ColType], keys: &[SortKey]) -> Ordering {
    for k in keys {
        let o = cmp_value(&a[k.column], &b[k.column], &types[k.column], k);
        if o != Ordering::Equal {
            return o;
        }
    }
    Ordering::Equal
}

/// Sorts the rows in place; equal rows keep their order.
pub fn sort_block(block: &mut Block, keys: &[SortKey]) {
    let types = block.types.clone();
    block.rows.sort_by(|a, b| cmp_rows(a, b, &types, keys));
}

/// The sorted rows from `offset`, at most `limit` of them.
pub fn top_n(mut block: Block, keys: &[SortKey], offset: usize, limit: Option<usize>) -> Block {
    sort_block(&mut block, keys);
    let end = limit.map_or(block.rows.len(), |l| offset.saturating_add(l).min(block.rows.len()));
    let start = offset.min(end);
    block.rows = block.rows.drain(start..end).collect();
    block
}

/// Sorted inputs merged into one sorted block (of the first input's types).
pub fn merge_sorted(inputs: Vec<Block>, keys: &[SortKey]) -> Block {
    let mut it = inputs.into_iter();
    let Some(mut out) = it.next() else { return Block::default() };
    for b in it {
        out.rows.extend(b.rows);
    }
    // a stable sort of the concatenation is the merge of sorted runs
    sort_block(&mut out, keys);
    out
}
