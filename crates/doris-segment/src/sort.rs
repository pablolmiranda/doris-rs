//! Row order within a segment (`MemTable::_sort`): by the key columns, NULLs first, numbers by
//! value, strings bytewise; rows with equal keys in descending input position (a later row
//! first), as the memtable orders a duplicate-key table's ties.
use std::cmp::Ordering;

use crate::writer::{Datum, SegmentSchema};

pub fn compare_datum(a: &Datum, b: &Datum) -> Ordering {
    match (a, b) {
        (Datum::Null, Datum::Null) => Ordering::Equal,
        (Datum::Null, _) => Ordering::Less,
        (_, Datum::Null) => Ordering::Greater,
        (Datum::Int(x), Datum::Int(y)) => x.cmp(y),
        (Datum::Bool(x), Datum::Bool(y)) => x.cmp(y),
        (Datum::Bytes(x), Datum::Bytes(y)) => x.cmp(y),
        (Datum::Float(x), Datum::Float(y)) => x.total_cmp(y),
        (Datum::Double(x), Datum::Double(y)) => x.total_cmp(y),
        _ => Ordering::Equal,
    }
}

pub fn sort_rows(schema: &SegmentSchema, rows: &mut Vec<Vec<Datum>>) {
    let keys: Vec<usize> = schema.columns.iter().enumerate().filter(|(_, c)| c.is_key).map(|(i, _)| i).collect();
    let mut indexed: Vec<(usize, Vec<Datum>)> = std::mem::take(rows).into_iter().enumerate().collect();
    indexed.sort_by(|(ia, a), (ib, b)| {
        keys.iter().map(|&k| compare_datum(&a[k], &b[k])).find(|o| o.is_ne()).unwrap_or(Ordering::Equal).then(ib.cmp(ia))
    });
    *rows = indexed.into_iter().map(|(_, r)| r).collect();
}
