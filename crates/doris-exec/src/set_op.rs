//! INTERSECT_NODE and EXCEPT_NODE over rows, as the C++ BE's set operators
//! (`set_sink_operator.cpp`, `set_probe_sink_operator.cpp`, `set_source_operator.cpp`) give
//! them:
//! - child 0's rows are hashed, so the result is DISTINCT;
//! - each later child marks the rows it finds;
//! - NULL equals NULL, and numbers are keyed by their bits, so `-0.0` and `0.0` differ
//!   (unlike aggregation, set operators do not normalize floats).
//!
//! C++ emits rows in its hash table's order, and doris-rs does not reproduce that order. It
//! keeps child 0's first-seen order and puts a one-column NULL row first, as C++'s
//! `DataWithNullKey` iterator does. The corpus marks every other multi-row unordered result as
//! unordered.
use std::collections::HashSet;

use doris_segment::writer::Datum;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetKind {
    Intersect,
    Except,
}

/// Child 0's distinct rows that every other child has (INTERSECT) or none has (EXCEPT).
pub fn set_rows(kind: SetKind, children: Vec<Vec<Vec<Datum>>>) -> Vec<Vec<Datum>> {
    let mut children = children.into_iter();
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut seen = HashSet::new();
    for row in children.next().unwrap_or_default() {
        let k = key(&row);
        if seen.insert(k.clone()) {
            keys.push(k);
            rows.push(row);
        }
    }
    let mut keep = vec![true; rows.len()];
    for child in children {
        let found: HashSet<Vec<u8>> = child.iter().map(|r| key(r)).collect();
        for (i, k) in keys.iter().enumerate() {
            let hit = found.contains(k);
            keep[i] &= match kind {
                SetKind::Intersect => hit,
                SetKind::Except => !hit,
            };
        }
    }
    let mut out: Vec<Vec<Datum>> = rows.into_iter().zip(keep).filter_map(|(r, k)| k.then_some(r)).collect();
    if let Some(at) = out.iter().position(|r| matches!(r.as_slice(), [Datum::Null])) {
        let null = out.remove(at);
        out.insert(0, null);
    }
    out
}

/// A row's key: equal rows (NULL = NULL, floats by their bits) have equal keys.
fn key(row: &[Datum]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in row {
        match v {
            Datum::Null => out.push(0),
            Datum::Bool(b) => out.extend([1, u8::from(*b)]),
            Datum::Int(i) => {
                out.push(2);
                out.extend(i.to_le_bytes());
            }
            Datum::Float(f) => {
                out.push(3);
                out.extend(f.to_bits().to_le_bytes());
            }
            Datum::Double(f) => {
                out.push(4);
                out.extend(f.to_bits().to_le_bytes());
            }
            Datum::Bytes(b) => {
                out.push(5);
                out.extend((b.len() as u64).to_le_bytes());
                out.extend(b);
            }
        }
    }
    out
}
