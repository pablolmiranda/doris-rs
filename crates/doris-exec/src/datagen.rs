//! DATA_GEN_SCAN_NODE: `numbers("number"="N"[, "const_value"="C"])` (`vnumbers_tvf.cpp`).
use doris_segment::writer::Datum;

/// One `TTVFNumbersScanRange`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Numbers {
    pub total: i64,
    pub use_const: bool,
    pub const_value: i64,
}

/// The `number` column: 0..total ascending, or `const_value` `total` times.
pub fn numbers(n: &Numbers) -> Vec<Datum> {
    let total = n.total.max(0);
    if n.use_const {
        vec![Datum::Int(n.const_value.into()); total as usize]
    } else {
        (0..total).map(|v| Datum::Int(v.into())).collect()
    }
}
