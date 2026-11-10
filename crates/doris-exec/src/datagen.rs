//! DATA_GEN_SCAN_NODE: `numbers("number"="N"[, "const_value"="C"])` (`vnumbers_tvf.cpp`).
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::value::ColType;
use crate::ExecError;

/// One `TTVFNumbersScanRange`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Numbers {
    pub total: i64,
    pub use_const: bool,
    pub const_value: i64,
}

/// The most rows doris-rs generates: it holds a node's output as one chunk, where C++ streams
/// `batch_size` blocks.
pub const MAX_NUMBERS: i64 = 10_000_000;

/// The scan's tuple: exactly one non-null BIGINT slot (`vnumbers_tvf.h`: `_slot_num = 1`, a
/// `ColumnInt64`).
pub fn check_numbers_tuple(types: &[ColType]) -> Result<(), ExecError> {
    match types {
        [t] if t.ptype == T::BIGINT && !t.nullable => Ok(()),
        _ => Err(ExecError::BadRequest("a numbers scan whose tuple is not one non-null BIGINT slot".into())),
    }
}

/// The `number` column: 0..total ascending, or `const_value` `total` times.
pub fn numbers(n: &Numbers) -> Result<Vec<Datum>, ExecError> {
    if n.total > MAX_NUMBERS {
        return Err(ExecError::Unsupported { what: format!("numbers() past {MAX_NUMBERS} rows") });
    }
    let total = n.total.max(0);
    Ok(if n.use_const { vec![Datum::Int(n.const_value.into()); total as usize] } else { (0..total).map(|v| Datum::Int(v.into())).collect() })
}
