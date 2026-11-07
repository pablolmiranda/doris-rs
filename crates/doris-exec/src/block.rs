//! Row blocks: what query operators pass each other.
use doris_segment::writer::Datum;

use crate::value::ColType;

/// Rows of typed columns. A value's meaning comes from its column's type:
/// - decimals are unscaled `Int` at the column's scale;
/// - DATEV2 is a packed u32 and DATETIMEV2 a packed u64, both as `Int`;
/// - strings are `Bytes`, CHAR without its padding;
/// - FLOAT is `Float` and DOUBLE is `Double`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Block {
    pub types: Vec<ColType>,
    pub rows: Vec<Vec<Datum>>,
}

impl Block {
    pub fn new(types: Vec<ColType>) -> Block {
        Block {
            types,
            rows: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}
