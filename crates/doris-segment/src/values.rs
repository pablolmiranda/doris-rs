//! Typed values decoded from a column, and the bit layouts of the date and time types.
//!
//! Layouts are those of the 4.1.5 BE (`core/value/vdatetime_value.h`, `core/decimal12.h`).
//! Unpacking never fails: a corrupt value yields an out-of-range field, not a panic.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Date {
    pub year: u32,
    pub month: u8,
    pub day: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DateTime {
    pub date: Date,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub microsecond: u32,
}

/// DECIMALV2 as stored (`decimal12_t`, packed): the value is `integer * 10^9 + fraction`, and
/// both parts carry the sign.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal12 {
    pub integer: i64,
    pub fraction: i32,
}

/// DATEV2: `day:5 | month:4 | year:23`, from the low bit.
pub fn unpack_datev2(v: u32) -> Date {
    Date {
        year: v >> 9,
        month: ((v >> 5) & 0xF) as u8,
        day: (v & 0x1F) as u8,
    }
}

/// DATETIMEV2: `microsecond:20 | second:6 | minute:6 | hour:5 | day:5 | month:4 | year:18`.
pub fn unpack_datetimev2(v: u64) -> DateTime {
    DateTime {
        date: Date {
            year: (v >> 46) as u32,
            month: ((v >> 42) & 0xF) as u8,
            day: ((v >> 37) & 0x1F) as u8,
        },
        hour: ((v >> 32) & 0x1F) as u8,
        minute: ((v >> 26) & 0x3F) as u8,
        second: ((v >> 20) & 0x3F) as u8,
        microsecond: (v & 0xF_FFFF) as u32,
    }
}

/// Legacy DATE (v1): a 3-byte little-endian `year << 9 | month << 5 | day`
/// (`VecDateTimeValue::from_olap_date`). Pass the 3 bytes zero-extended to a u32.
pub fn unpack_date_v1(v: u32) -> Date {
    Date {
        year: (v >> 9) & 0x7FFF,
        month: ((v >> 5) & 0xF) as u8,
        day: (v & 0x1F) as u8,
    }
}

/// Legacy DATETIME (v1): the decimal integer `YYYYMMDDhhmmss`
/// (`VecDateTimeValue::from_olap_datetime`).
pub fn unpack_datetime_v1(v: i64) -> DateTime {
    let v = v.unsigned_abs();
    let (date, time) = (v / 1_000_000, v % 1_000_000);
    DateTime {
        date: Date {
            year: (date / 10_000) as u32,
            month: (date / 100 % 100) as u8,
            day: (date % 100) as u8,
        },
        hour: (time / 10_000) as u8,
        minute: (time / 100 % 100) as u8,
        second: (time % 100) as u8,
        microsecond: 0,
    }
}

/// All rows of one column, typed by storage family. `None` is SQL NULL.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValues {
    Bool(Vec<Option<bool>>),
    I8(Vec<Option<i8>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    I64(Vec<Option<i64>>),
    I128(Vec<Option<i128>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    /// DECIMAL32, DECIMAL64, DECIMAL128I: the unscaled integer and the column's scale.
    Decimal {
        unscaled: Vec<Option<i128>>,
        precision: u32,
        scale: u32,
    },
    /// Legacy DECIMALV2, kept in its stored form.
    DecimalV2 {
        values: Vec<Option<Decimal12>>,
        precision: u32,
        scale: u32,
    },
    /// DATEV2 and legacy DATE.
    Date(Vec<Option<Date>>),
    /// DATETIMEV2 (with its scale) and legacy DATETIME (scale 0).
    DateTime {
        values: Vec<Option<DateTime>>,
        scale: u32,
    },
    /// Host-order u32: `127.0.0.1` is `0x7f000001`.
    Ipv4(Vec<Option<u32>>),
    /// The address is the big-endian bytes of the u128.
    Ipv6(Vec<Option<u128>>),
    /// CHAR (already cut at its first NUL, as the reference reads it), VARCHAR, STRING.
    Bytes(Vec<Option<Vec<u8>>>),
}

impl ColumnValues {
    pub fn len(&self) -> usize {
        match self {
            ColumnValues::Bool(v) => v.len(),
            ColumnValues::I8(v) => v.len(),
            ColumnValues::I16(v) => v.len(),
            ColumnValues::I32(v) => v.len(),
            ColumnValues::I64(v) => v.len(),
            ColumnValues::I128(v) => v.len(),
            ColumnValues::F32(v) => v.len(),
            ColumnValues::F64(v) => v.len(),
            ColumnValues::Decimal { unscaled, .. } => unscaled.len(),
            ColumnValues::DecimalV2 { values, .. } => values.len(),
            ColumnValues::Date(v) => v.len(),
            ColumnValues::DateTime { values, .. } => values.len(),
            ColumnValues::Ipv4(v) => v.len(),
            ColumnValues::Ipv6(v) => v.len(),
            ColumnValues::Bytes(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
