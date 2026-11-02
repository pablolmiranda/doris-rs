//! Column decoding: every fixed-width scalar type over BIT_SHUFFLE (or RLE for BOOL and
//! integers) pages, with the per-page null bitmap applied.
//!
//! A data page body is `encoded values | nullmap (nullmap_size bytes)`. Values are stored
//! only for non-null rows (`ScalarColumnWriter::append_nulls` touches only the bitmap), so the
//! null bitmap decides which rows consume a value.
use crate::bitshuffle;
use crate::error::Error;
use crate::footer::SegmentFile;
use crate::ordinal::data_pages;
use crate::page::read_page;
use crate::pb::segment_v2::{ColumnMetaPb, EncodingTypePb};
use crate::rle::RleDecoder;
use crate::values::{
    unpack_date_v1, unpack_datetime_v1, unpack_datetimev2, unpack_datev2, ColumnValues, Decimal12,
};

/// Storage field type ids from `be/src/storage/olap_common.h` (`FieldType`).
pub mod field_type {
    pub const TINYINT: i32 = 1;
    pub const SMALLINT: i32 = 3;
    pub const INT: i32 = 5;
    pub const BIGINT: i32 = 7;
    pub const LARGEINT: i32 = 9;
    pub const FLOAT: i32 = 10;
    pub const DOUBLE: i32 = 11;
    pub const DATE: i32 = 14;
    pub const DATETIME: i32 = 15;
    pub const DECIMAL: i32 = 16;
    pub const BOOL: i32 = 24;
    pub const DATEV2: i32 = 28;
    pub const DATETIMEV2: i32 = 29;
    pub const DECIMAL32: i32 = 31;
    pub const DECIMAL64: i32 = 32;
    pub const DECIMAL128I: i32 = 33;
    pub const IPV4: i32 = 38;
    pub const IPV6: i32 = 39;
}

/// Stored element size of a fixed-width type (`sizeof(CppType)` in `storage/types.h`).
pub fn element_size(field_type: i32) -> Option<usize> {
    use field_type::*;
    Some(match field_type {
        BOOL | TINYINT => 1,
        SMALLINT => 2,
        DATE => 3,
        INT | FLOAT | DATEV2 | DECIMAL32 | IPV4 => 4,
        BIGINT | DOUBLE | DATETIME | DATETIMEV2 | DECIMAL64 => 8,
        DECIMAL => 12,
        LARGEINT | DECIMAL128I | IPV6 => 16,
        _ => return None,
    })
}

const BITSHUFFLE_HEADER: usize = 16;
const RLE_HEADER: usize = 4;

/// Find a column's metadata by its unique id.
pub fn column_meta(file: &SegmentFile, unique_id: u32) -> Result<&ColumnMetaPb, Error> {
    file.footer
        .columns
        .iter()
        .find(|c| c.unique_id == Some(unique_id))
        .ok_or(Error::ColumnNotFound(unique_id))
}

/// Decode a BIT_SHUFFLE page body (without its null bitmap) into little-endian element bytes.
///
/// The 16-byte header (`num_elements`, `compressed_size`, `num_element_after_padding`,
/// `size_of_element`, all u32 LE) is validated before anything is allocated or decompressed:
/// the element size must equal the column type's (`BitShufflePageDecoder::init`), so a
/// crc-valid but corrupt header yields `Error::Corrupt` rather than a panic.
pub fn decode_bitshuffle_values(
    values: &[u8],
    non_null: usize,
    elem_size: usize,
) -> Result<Vec<u8>, Error> {
    if values.len() < BITSHUFFLE_HEADER {
        return Err(Error::Corrupt(
            "bitshuffle page shorter than its header".into(),
        ));
    }
    let u32_at =
        |i: usize| u32::from_le_bytes(values[i..i + 4].try_into().expect("4 bytes")) as usize;
    let num_elements = u32_at(0);
    let compressed_size = u32_at(4);
    let padded = u32_at(8);
    let header_elem_size = u32_at(12);
    if header_elem_size != elem_size {
        return Err(Error::Corrupt(format!(
            "bitshuffle page element size {header_elem_size}, column type needs {elem_size}"
        )));
    }
    if num_elements != non_null {
        return Err(Error::Corrupt(format!(
            "bitshuffle page holds {num_elements} values but the null bitmap implies {non_null}"
        )));
    }
    if padded != num_elements.div_ceil(8) * 8 || compressed_size != values.len() {
        return Err(Error::Corrupt(format!(
            "bitshuffle page header is inconsistent: num={num_elements} padded={padded} compressed_size={compressed_size} body={}",
            values.len()
        )));
    }
    let mut elems = bitshuffle::decompress_lz4(
        &values[BITSHUFFLE_HEADER..compressed_size],
        padded,
        elem_size,
    )?;
    elems.truncate(num_elements * elem_size);
    Ok(elems)
}

/// Raw little-endian element bytes of the non-null values of one page, plus a null flag per row.
struct PageValues {
    nulls: Vec<bool>,
    elems: Vec<u8>,
}

/// Split a data page body into its null flags (one per row) and the encoded values.
fn split_nulls(
    body: &[u8],
    num_values: usize,
    nullmap_size: usize,
) -> Result<(Vec<bool>, &[u8]), Error> {
    if nullmap_size > body.len() {
        return Err(Error::Corrupt("nullmap larger than page body".into()));
    }
    let (values, nullmap) = body.split_at(body.len() - nullmap_size);
    let nulls = if nullmap_size == 0 {
        vec![false; num_values]
    } else {
        RleDecoder::new(nullmap, 1)
            .take(num_values)?
            .into_iter()
            .map(|b| b != 0)
            .collect()
    };
    Ok((nulls, values))
}

fn decode_page_values(
    meta: &ColumnMetaPb,
    body: &[u8],
    num_values: usize,
    nullmap_size: usize,
    elem_size: usize,
) -> Result<PageValues, Error> {
    let unique_id = meta.unique_id.unwrap_or(0);
    let (nulls, values) = split_nulls(body, num_values, nullmap_size)?;
    let non_null = nulls.iter().filter(|n| !**n).count();
    let elems = match meta.encoding() {
        EncodingTypePb::BitShuffle => decode_bitshuffle_values(values, non_null, elem_size)?,
        EncodingTypePb::Rle => {
            // RlePageDecoder: BOOL uses one bit per value, integers their full width.
            let bit_width = match meta.r#type {
                Some(field_type::BOOL) => 1u32,
                Some(
                    field_type::TINYINT
                    | field_type::SMALLINT
                    | field_type::INT
                    | field_type::BIGINT,
                ) => (elem_size * 8) as u32,
                _ => {
                    return Err(Error::UnsupportedEncoding {
                        column: unique_id,
                        encoding: EncodingTypePb::Rle as i32,
                    })
                }
            };
            if values.len() < RLE_HEADER {
                return Err(Error::Corrupt("rle page shorter than its header".into()));
            }
            let num_elements =
                u32::from_le_bytes(values[..4].try_into().expect("4 bytes")) as usize;
            if num_elements != non_null {
                return Err(Error::Corrupt(format!(
                    "rle page holds {num_elements} values but the null bitmap implies {non_null}"
                )));
            }
            let decoded = RleDecoder::new(&values[RLE_HEADER..], bit_width).take(num_elements)?;
            let mut elems = Vec::with_capacity(num_elements * elem_size);
            for v in decoded {
                elems.extend_from_slice(&v.to_le_bytes()[..elem_size]);
            }
            elems
        }
        other => {
            return Err(Error::UnsupportedEncoding {
                column: unique_id,
                encoding: other as i32,
            })
        }
    };
    Ok(PageValues { nulls, elems })
}

/// Map each row of every page to `None` (null) or the next element parsed by `parse`.
fn gather<T>(pages: &[PageValues], elem_size: usize, parse: impl Fn(&[u8]) -> T) -> Vec<Option<T>> {
    let mut out = Vec::new();
    for p in pages {
        let mut next = 0usize;
        for &is_null in &p.nulls {
            if is_null {
                out.push(None);
            } else {
                out.push(Some(parse(&p.elems[next..next + elem_size])));
                next += elem_size;
            }
        }
    }
    out
}

/// Little-endian bytes of one element, zero-extended to N bytes.
fn le<const N: usize>(b: &[u8]) -> [u8; N] {
    let mut a = [0u8; N];
    a[..b.len()].copy_from_slice(b);
    a
}

fn non_negative(value: Option<i32>, what: &str, unique_id: u32) -> Result<u32, Error> {
    u32::try_from(value.unwrap_or(0))
        .map_err(|_| Error::Corrupt(format!("column {unique_id}: negative {what}")))
}

/// Decode every row of a column, in ordinal order.
pub fn read_column(file: &SegmentFile, unique_id: u32) -> Result<ColumnValues, Error> {
    use field_type::*;
    let meta = column_meta(file, unique_id)?;
    let ft = meta.r#type.unwrap_or(-1);
    let elem_size = element_size(ft).ok_or(Error::UnsupportedType {
        column: unique_id,
        field_type: ft,
    })?;
    let mut pages = Vec::new();
    for ptr in data_pages(file, meta)? {
        let page = read_page(file, &ptr, meta.compression())?;
        let dpf = page
            .footer
            .data_page_footer
            .as_ref()
            .ok_or_else(|| Error::Corrupt("data page without data_page_footer".into()))?;
        let num_values = dpf.num_values.unwrap_or(0) as usize;
        let nullmap_size = dpf.nullmap_size.unwrap_or(0) as usize;
        pages.push(decode_page_values(
            meta,
            &page.body,
            num_values,
            nullmap_size,
            elem_size,
        )?);
    }
    let precision = || non_negative(meta.precision, "precision", unique_id);
    let scale = || non_negative(meta.frac, "scale", unique_id);
    let g = |parse: &dyn Fn(&[u8]) -> i128| gather(&pages, elem_size, parse);
    Ok(match ft {
        BOOL => ColumnValues::Bool(gather(&pages, elem_size, |b| b[0] != 0)),
        TINYINT => ColumnValues::I8(gather(&pages, elem_size, |b| b[0] as i8)),
        SMALLINT => ColumnValues::I16(gather(&pages, elem_size, |b| i16::from_le_bytes(le(b)))),
        INT => ColumnValues::I32(gather(&pages, elem_size, |b| i32::from_le_bytes(le(b)))),
        BIGINT => ColumnValues::I64(gather(&pages, elem_size, |b| i64::from_le_bytes(le(b)))),
        LARGEINT => ColumnValues::I128(gather(&pages, elem_size, |b| i128::from_le_bytes(le(b)))),
        FLOAT => ColumnValues::F32(gather(&pages, elem_size, |b| f32::from_le_bytes(le(b)))),
        DOUBLE => ColumnValues::F64(gather(&pages, elem_size, |b| f64::from_le_bytes(le(b)))),
        DECIMAL32 => ColumnValues::Decimal {
            unscaled: g(&|b| i128::from(i32::from_le_bytes(le(b)))),
            precision: precision()?,
            scale: scale()?,
        },
        DECIMAL64 => ColumnValues::Decimal {
            unscaled: g(&|b| i128::from(i64::from_le_bytes(le(b)))),
            precision: precision()?,
            scale: scale()?,
        },
        DECIMAL128I => ColumnValues::Decimal {
            unscaled: g(&|b| i128::from_le_bytes(le(b))),
            precision: precision()?,
            scale: scale()?,
        },
        DECIMAL => ColumnValues::DecimalV2 {
            values: gather(&pages, elem_size, |b| Decimal12 {
                integer: i64::from_le_bytes(le(&b[..8])),
                fraction: i32::from_le_bytes(le(&b[8..12])),
            }),
            precision: precision()?,
            scale: scale()?,
        },
        DATEV2 => ColumnValues::Date(gather(&pages, elem_size, |b| {
            unpack_datev2(u32::from_le_bytes(le(b)))
        })),
        DATE => ColumnValues::Date(gather(&pages, elem_size, |b| {
            unpack_date_v1(u32::from_le_bytes(le(b)))
        })),
        DATETIMEV2 => ColumnValues::DateTime {
            values: gather(&pages, elem_size, |b| {
                unpack_datetimev2(u64::from_le_bytes(le(b)))
            }),
            scale: scale()?,
        },
        DATETIME => ColumnValues::DateTime {
            values: gather(&pages, elem_size, |b| {
                unpack_datetime_v1(i64::from_le_bytes(le(b)))
            }),
            scale: 0,
        },
        IPV4 => ColumnValues::Ipv4(gather(&pages, elem_size, |b| u32::from_le_bytes(le(b)))),
        IPV6 => ColumnValues::Ipv6(gather(&pages, elem_size, |b| u128::from_le_bytes(le(b)))),
        _ => unreachable!("element_size covers exactly these types"),
    })
}
