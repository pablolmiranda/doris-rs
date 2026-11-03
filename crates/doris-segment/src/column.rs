//! Integer column decoding: BOOL/TINYINT/SMALLINT/INT/BIGINT/LARGEINT over BIT_SHUFFLE or
//! RLE pages, with the per-page null bitmap applied.
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

/// Storage field type ids from `be/src/storage/olap_common.h` (`FieldType`).
pub mod field_type {
    pub const TINYINT: i32 = 1;
    pub const SMALLINT: i32 = 3;
    pub const INT: i32 = 5;
    pub const BIGINT: i32 = 7;
    pub const LARGEINT: i32 = 9;
    pub const BOOL: i32 = 24;
}

const BITSHUFFLE_HEADER: usize = 16;
const RLE_HEADER: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntValues {
    Bool(Vec<Option<bool>>),
    I8(Vec<Option<i8>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    I64(Vec<Option<i64>>),
    I128(Vec<Option<i128>>),
}

impl IntValues {
    pub fn len(&self) -> usize {
        match self {
            IntValues::Bool(v) => v.len(),
            IntValues::I8(v) => v.len(),
            IntValues::I16(v) => v.len(),
            IntValues::I32(v) => v.len(),
            IntValues::I64(v) => v.len(),
            IntValues::I128(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

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
/// `size_of_element`, all u32 LE) is validated the way `parse_bit_shuffle_header` does in the
/// C++ reader before anything is allocated or decompressed, so a crc-valid but corrupt header
/// yields `Error::Corrupt` rather than a panic.
pub fn decode_bitshuffle_values(values: &[u8], non_null: usize) -> Result<(Vec<u8>, usize), Error> {
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
    let elem_size = u32_at(12);
    if !matches!(elem_size, 1 | 2 | 4 | 8 | 16) {
        return Err(Error::Corrupt(format!(
            "bitshuffle page element size {elem_size} is not 1, 2, 4, 8 or 16"
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
    Ok((elems, elem_size))
}

/// Raw little-endian element bytes of the non-null values of one page, plus a null flag per row.
struct PageValues {
    nulls: Vec<bool>,
    elems: Vec<u8>,
    elem_size: usize,
}

fn decode_page_values(
    meta: &ColumnMetaPb,
    body: &[u8],
    num_values: usize,
    nullmap_size: usize,
) -> Result<PageValues, Error> {
    let unique_id = meta.unique_id.unwrap_or(0);
    if nullmap_size > body.len() {
        return Err(Error::Corrupt("nullmap larger than page body".into()));
    }
    let (values, nullmap) = body.split_at(body.len() - nullmap_size);
    let nulls: Vec<bool> = if nullmap_size == 0 {
        vec![false; num_values]
    } else {
        RleDecoder::new(nullmap, 1)
            .take(num_values)?
            .into_iter()
            .map(|b| b != 0)
            .collect()
    };
    let non_null = nulls.iter().filter(|n| !**n).count();
    match meta.encoding() {
        EncodingTypePb::BitShuffle => {
            let (elems, elem_size) = decode_bitshuffle_values(values, non_null)?;
            Ok(PageValues {
                nulls,
                elems,
                elem_size,
            })
        }
        EncodingTypePb::Rle => {
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
            let (bit_width, elem_size) = match meta.r#type {
                Some(field_type::BOOL) => (1u32, 1usize),
                Some(field_type::TINYINT) => (8, 1),
                Some(field_type::SMALLINT) => (16, 2),
                Some(field_type::INT) => (32, 4),
                Some(field_type::BIGINT) => (64, 8),
                other => {
                    return Err(Error::UnsupportedType {
                        column: unique_id,
                        field_type: other.unwrap_or(-1),
                    })
                }
            };
            let decoded = RleDecoder::new(&values[RLE_HEADER..], bit_width).take(num_elements)?;
            let mut elems = Vec::with_capacity(num_elements * elem_size);
            for v in decoded {
                elems.extend_from_slice(&v.to_le_bytes()[..elem_size]);
            }
            Ok(PageValues {
                nulls,
                elems,
                elem_size,
            })
        }
        other => Err(Error::UnsupportedEncoding {
            column: unique_id,
            encoding: other as i32,
        }),
    }
}

fn gather<T: Copy>(pages: &[PageValues], parse: impl Fn(&[u8]) -> T) -> Vec<Option<T>> {
    let mut out = Vec::new();
    for p in pages {
        let mut next = 0usize;
        for &is_null in &p.nulls {
            if is_null {
                out.push(None);
            } else {
                out.push(Some(parse(&p.elems[next..next + p.elem_size])));
                next += p.elem_size;
            }
        }
    }
    out
}

/// Decode every row of an integer-typed column, in ordinal order.
pub fn read_int_column(file: &SegmentFile, unique_id: u32) -> Result<IntValues, Error> {
    let meta = column_meta(file, unique_id)?;
    let field_type = meta.r#type.unwrap_or(-1);
    let expected_size = match field_type {
        field_type::BOOL | field_type::TINYINT => 1,
        field_type::SMALLINT => 2,
        field_type::INT => 4,
        field_type::BIGINT => 8,
        field_type::LARGEINT => 16,
        _ => {
            return Err(Error::UnsupportedType {
                column: unique_id,
                field_type,
            })
        }
    };
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
        let pv = decode_page_values(meta, &page.body, num_values, nullmap_size)?;
        if pv.elem_size != expected_size {
            return Err(Error::Corrupt(format!(
                "column {unique_id}: page element size {} but type {field_type} needs {expected_size}",
                pv.elem_size
            )));
        }
        pages.push(pv);
    }
    let le = |b: &[u8]| -> [u8; 16] {
        let mut a = [0u8; 16];
        a[..b.len()].copy_from_slice(b);
        a
    };
    Ok(match field_type {
        field_type::BOOL => IntValues::Bool(gather(&pages, |b| b[0] != 0)),
        field_type::TINYINT => IntValues::I8(gather(&pages, |b| b[0] as i8)),
        field_type::SMALLINT => IntValues::I16(gather(&pages, |b| {
            i16::from_le_bytes(b.try_into().expect("2"))
        })),
        field_type::INT => IntValues::I32(gather(&pages, |b| {
            i32::from_le_bytes(b.try_into().expect("4"))
        })),
        field_type::BIGINT => IntValues::I64(gather(&pages, |b| {
            i64::from_le_bytes(b.try_into().expect("8"))
        })),
        field_type::LARGEINT => IntValues::I128(gather(&pages, |b| i128::from_le_bytes(le(b)))),
        _ => unreachable!("checked above"),
    })
}
