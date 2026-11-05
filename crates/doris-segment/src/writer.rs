//! Writes V2 segment files the 4.1.5 C++ BE reads. Every column is PLAIN (`PlainPageBuilder`
//! for fixed-width types, `BinaryPlainPageBuilder` V1 for strings) with an RLE null map on
//! pages that hold a NULL, uncompressed. Each column gets an ordinal index (the data page itself
//! when there is one, else one leaf index page) and a zone map index whose page and segment zone
//! maps are `pass_all` (always correct: the reader then scans instead of pruning). Then the short
//! key page and the footer. Layout, as the C++ writer orders it: data pages column by column,
//! ordinal index pages, zone map pages, the short key page, `footer | len | crc32c | "D0R1"`.
use prost::Message;

use crate::column::field_type::*;
use crate::column::element_size;
use crate::error::Error;
use crate::key_coder::encode_key;
use crate::pb::doris::ColumnPb;
use crate::pb::segment_v2::{
    index_page_footer_pb, BTreeMetaPb, ColumnIndexMetaPb, ColumnIndexTypePb, ColumnMetaPb,
    CompressionTypePb, DataPageFooterPb, EncodingTypePb, IndexPageFooterPb, IndexedColumnMetaPb, OrdinalIndexPb,
    PageFooterPb, PagePointerPb, PageTypePb, SegmentFooterPb, ShortKeyFooterPb, ZoneMapIndexPb, ZoneMapPb,
};
use crate::values::{pack_datetimev2, pack_datev2, ColumnValues};

pub use crate::sort::sort_rows;

/// A value as the writer takes it. Integers of every width, DECIMALV3 (unscaled), DATEV2
/// (packed u32) and DATETIMEV2 (packed u64) are `Int`; CHAR, VARCHAR and STRING are `Bytes`,
/// CHAR before its padding.
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Null,
    Bool(bool),
    Int(i128),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
}

/// One column of the tablet schema, as the segment needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub unique_id: u32,
    /// The storage `FieldType` (`crate::column::field_type`).
    pub field_type: i32,
    pub length: u32,
    pub precision: u32,
    pub frac: u32,
    pub nullable: bool,
    pub is_key: bool,
    pub index_length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentSchema {
    pub columns: Vec<ColumnSpec>,
    pub num_short_key_columns: u32,
    pub num_rows_per_block: u32,
}

#[derive(Debug, Clone)]
pub struct WrittenSegment {
    pub bytes: Vec<u8>,
    pub num_rows: u32,
    /// Full-encoded keys of the first and last row (`segments_key_bounds`).
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

/// The storage field type of a `TabletColumnPB.type` name, for the types M4 writes.
pub fn field_type_of(name: &str) -> Option<i32> {
    Some(match name {
        "BOOLEAN" => BOOL,
        "TINYINT" => TINYINT,
        "SMALLINT" => SMALLINT,
        "INT" => INT,
        "BIGINT" => BIGINT,
        "LARGEINT" => LARGEINT,
        "FLOAT" => FLOAT,
        "DOUBLE" => DOUBLE,
        "DECIMAL32" => DECIMAL32,
        "DECIMAL64" => DECIMAL64,
        "DECIMAL128I" => DECIMAL128I,
        "DATEV2" => DATEV2,
        "DATETIMEV2" => DATETIMEV2,
        "CHAR" => CHAR,
        "VARCHAR" => VARCHAR,
        "STRING" => STRING,
        _ => return None,
    })
}

impl ColumnSpec {
    pub fn from_pb(c: &ColumnPb) -> Result<ColumnSpec, Error> {
        let unique_id = c.unique_id as u32;
        let field_type = field_type_of(&c.r#type).ok_or(Error::UnsupportedType { column: unique_id, field_type: -1 })?;
        let n = |v: Option<i32>| v.unwrap_or(0).max(0) as u32;
        Ok(ColumnSpec {
            unique_id,
            field_type,
            length: n(c.length),
            precision: n(c.precision),
            frac: n(c.frac),
            nullable: c.is_nullable.unwrap_or(false),
            is_key: c.is_key.unwrap_or(false),
            index_length: n(c.index_length),
        })
    }
}

/// Decoded values as writer input (the reader's types back to `Datum`).
pub fn datums(values: &ColumnValues) -> Vec<Datum> {
    fn map<T>(v: &[Option<T>], f: impl Fn(&T) -> Datum) -> Vec<Datum> {
        v.iter().map(|x| x.as_ref().map_or(Datum::Null, &f)).collect()
    }
    match values {
        ColumnValues::Bool(v) => map(v, |b| Datum::Bool(*b)),
        ColumnValues::I8(v) => map(v, |x| Datum::Int((*x).into())),
        ColumnValues::I16(v) => map(v, |x| Datum::Int((*x).into())),
        ColumnValues::I32(v) => map(v, |x| Datum::Int((*x).into())),
        ColumnValues::I64(v) => map(v, |x| Datum::Int((*x).into())),
        ColumnValues::I128(v) => map(v, |x| Datum::Int(*x)),
        ColumnValues::F32(v) => map(v, |x| Datum::Float(*x)),
        ColumnValues::F64(v) => map(v, |x| Datum::Double(*x)),
        ColumnValues::Decimal { unscaled, .. } => map(unscaled, |x| Datum::Int(*x)),
        ColumnValues::DecimalV2 { values, .. } => {
            map(values, |d| Datum::Int(i128::from(d.integer) * 1_000_000_000 + i128::from(d.fraction)))
        }
        ColumnValues::Date(v) => map(v, |d| Datum::Int(pack_datev2(d).into())),
        ColumnValues::DateTime { values, .. } => map(values, |d| Datum::Int(pack_datetimev2(d).into())),
        ColumnValues::Ipv4(v) => map(v, |x| Datum::Int((*x).into())),
        ColumnValues::Ipv6(v) => map(v, |x| Datum::Int(*x as i128)),
        ColumnValues::Bytes(v) => map(v, |b| Datum::Bytes(b.clone())),
    }
}

/// CHAR(n) on disk: the value zero-padded to n bytes (`OlapColumnDataConvertorChar`).
pub fn char_padded(b: &[u8], n: usize) -> Vec<u8> {
    let mut v = b.to_vec();
    if v.len() < n {
        v.resize(n, 0);
    }
    v
}

const DATA_PAGE_SIZE: usize = 65536;
/// `FieldType::OLAP_FIELD_TYPE_BITMAP`, the type of the page zone map IndexedColumn.
const ZONE_MAP_COLUMN_TYPE: i32 = 25;

/// `body | PageFooterPB | footer_size | crc32c`, appended to `out`; returns its pointer.
fn write_page(out: &mut Vec<u8>, body: &[u8], footer: &PageFooterPb) -> PagePointerPb {
    let start = out.len();
    out.extend_from_slice(body);
    let f = footer.encode_to_vec();
    out.extend_from_slice(&f);
    out.extend_from_slice(&(f.len() as u32).to_le_bytes());
    let crc = crc32c::crc32c(&out[start..]);
    out.extend_from_slice(&crc.to_le_bytes());
    PagePointerPb { offset: start as u64, size: (out.len() - start) as u32 }
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// `RleEncoder<bool>` with bit width 1, written as repeated runs only: `varint(len << 1)` and
/// the value in one byte (1 = NULL).
fn rle_null_map(nulls: &[bool]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < nulls.len() {
        let mut j = i;
        while j < nulls.len() && nulls[j] == nulls[i] {
            j += 1;
        }
        put_varint(&mut out, ((j - i) as u64) << 1);
        out.push(u8::from(nulls[i]));
        i = j;
    }
    out
}

/// A value's stored bytes (little-endian; CHAR padded).
fn value_bytes(spec: &ColumnSpec, d: &Datum) -> Result<Vec<u8>, Error> {
    let bad = || Error::Corrupt(format!("column {}: value {d:?} does not fit type {}", spec.unique_id, spec.field_type));
    Ok(match (spec.field_type, d) {
        (CHAR, Datum::Bytes(b)) => {
            if b.len() > spec.length as usize {
                return Err(bad());
            }
            char_padded(b, spec.length as usize)
        }
        (VARCHAR | STRING, Datum::Bytes(b)) => b.clone(),
        (BOOL, Datum::Bool(v)) => vec![u8::from(*v)],
        (FLOAT, Datum::Float(v)) => v.to_le_bytes().to_vec(),
        (DOUBLE, Datum::Double(v)) => v.to_le_bytes().to_vec(),
        (ft, Datum::Int(v)) if element_size(ft).is_some() && !matches!(ft, FLOAT | DOUBLE | BOOL) => {
            v.to_le_bytes()[..element_size(ft).unwrap()].to_vec()
        }
        _ => return Err(bad()),
    })
}

struct ColumnPages {
    pages: Vec<PagePointerPb>,
    /// (first ordinal, has_null, has_not_null) per page
    stats: Vec<(u64, bool, bool)>,
}

fn write_column(out: &mut Vec<u8>, spec: &ColumnSpec, values: &[&Datum]) -> Result<ColumnPages, Error> {
    let binary = matches!(spec.field_type, CHAR | VARCHAR | STRING);
    let mut pages = ColumnPages { pages: Vec::new(), stats: Vec::new() };
    let mut start = 0usize;
    while start < values.len() {
        let mut nulls = Vec::new();
        let mut encoded: Vec<Vec<u8>> = Vec::new();
        let mut size = 4usize;
        let mut end = start;
        while end < values.len() && size < DATA_PAGE_SIZE {
            let d = values[end];
            nulls.push(matches!(d, Datum::Null));
            if !matches!(d, Datum::Null) {
                let b = value_bytes(spec, d)?;
                size += b.len() + if binary { 4 } else { 0 };
                encoded.push(b);
            } else if !spec.nullable {
                return Err(Error::Corrupt(format!("column {}: NULL in a NOT NULL column", spec.unique_id)));
            }
            end += 1;
        }
        let mut body = Vec::with_capacity(size);
        if binary {
            // BinaryPlainPageBuilder (V1): values | u32 offset each | u32 count
            let mut offsets = Vec::with_capacity(encoded.len());
            for b in &encoded {
                offsets.push(body.len() as u32);
                body.extend_from_slice(b);
            }
            for o in offsets {
                body.extend_from_slice(&o.to_le_bytes());
            }
            body.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
        } else {
            // PlainPageBuilder: u32 count | values
            body.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            for b in &encoded {
                body.extend_from_slice(b);
            }
        }
        let has_null = nulls.iter().any(|n| *n);
        let nullmap = if spec.nullable && has_null { rle_null_map(&nulls) } else { Vec::new() };
        body.extend_from_slice(&nullmap);
        let footer = PageFooterPb {
            r#type: Some(PageTypePb::DataPage as i32),
            uncompressed_size: Some(body.len() as u32),
            data_page_footer: Some(DataPageFooterPb {
                first_ordinal: Some(start as u64),
                num_values: Some((end - start) as u64),
                nullmap_size: Some(nullmap.len() as u32),
                ..Default::default()
            }),
            ..Default::default()
        };
        pages.pages.push(write_page(out, &body, &footer));
        pages.stats.push((start as u64, has_null, !encoded.is_empty()));
        start = end;
    }
    Ok(pages)
}

/// `IndexPageBuilder` leaf page: per entry a length-prefixed key, then the page pointer.
fn write_index_page(out: &mut Vec<u8>, entries: &[(Vec<u8>, PagePointerPb)]) -> PagePointerPb {
    let mut body = Vec::new();
    for (key, ptr) in entries {
        put_varint(&mut body, key.len() as u64);
        body.extend_from_slice(key);
        put_varint(&mut body, ptr.offset);
        put_varint(&mut body, u64::from(ptr.size));
    }
    let footer = PageFooterPb {
        r#type: Some(PageTypePb::IndexPage as i32),
        uncompressed_size: Some(body.len() as u32),
        index_page_footer: Some(IndexPageFooterPb {
            num_entries: Some(entries.len() as u32),
            r#type: Some(index_page_footer_pb::Type::Leaf as i32),
        }),
        ..Default::default()
    };
    write_page(out, &body, &footer)
}

fn btree(out: &mut Vec<u8>, pages: &[PagePointerPb], first_ordinals: &[u64]) -> BTreeMetaPb {
    if pages.len() == 1 {
        return BTreeMetaPb { root_page: Some(pages[0]), is_root_data_page: Some(true) };
    }
    let entries: Vec<_> = pages.iter().zip(first_ordinals).map(|(p, o)| (o.to_be_bytes().to_vec(), *p)).collect();
    BTreeMetaPb { root_page: Some(write_index_page(out, &entries)), is_root_data_page: Some(false) }
}

fn zone_map(has_null: bool, has_not_null: bool) -> ZoneMapPb {
    ZoneMapPb {
        min: Some(Vec::new()),
        max: Some(Vec::new()),
        has_null: Some(has_null),
        has_not_null: Some(has_not_null),
        pass_all: Some(true),
        has_positive_inf: Some(false),
        has_negative_inf: Some(false),
        has_nan: Some(false),
    }
}

/// The page zone maps as an IndexedColumn (`ZoneMapIndexWriter::finish`): BinaryPlainPage V1
/// pages of serialized ZoneMapPB, with an ordinal index.
fn write_zone_maps(out: &mut Vec<u8>, stats: &[(u64, bool, bool)]) -> ZoneMapIndexPb {
    let maps: Vec<Vec<u8>> = stats.iter().map(|(_, n, nn)| zone_map(*n, *nn).encode_to_vec()).collect();
    let mut pages = Vec::new();
    let mut firsts = Vec::new();
    let mut start = 0;
    while start < maps.len() {
        let mut body = Vec::new();
        let mut offsets = Vec::new();
        let mut end = start;
        while end < maps.len() && body.len() + 4 * (offsets.len() + 2) < DATA_PAGE_SIZE {
            offsets.push(body.len() as u32);
            body.extend_from_slice(&maps[end]);
            end += 1;
        }
        for o in &offsets {
            body.extend_from_slice(&o.to_le_bytes());
        }
        body.extend_from_slice(&(offsets.len() as u32).to_le_bytes());
        let footer = PageFooterPb {
            r#type: Some(PageTypePb::DataPage as i32),
            uncompressed_size: Some(body.len() as u32),
            data_page_footer: Some(DataPageFooterPb {
                first_ordinal: Some(start as u64),
                num_values: Some((end - start) as u64),
                nullmap_size: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };
        pages.push(write_page(out, &body, &footer));
        firsts.push(start as u64);
        start = end;
    }
    let size: u64 = pages.iter().map(|p| u64::from(p.size)).sum();
    let ordinal = btree(out, &pages, &firsts);
    ZoneMapIndexPb {
        segment_zone_map: Some(zone_map(stats.iter().any(|s| s.1), stats.iter().any(|s| s.2))),
        page_zone_maps: Some(IndexedColumnMetaPb {
            data_type: Some(ZONE_MAP_COLUMN_TYPE),
            encoding: Some(EncodingTypePb::PlainEncoding as i32),
            num_values: Some(maps.len() as i64),
            ordinal_index_meta: Some(ordinal),
            value_index_meta: None,
            compression: Some(CompressionTypePb::NoCompression as i32),
            size: Some(size),
        }),
    }
}

/// Write one segment of `rows`, which must already be in key order (`sort_rows`).
pub fn write_segment(schema: &SegmentSchema, rows: &[Vec<Datum>]) -> Result<WrittenSegment, Error> {
    if rows.is_empty() {
        return Err(Error::Corrupt("a segment needs at least one row".into()));
    }
    let width = schema.columns.len();
    if let Some(r) = rows.iter().find(|r| r.len() != width) {
        return Err(Error::Corrupt(format!("row of {} values for {width} columns", r.len())));
    }
    let mut out = Vec::new();
    let mut written = Vec::with_capacity(width);
    for (c, spec) in schema.columns.iter().enumerate() {
        let values: Vec<&Datum> = rows.iter().map(|r| &r[c]).collect();
        written.push(write_column(&mut out, spec, &values)?);
    }
    let mut ordinals = Vec::with_capacity(width);
    for w in &written {
        let firsts: Vec<u64> = w.stats.iter().map(|s| s.0).collect();
        ordinals.push(btree(&mut out, &w.pages, &firsts));
    }
    let mut zones = Vec::with_capacity(width);
    for w in &written {
        zones.push(write_zone_maps(&mut out, &w.stats));
    }

    // ShortKeyIndexBuilder: a key every num_rows_per_block rows
    let block = schema.num_rows_per_block.max(1) as usize;
    let mut keys = Vec::new();
    let mut offsets = Vec::new();
    let mut num_items = 0u32;
    for r in rows.iter().step_by(block) {
        put_varint(&mut offsets, keys.len() as u64);
        keys.extend_from_slice(&encode_key(schema, r, true));
        num_items += 1;
    }
    let (key_bytes, offset_bytes) = (keys.len() as u32, offsets.len() as u32);
    keys.extend_from_slice(&offsets);
    let sk_footer = PageFooterPb {
        r#type: Some(PageTypePb::ShortKeyPage as i32),
        uncompressed_size: Some(keys.len() as u32),
        short_key_page_footer: Some(ShortKeyFooterPb {
            num_items: Some(num_items),
            key_bytes: Some(key_bytes),
            offset_bytes: Some(offset_bytes),
            segment_id: Some(0),
            num_rows_per_block: Some(schema.num_rows_per_block),
            num_segment_rows: Some(rows.len() as u32),
        }),
        ..Default::default()
    };
    let short_key_page = write_page(&mut out, &keys, &sk_footer);

    let columns = schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, spec)| ColumnMetaPb {
            column_id: Some(i as u32),
            unique_id: Some(spec.unique_id),
            r#type: Some(spec.field_type),
            length: Some(spec.length as i32),
            encoding: Some(EncodingTypePb::PlainEncoding as i32),
            compression: Some(CompressionTypePb::NoCompression as i32),
            is_nullable: Some(spec.nullable),
            indexes: vec![
                ColumnIndexMetaPb {
                    r#type: Some(ColumnIndexTypePb::OrdinalIndex as i32),
                    ordinal_index: Some(OrdinalIndexPb { root_page: Some(ordinals[i]) }),
                    ..Default::default()
                },
                ColumnIndexMetaPb {
                    r#type: Some(ColumnIndexTypePb::ZoneMapIndex as i32),
                    zone_map_index: Some(zones[i].clone()),
                    ..Default::default()
                },
            ],
            num_rows: Some(rows.len() as u64),
            default_value: Some(Vec::new()),
            precision: Some(spec.precision as i32),
            frac: Some(spec.frac as i32),
            result_is_nullable: Some(false),
            function_name: Some("NONE".into()),
            be_exec_version: Some(-1),
            ..Default::default()
        })
        .collect();
    let footer = SegmentFooterPb {
        num_rows: Some(rows.len() as u32),
        columns,
        short_key_index_page: Some(short_key_page),
        ..Default::default()
    };
    let f = footer.encode_to_vec();
    out.extend_from_slice(&f);
    out.extend_from_slice(&(f.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(&f).to_le_bytes());
    out.extend_from_slice(b"D0R1");
    Ok(WrittenSegment {
        bytes: out,
        num_rows: rows.len() as u32,
        min_key: encode_key(schema, &rows[0], false),
        max_key: encode_key(schema, &rows[rows.len() - 1], false),
    })
}
