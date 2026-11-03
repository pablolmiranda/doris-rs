//! Integer column decoding against the golden corpus expected rows.
use std::collections::BTreeMap;
use std::path::Path;

use doris_segment::column::{read_int_column, IntValues};
use doris_segment::corpus::Manifest;
use doris_segment::page::decode_page;
use doris_segment::pb::segment_v2::{CompressionTypePb, PageFooterPb, PageTypePb};
use doris_segment::{Error, SegmentFile};
use prost::Message;

/// Column names of the two integer tables, in schema order.
const INT_COLUMNS: [&str; 7] = [
    "k",
    "c_bool",
    "c_tinyint",
    "c_smallint",
    "c_int",
    "c_bigint",
    "c_largeint",
];

fn expected_rows(table: &str) -> Vec<Vec<String>> {
    let path = Manifest::corpus_dir()
        .join("expected")
        .join(format!("{table}.tsv"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    text.lines()
        .map(|l| l.split('\t').map(str::to_string).collect())
        .collect()
}

fn to_strings(values: &IntValues) -> Vec<String> {
    fn fmt<T: ToString>(v: &[Option<T>]) -> Vec<String> {
        v.iter()
            .map(|x| x.as_ref().map_or("NULL".to_string(), ToString::to_string))
            .collect()
    }
    match values {
        IntValues::Bool(v) => v
            .iter()
            .map(|x| {
                x.map_or(
                    "NULL".to_string(),
                    |b| if b { "1".into() } else { "0".into() },
                )
            })
            .collect(),
        IntValues::I8(v) => fmt(v),
        IntValues::I16(v) => fmt(v),
        IntValues::I32(v) => fmt(v),
        IntValues::I64(v) => fmt(v),
        IntValues::I128(v) => fmt(v),
    }
}

/// Decode every integer column of every segment of `table`, concatenate rows in manifest file
/// order, and return them sorted by `k` so they line up with the expected TSV.
fn decoded_rows(m: &Manifest, table: &str) -> Vec<Vec<String>> {
    let t = &m.tables[table];
    let mut rows: Vec<Vec<String>> = Vec::new();
    for f in &t.files {
        let seg = SegmentFile::open(&m.resolve(&f.path)).unwrap();
        let by_name: BTreeMap<&str, Vec<String>> = INT_COLUMNS
            .iter()
            .enumerate()
            .map(|(unique_id, name)| {
                let col = read_int_column(&seg, unique_id as u32)
                    .unwrap_or_else(|e| panic!("{table}/{}: column {name}: {e}", f.path.display()));
                (*name, to_strings(&col))
            })
            .collect();
        let n = seg.footer.num_rows.unwrap() as usize;
        for (name, col) in &by_name {
            assert_eq!(col.len(), n, "{table}: column {name} length");
        }
        rows.extend((0..n).map(|i| {
            INT_COLUMNS
                .iter()
                .map(|name| by_name[name][i].clone())
                .collect::<Vec<String>>()
        }));
    }
    rows.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    rows
}

#[test]
fn scalar_ints_columns_match_expected_tsv() {
    let m = Manifest::load();
    let expected = expected_rows("scalar_ints");
    let got = decoded_rows(&m, "scalar_ints");
    assert_eq!(got.len(), expected.len());
    for (g, e) in got.iter().zip(&expected) {
        assert_eq!(g, e);
    }
}

#[test]
fn zstd_ints_decode_identically() {
    let m = Manifest::load();
    let expected = expected_rows("zstd_ints");
    let got = decoded_rows(&m, "zstd_ints");
    assert_eq!(got, expected);
}

#[test]
fn empty_table_has_no_segments_and_zero_expected_rows() {
    let m = Manifest::load();
    assert!(m.tables["empty_table"].files.is_empty());
    assert!(expected_rows("empty_table").is_empty());
}

#[test]
fn page_whose_decompressed_size_differs_from_declared_is_an_error() {
    // Build a page by hand: LZ4 frame of 64 bytes, footer claiming 100 bytes, valid crc.
    let body: Vec<u8> = (0..64u8).collect();
    let compressed = lz4_flex::frame::FrameEncoder::new(Vec::new());
    let mut enc = compressed;
    std::io::Write::write_all(&mut enc, &body).unwrap();
    let compressed = enc.finish().unwrap();
    let footer = PageFooterPb {
        r#type: Some(PageTypePb::DataPage as i32),
        uncompressed_size: Some(100),
        ..Default::default()
    };
    let footer_bytes = footer.encode_to_vec();
    let mut page = compressed.clone();
    page.extend_from_slice(&footer_bytes);
    page.extend_from_slice(&(footer_bytes.len() as u32).to_le_bytes());
    let crc = crc32c::crc32c(&page);
    page.extend_from_slice(&crc.to_le_bytes());
    let err = decode_page(&page, CompressionTypePb::Lz4f).unwrap_err();
    assert!(
        matches!(
            err,
            Error::PageSizeMismatch {
                expected: 100,
                actual: 64
            }
        ),
        "{err}"
    );
}

#[test]
fn page_with_bad_checksum_is_an_error() {
    let m = Manifest::load();
    let seg =
        SegmentFile::open(&m.resolve(Path::new(&m.tables["scalar_ints"].files[0].path))).unwrap();
    // first data page of the first column starts at offset 0 in a V2 segment
    let col = &seg.footer.columns[0];
    let ptr = doris_segment::ordinal::data_pages(&seg, col).unwrap()[0];
    let mut raw =
        seg.bytes[ptr.offset as usize..(ptr.offset + u64::from(ptr.size)) as usize].to_vec();
    raw[0] ^= 0xFF;
    let err = decode_page(&raw, CompressionTypePb::Lz4f).unwrap_err();
    assert!(matches!(err, Error::PageChecksumMismatch { .. }), "{err}");
}

/// Integer columns of the two large tables (unique ids in schema order; c_varchar is id 5 and
/// is not decoded here).
const BIG_INT_COLUMNS: [(u32, &str); 5] = [
    (0, "k"),
    (1, "c_bool"),
    (2, "c_mod"),
    (3, "c_const"),
    (4, "c_bigint"),
];

fn big_table_matches_expected(table: &str, codec: CompressionTypePb) {
    let m = Manifest::load();
    let t = &m.tables[table];
    // The reference chooses the codec per segment: a flush block at or under
    // segment_compression_threshold_kb (256 KB) is written with NO_COMPRESSION, so the small
    // trailing segments of a big INSERT legitimately differ from the table's codec.
    let mut saw_codec = false;
    let mut saw_compressed_page = false;
    let mut saw_index_page = false;
    let mut got: Vec<Vec<String>> = Vec::new();
    for f in &t.files {
        let seg = SegmentFile::open(&m.resolve(&f.path)).unwrap();
        let n = seg.footer.num_rows.unwrap() as usize;
        let mut cols: Vec<Vec<String>> = Vec::new();
        for (id, name) in BIG_INT_COLUMNS {
            let meta = doris_segment::column::column_meta(&seg, id).unwrap();
            let col_codec = meta.compression();
            saw_codec |= col_codec == codec;
            let pages = doris_segment::ordinal::data_pages(&seg, meta).unwrap();
            saw_index_page |= pages.len() > 1;
            for ptr in &pages {
                let page = doris_segment::page::read_page(&seg, ptr, col_codec)
                    .unwrap_or_else(|e| panic!("{table}: {name}: {e}"));
                saw_compressed_page |= page.compressed && col_codec == codec;
            }
            let col = read_int_column(&seg, id).unwrap_or_else(|e| panic!("{table}: {name}: {e}"));
            assert_eq!(col.len(), n, "{table}: column {name} length");
            cols.push(to_strings(&col));
        }
        got.extend((0..n).map(|i| cols.iter().map(|c| c[i].clone()).collect::<Vec<String>>()));
    }
    assert!(saw_codec, "{table}: no segment uses {codec:?}");
    assert!(
        saw_compressed_page,
        "{table}: no integer page body was stored compressed, so the {codec:?} path is unverified"
    );
    assert!(
        saw_index_page,
        "{table}: every column fit in one page, so the ordinal index page path is unverified"
    );
    let expected = expected_rows(table);
    assert_eq!(got.len(), expected.len(), "{table}: row count");
    got.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    for (g, e) in got.iter().zip(&expected) {
        assert_eq!(g[..], e[..BIG_INT_COLUMNS.len()]);
    }
}

#[test]
fn lz4f_big_decodes_through_compressed_pages_and_index_pages() {
    big_table_matches_expected("lz4f_big", CompressionTypePb::Lz4f);
}

#[test]
fn zstd_big_decodes_through_compressed_pages_and_index_pages() {
    big_table_matches_expected("zstd_big", CompressionTypePb::Zstd);
}

#[test]
fn bitshuffle_header_with_zero_element_size_is_an_error_not_a_panic() {
    // header: num_elements=1, compressed_size=16, padded=8, elem_size=0, no data
    let mut values = Vec::new();
    for v in [1u32, 16, 8, 0] {
        values.extend_from_slice(&v.to_le_bytes());
    }
    let err = doris_segment::column::decode_bitshuffle_values(&values, 1).unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    // and an absurd element size must not try to allocate
    let mut values = Vec::new();
    for v in [1u32, 16, 8, 0xFFFF_FFFF] {
        values.extend_from_slice(&v.to_le_bytes());
    }
    let err = doris_segment::column::decode_bitshuffle_values(&values, 1).unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
}
