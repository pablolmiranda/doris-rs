//! Integer column decoding against the golden corpus expected rows.
mod common;

use std::path::Path;

use common::{assert_table_matches, expected_rows};
use doris_segment::corpus::Manifest;
use doris_segment::page::decode_page;
use doris_segment::pb::segment_v2::{CompressionTypePb, PageFooterPb, PageTypePb};
use doris_segment::{Error, SegmentFile};
use prost::Message;

/// Columns of the two integer tables, (unique id, name) in schema order.
const INT_COLUMNS: [(u32, &str); 7] = [
    (0, "k"),
    (1, "c_bool"),
    (2, "c_tinyint"),
    (3, "c_smallint"),
    (4, "c_int"),
    (5, "c_bigint"),
    (6, "c_largeint"),
];

#[test]
fn scalar_ints_columns_match_expected_tsv() {
    assert_table_matches("scalar_ints", &INT_COLUMNS);
}

#[test]
fn zstd_ints_decode_identically() {
    assert_table_matches("zstd_ints", &INT_COLUMNS);
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
    for f in &t.files {
        let seg = SegmentFile::open(&m.resolve(&f.path)).unwrap();
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
        }
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
    assert_table_matches(table, &BIG_INT_COLUMNS);
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
    let err = doris_segment::column::decode_bitshuffle_values(&values, 1, 4).unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    // and an absurd element size must not try to allocate
    let mut values = Vec::new();
    for v in [1u32, 16, 8, 0xFFFF_FFFF] {
        values.extend_from_slice(&v.to_le_bytes());
    }
    let err = doris_segment::column::decode_bitshuffle_values(&values, 1, 4).unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
}

/// A one-column INT segment with a single data page whose footer claims `num_values` rows,
/// built from scratch so every checksum is valid.
fn segment_with_claimed_rows(num_values: u64, segment_rows: u32) -> SegmentFile {
    use doris_segment::pb::segment_v2::{
        BTreeMetaPb, ColumnIndexMetaPb, ColumnIndexTypePb, ColumnMetaPb, EncodingTypePb,
        OrdinalIndexPb, PagePointerPb, SegmentFooterPb,
    };
    let page = common::data_page(vec![0u8; 16], num_values, 0);
    let column = ColumnMetaPb {
        column_id: Some(0),
        unique_id: Some(0),
        r#type: Some(5),
        length: Some(4),
        encoding: Some(EncodingTypePb::BitShuffle as i32),
        compression: Some(CompressionTypePb::NoCompression as i32),
        is_nullable: Some(false),
        indexes: vec![ColumnIndexMetaPb {
            r#type: Some(ColumnIndexTypePb::OrdinalIndex as i32),
            ordinal_index: Some(OrdinalIndexPb {
                root_page: Some(BTreeMetaPb {
                    root_page: Some(PagePointerPb {
                        offset: 0,
                        size: page.len() as u32,
                    }),
                    is_root_data_page: Some(true),
                }),
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let footer = SegmentFooterPb {
        version: Some(1),
        columns: vec![column],
        num_rows: Some(segment_rows),
        ..Default::default()
    };
    common::segment(page, footer)
}

#[test]
fn page_claiming_more_rows_than_the_segment_is_an_error() {
    // a crc-valid page footer claiming 2^62 rows must not drive a 2^62-entry allocation
    let seg = segment_with_claimed_rows(1 << 62, 3);
    let err = doris_segment::column::read_column(&seg, 0).unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
}
