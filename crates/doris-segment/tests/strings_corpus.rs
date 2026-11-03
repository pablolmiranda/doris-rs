//! Binary columns (CHAR, VARCHAR, STRING): dictionary pages, codeword pages, and the plain
//! fallback pages written once a dictionary fills.
mod common;

use std::collections::BTreeSet;

use common::{assert_table_matches, decode_rows, Cell};
use doris_segment::binary::{
    decode_dict_data_page, decode_plain_v1, read_dictionary, DictPageMode,
};
use doris_segment::column::column_meta;
use doris_segment::corpus::Manifest;
use doris_segment::ordinal::data_pages;
use doris_segment::page::read_page;
use doris_segment::{Error, SegmentFile};

/// Data page modes (first u32 of each data page body) seen across every segment of a column.
fn page_modes(table: &str, unique_id: u32) -> BTreeSet<u32> {
    let m = Manifest::load();
    let mut modes = BTreeSet::new();
    for f in &m.tables[table].files {
        let seg = SegmentFile::open(&m.resolve(&f.path)).unwrap();
        let meta = column_meta(&seg, unique_id).unwrap();
        for ptr in data_pages(&seg, meta).unwrap() {
            let page = read_page(&seg, &ptr, meta.compression()).unwrap();
            modes.insert(u32::from_le_bytes(page.body[0..4].try_into().unwrap()));
        }
    }
    modes
}

#[test]
fn scalar_others_string_columns_match_expected() {
    assert_table_matches(
        "scalar_others",
        &[(0, "k"), (6, "c_char"), (7, "c_varchar"), (8, "c_string")],
    );
}

#[test]
fn strings_fallback_columns_match_expected() {
    assert_table_matches(
        "strings_fallback",
        &[(0, "k"), (1, "c_char"), (2, "c_varchar"), (3, "c_string")],
    );
    // both on-disk shapes must have been decoded in every column: codewords (5) and plain (2)
    for (uid, name) in [(1, "c_char"), (2, "c_varchar"), (3, "c_string")] {
        let modes = page_modes("strings_fallback", uid);
        assert!(
            modes.contains(&5) && modes.contains(&2),
            "{name}: page modes {modes:?}"
        );
    }
}

#[test]
fn big_tables_varchar_matches_under_compression() {
    for table in ["lz4f_big", "zstd_big"] {
        assert_table_matches(table, &[(0, "k"), (5, "c_varchar")]);
        let m = Manifest::load();
        let compressed = m.tables[table].files.iter().any(|f| {
            let seg = SegmentFile::open(&m.resolve(&f.path)).unwrap();
            let meta = column_meta(&seg, 5).unwrap();
            data_pages(&seg, meta)
                .unwrap()
                .iter()
                .any(|ptr| read_page(&seg, ptr, meta.compression()).unwrap().compressed)
        });
        assert!(
            compressed,
            "{table}: no c_varchar page was stored compressed"
        );
    }
    // pins the corpus README: zstd_big's dictionary pages are ZSTD-compressed (lz4f_big's are
    // stored uncompressed, so an LZ4F-compressed dictionary is not exercised)
    let m = Manifest::load();
    let compressed_dict = m.tables["zstd_big"].files.iter().any(|f| {
        let seg = SegmentFile::open(&m.resolve(&f.path)).unwrap();
        let meta = column_meta(&seg, 5).unwrap();
        let ptr = meta.dict_page.expect("dictionary page");
        read_page(&seg, &ptr, meta.compression())
            .unwrap()
            .compressed
    });
    assert!(
        compressed_dict,
        "zstd_big: no dictionary page was stored compressed"
    );
}

#[test]
fn char_truncates_at_first_nul_varchar_keeps_it() {
    // row k=3 holds unhex('610062') -- 'a', NUL, 'b' -- in all three string columns
    let m = Manifest::load();
    let cols = [(0, "k"), (1, "c_char"), (2, "c_varchar"), (3, "c_string")];
    let rows = decode_rows(&m, "strings_fallback", &cols);
    let row = rows
        .iter()
        .find(|r| matches!(&r[0], Cell::Text(k) if k == "3"))
        .expect("row k=3");
    let text = |c: &Cell| match c {
        Cell::Text(s) => s.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(text(&row[1]), "a", "CHAR stops at the first NUL (strnlen)");
    assert_eq!(text(&row[2]), "a\0b", "VARCHAR keeps the NUL");
    assert_eq!(text(&row[3]), "a\0b", "STRING keeps the NUL");
}

#[test]
fn plain_page_v1_decodes_values_in_order() {
    // "ab", "", "cde": strings | offsets 0, 2, 2 | num_elems 3
    let mut page = b"abcde".to_vec();
    for v in [0u32, 2, 2, 3] {
        page.extend_from_slice(&v.to_le_bytes());
    }
    let values = decode_plain_v1(&page).unwrap();
    assert_eq!(values, vec![&b"ab"[..], &b""[..], &b"cde"[..]]);
}

#[test]
fn plain_page_offsets_outside_page_are_an_error() {
    // num_elems 5 implies a 24-byte trailer in a 14-byte page
    let mut page = b"ab".to_vec();
    for v in [0u32, 1, 5] {
        page.extend_from_slice(&v.to_le_bytes());
    }
    assert!(matches!(decode_plain_v1(&page), Err(Error::Corrupt(_))));
    // an offset past the start of the offsets array
    let mut page = b"ab".to_vec();
    for v in [0u32, 100, 2] {
        page.extend_from_slice(&v.to_le_bytes());
    }
    assert!(matches!(decode_plain_v1(&page), Err(Error::Corrupt(_))));
}

#[test]
fn plain_page_decreasing_offsets_are_an_error() {
    let mut page = b"abcd".to_vec();
    for v in [2u32, 1, 2] {
        page.extend_from_slice(&v.to_le_bytes());
    }
    assert!(matches!(decode_plain_v1(&page), Err(Error::Corrupt(_))));
}

#[test]
fn dictionary_codeword_out_of_range_is_an_error() {
    // a real, crc-valid codeword page from the reference, decoded against a dictionary cut
    // short: codewords beyond the shortened dictionary must be an error, not a panic
    let m = Manifest::load();
    let seg = SegmentFile::open(&m.resolve(&m.tables["scalar_others"].files[0].path)).unwrap();
    let meta = column_meta(&seg, 6).unwrap(); // c_char: 8 distinct values in this segment
    let mut dict = read_dictionary(&seg, meta).unwrap();
    assert_eq!(dict.len(), 8);
    let ptr = data_pages(&seg, meta).unwrap()[0];
    let page = read_page(&seg, &ptr, meta.compression()).unwrap();
    let dpf = page.footer.data_page_footer.unwrap();
    let values = &page.body[..page.body.len() - dpf.nullmap_size.unwrap() as usize];
    let (mode, full) = decode_dict_data_page(values, 8, &dict).unwrap();
    assert_eq!((mode, full.len()), (DictPageMode::Dict, 8));
    dict.truncate(2);
    let err = decode_dict_data_page(values, 8, &dict).unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
}

#[test]
fn dictionary_pointer_past_the_end_of_the_file_is_an_error_not_a_panic() {
    use doris_segment::pb::segment_v2::{
        ColumnMetaPb, CompressionTypePb, EncodingTypePb, PagePointerPb, SegmentFooterPb,
    };
    // a crc-valid footer whose dictionary pointer sits at u64::MAX: offset + size overflows
    let column = ColumnMetaPb {
        column_id: Some(0),
        unique_id: Some(0),
        r#type: Some(17),
        length: Some(66),
        encoding: Some(EncodingTypePb::DictEncoding as i32),
        compression: Some(CompressionTypePb::NoCompression as i32),
        is_nullable: Some(true),
        dict_page: Some(PagePointerPb {
            offset: u64::MAX,
            size: 16,
        }),
        ..Default::default()
    };
    let footer = SegmentFooterPb {
        version: Some(1),
        columns: vec![column],
        num_rows: Some(1),
        ..Default::default()
    };
    let seg = common::segment(Vec::new(), footer);
    let err = doris_segment::column::read_column(&seg, 0).unwrap_err();
    assert!(matches!(err, Error::PageOutOfBounds { .. }), "{err}");
}
