//! Fixed-width scalar types (floats, decimals, dates, datetimes, IPs, legacy formats) against
//! the golden corpus.
mod common;

use common::assert_table_matches;
use doris_segment::bitshuffle::default_block_size;
use doris_segment::column::column_meta;
use doris_segment::corpus::Manifest;
use doris_segment::ordinal::data_pages;
use doris_segment::page::read_page;
use doris_segment::values::{
    unpack_date_v1, unpack_datetime_v1, unpack_datetimev2, unpack_datev2, Date, DateTime,
};
use doris_segment::SegmentFile;

const fn date(year: u32, month: u8, day: u8) -> Date {
    Date { year, month, day }
}

#[test]
fn datev2_and_datetimev2_unpack() {
    // literals computed independently from the bit layouts in vdatetime_value.h
    assert_eq!(unpack_datev2(1_036_381), date(2024, 2, 29));
    assert_eq!(unpack_datev2(545), date(1, 1, 1));
    assert_eq!(
        unpack_datetimev2(703_674_213_004_689_983),
        DateTime {
            date: date(9999, 12, 31),
            hour: 23,
            minute: 59,
            second: 59,
            microsecond: 999_999
        }
    );
    assert_eq!(
        unpack_datetimev2(138_630_961_515_462_657),
        DateTime {
            date: date(1970, 1, 1),
            hour: 0,
            minute: 0,
            second: 0,
            microsecond: 1
        }
    );
}

#[test]
fn legacy_date_and_datetime_unpack() {
    // DATE v1 is a 3-byte little-endian year << 9 | month << 5 | day
    assert_eq!(
        unpack_date_v1(u32::from_le_bytes([93, 208, 15, 0])),
        date(2024, 2, 29)
    );
    // DATETIME v1 is the integer YYYYMMDDhhmmss
    assert_eq!(
        unpack_datetime_v1(20_240_229_123_456),
        DateTime {
            date: date(2024, 2, 29),
            hour: 12,
            minute: 34,
            second: 56,
            microsecond: 0
        }
    );
}

#[test]
fn scalar_others_fixed_width_columns_match_expected() {
    assert_table_matches(
        "scalar_others",
        &[
            (0, "k"),
            (1, "c_float"),
            (2, "c_double"),
            (3, "c_decimal"),
            (4, "c_date"),
            (5, "c_datetime"),
        ],
    );
}

#[test]
fn scalar_more_fixed_width_columns_match_expected() {
    assert_table_matches(
        "scalar_more",
        &[
            (0, "k"),
            (1, "c_dec32"),
            (2, "c_dec128"),
            (3, "c_dt0"),
            (4, "c_dt3"),
            (5, "c_ipv4"),
            (6, "c_ipv6"),
            (7, "c_datev1"),
            (8, "c_datetimev1"),
            (9, "c_decv2"),
        ],
    );
}

/// Guard on the corpus itself: the 3-byte (DATE v1) and 12-byte (DECIMALV2) columns must keep
/// a bit-shuffle page larger than one LZ4 block, so the multi-block path stays exercised by
/// `scalar_more_fixed_width_columns_match_expected`.
#[test]
fn odd_element_sizes_cross_lz4_blocks() {
    let m = Manifest::load();
    let mut max_elems = [0usize; 2];
    for f in &m.tables["scalar_more"].files {
        let seg = SegmentFile::open(&m.resolve(&f.path)).unwrap();
        for (slot, uid) in [(0usize, 7u32), (1, 9)] {
            let meta = column_meta(&seg, uid).unwrap();
            for ptr in data_pages(&seg, meta).unwrap() {
                let page = read_page(&seg, &ptr, meta.compression()).unwrap();
                let n = u32::from_le_bytes(page.body[0..4].try_into().unwrap()) as usize;
                max_elems[slot] = max_elems[slot].max(n);
            }
        }
    }
    assert!(
        max_elems[0] > default_block_size(3),
        "c_datev1: largest page {} elems",
        max_elems[0]
    );
    assert!(
        max_elems[1] > default_block_size(12),
        "c_decv2: largest page {} elems",
        max_elems[1]
    );
}
