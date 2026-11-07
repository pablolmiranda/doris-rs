//! The short key page and the zone map index of every corpus segment.
mod common;

use doris_segment::corpus::Manifest;
use doris_segment::ordinal::data_pages;
use doris_segment::{read_short_keys, read_zone_maps, SegmentFile};

fn segments() -> Vec<(String, SegmentFile)> {
    let m = Manifest::load();
    let mut out = Vec::new();
    for (name, t) in &m.tables {
        for f in &t.files {
            out.push((name.clone(), SegmentFile::open(&m.resolve(&f.path)).unwrap()));
        }
    }
    assert!(!out.is_empty());
    out
}

#[test]
fn corpus_short_keys_decode() {
    for (name, seg) in segments() {
        let rows = seg.footer.num_rows.unwrap();
        let sk = read_short_keys(&seg).unwrap_or_else(|e| panic!("{name}: {e}"));
        // ShortKeyIndexBuilder: one entry at row 0 and every num_rows_per_block rows after it
        assert_eq!(sk.num_rows_per_block, 1024, "{name}");
        assert_eq!(sk.num_segment_rows, rows, "{name}");
        assert_eq!(sk.num_items, rows.div_ceil(1024), "{name}");
        assert_eq!(sk.key_bytes.len(), sk.num_items as usize, "{name}");
        // each key starts with a column marker: 0x01 null first, 0x02 normal
        for k in &sk.key_bytes {
            assert!(matches!(k.first(), Some(0x01 | 0x02)), "{name}: {k:?}");
        }
    }
}

#[test]
fn corpus_zone_maps_decode() {
    for (name, seg) in segments() {
        for col in &seg.footer.columns {
            let id = col.unique_id.unwrap();
            let zm = read_zone_maps(&seg, id).unwrap_or_else(|e| panic!("{name} column {id}: {e}"));
            let pages = data_pages(&seg, col).unwrap();
            assert_eq!(zm.pages.len(), pages.len(), "{name} column {id}");
            // the segment zone map merges its pages'
            let any = |f: fn(&doris_segment::pb::segment_v2::ZoneMapPb) -> bool| zm.pages.iter().any(f);
            assert_eq!(zm.segment.has_null(), any(|z| z.has_null()), "{name} column {id}");
            assert_eq!(zm.segment.has_not_null(), any(|z| z.has_not_null()), "{name} column {id}");
        }
        // the key column k is NOT NULL in every corpus table and has rows
        let k = seg.footer.columns.first().unwrap();
        let zm = read_zone_maps(&seg, k.unique_id.unwrap()).unwrap();
        assert!(zm.segment.has_not_null() && !zm.segment.has_null(), "{name}");
    }
}
