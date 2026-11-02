//! Footer parsing against every segment file in the golden corpus.
use doris_segment::corpus::Manifest;
use doris_segment::{Error, SegmentFile};

#[test]
fn every_corpus_segment_footer_parses_and_rows_sum_to_expected() {
    let m = Manifest::load();
    assert!(
        m.tables.len() >= 4,
        "corpus should have at least four tables"
    );
    for (name, t) in &m.tables {
        let total: u64 = t
            .files
            .iter()
            .map(|f| {
                let seg = SegmentFile::open(&m.resolve(&f.path))
                    .unwrap_or_else(|e| panic!("{name}: {}: {e}", f.path.display()));
                assert_eq!(
                    seg.footer.version.unwrap_or(1),
                    1,
                    "{name}: V2 baseline footer expected"
                );
                assert!(
                    !seg.footer.columns.is_empty(),
                    "{name}: footer lists no columns"
                );
                u64::from(seg.footer.num_rows.expect("num_rows"))
            })
            .sum();
        assert_eq!(total, t.rows, "{name}: rows across segments");
    }
}

#[test]
fn too_short_file_is_an_error() {
    let err = doris_segment::read_footer(&[1, 2, 3, 4, 5]).unwrap_err();
    assert!(matches!(err, Error::TooShort { len: 5 }), "{err}");
}

#[test]
fn wrong_magic_is_an_error() {
    let m = Manifest::load();
    let mut bytes = m.read(&m.tables["scalar_ints"].files[0].path);
    let n = bytes.len();
    bytes[n - 4..].copy_from_slice(b"XXXX");
    let err = doris_segment::read_footer(&bytes).unwrap_err();
    assert!(matches!(err, Error::BadMagic(_)), "{err}");
}

#[test]
fn corrupted_footer_fails_checksum() {
    let m = Manifest::load();
    let mut bytes = m.read(&m.tables["scalar_ints"].files[0].path);
    let n = bytes.len();
    // flip a byte well inside the footer region (the footer is the last ~hundreds of bytes
    // before the 12-byte trailer)
    bytes[n - 40] ^= 0xFF;
    let err = doris_segment::read_footer(&bytes).unwrap_err();
    assert!(matches!(err, Error::ChecksumMismatch { .. }), "{err}");
}
