//! Profile info strings are bytes: the BE writes raw key bytes into them (a scan's key range
//! reads `ScanKeys:ScanKey=[null : \xff]`), which are not UTF-8.
use doris_thrift::gen::runtime_profile::TRuntimeProfileNode;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol, TSerializable};

/// A TRuntimeProfileNode in the binary protocol, the way the BE sends one, with one info
/// string whose value is not UTF-8.
fn node_bytes() -> Vec<u8> {
    let s = |b: &[u8]| [&(b.len() as u32).to_be_bytes()[..], b].concat();
    [
        &[11u8, 0, 1][..], &s(b"OlapScanner"),
        &[8, 0, 2, 0, 0, 0, 0],
        &[15, 0, 3, 12, 0, 0, 0, 0],
        &[10, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0],
        &[2, 0, 5, 1],
        &[13, 0, 6, 11, 11, 0, 0, 0, 1], &s(b"KeyRanges"), &s(b"ScanKeys:ScanKey=[null : \xff]"),
        &[15, 0, 7, 11, 0, 0, 0, 1], &s(b"KeyRanges"),
        &[13, 0, 8, 11, 14, 0, 0, 0, 0],
        &[10, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0],
        &[0],
    ]
    .concat()
}

#[test]
fn an_info_string_that_is_not_utf8_round_trips_byte_for_byte() {
    let bytes = node_bytes();
    let mut input = TBinaryInputProtocol::new(&bytes[..], true);
    let node = TRuntimeProfileNode::read_from_in_protocol(&mut input).expect("decodes");
    assert_eq!(node.info_strings["KeyRanges"], b"ScanKeys:ScanKey=[null : \xff]".to_vec());
    let mut out = Vec::new();
    node.write_to_out_protocol(&mut TBinaryOutputProtocol::new(&mut out, true)).expect("encodes");
    assert_eq!(out, bytes);
}
