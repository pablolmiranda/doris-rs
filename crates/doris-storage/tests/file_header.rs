use doris_proto::doris::{PUniqueId, TabletMetaPb};
use doris_storage::{decode_file_header, encode_file_header};
use prost::Message;

#[test]
fn the_file_header_is_the_cpp_one() {
    // FileHeader<TabletMetaPB> (be/src/storage/file_header.h): packed FixedFileHeaderV2 (magic
    // u64 0, version u32 1 = DORIS_V1, file_length u64, checksum u32 0, protobuf_length u64,
    // protobuf_checksum u32 = adler32 of the protobuf), a u32 extra header of 0, the protobuf
    let meta = TabletMetaPb { tablet_id: Some(7), schema_hash: Some(9), tablet_uid: Some(PUniqueId { hi: 1, lo: 2 }), ..Default::default() };
    let proto = meta.encode_to_vec();
    let bytes = encode_file_header(&meta);
    assert_eq!(bytes.len(), 40 + proto.len());
    assert_eq!(&bytes[0..8], &0u64.to_le_bytes());
    assert_eq!(&bytes[8..12], &1u32.to_le_bytes());
    assert_eq!(&bytes[12..20], &(bytes.len() as u64).to_le_bytes());
    assert_eq!(&bytes[20..24], &0u32.to_le_bytes());
    assert_eq!(&bytes[24..32], &(proto.len() as u64).to_le_bytes());
    assert_eq!(&bytes[32..36], &doris_storage::adler32(&proto).to_le_bytes());
    assert_eq!(&bytes[36..40], &0u32.to_le_bytes());
    assert_eq!(&bytes[40..], proto.as_slice());
    assert_eq!(decode_file_header(&bytes).unwrap(), meta);
    // zlib's adler32 of "Wikipedia"
    assert_eq!(doris_storage::adler32(b"Wikipedia"), 0x11E6_0398);
}
