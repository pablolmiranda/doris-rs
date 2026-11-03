//! Compact protocol field headers follow the Java and C++ rule: a field id up to 15 above the
//! previous one is written in the short (delta) form, delta 15 included.
use doris_thrift::compact::CompactOutputProtocol;
use thrift::protocol::{
    TFieldIdentifier, TListIdentifier, TOutputProtocol, TStructIdentifier, TType,
};

fn encode(write: impl FnOnce(&mut dyn TOutputProtocol)) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut o = CompactOutputProtocol::new(&mut bytes);
        o.write_struct_begin(&TStructIdentifier::new("s")).unwrap();
        write(&mut o);
        o.write_field_stop().unwrap();
        o.write_struct_end().unwrap();
        o.flush().unwrap();
    }
    bytes
}

fn i32_field(o: &mut dyn TOutputProtocol, id: i16, v: i32) {
    o.write_field_begin(&TFieldIdentifier::new("f", TType::I32, id))
        .unwrap();
    o.write_i32(v).unwrap();
    o.write_field_end().unwrap();
}

#[test]
fn field_delta_of_15_uses_the_short_form() {
    // header 0xf5: delta 15, type i32; value zigzag(1) = 2; stop
    assert_eq!(encode(|o| i32_field(o, 15, 1)), [0xf5, 0x02, 0x00]);
}

#[test]
fn bool_field_delta_of_15_uses_the_short_form() {
    let bytes = encode(|o| {
        o.write_field_begin(&TFieldIdentifier::new("b", TType::Bool, 15))
            .unwrap();
        o.write_bool(true).unwrap();
        o.write_field_end().unwrap();
    });
    assert_eq!(bytes, [0xf1, 0x00]);
}

#[test]
fn field_delta_over_15_uses_the_long_form() {
    // type i32, then the field id as zigzag varint (16 -> 32)
    assert_eq!(encode(|o| i32_field(o, 16, 1)), [0x05, 0x20, 0x02, 0x00]);
    // a field id below the previous one is written in the long form too
    assert_eq!(
        encode(|o| {
            i32_field(o, 2, 1);
            i32_field(o, 1, 1);
        }),
        [0x25, 0x02, 0x05, 0x02, 0x02, 0x00]
    );
}

#[test]
fn nested_struct_restarts_and_restores_field_deltas() {
    let bytes = encode(|o| {
        o.write_field_begin(&TFieldIdentifier::new("n", TType::Struct, 1))
            .unwrap();
        o.write_struct_begin(&TStructIdentifier::new("n")).unwrap();
        i32_field(o, 15, 1);
        o.write_field_stop().unwrap();
        o.write_struct_end().unwrap();
        o.write_field_end().unwrap();
        i32_field(o, 16, 2);
    });
    assert_eq!(bytes, [0x1c, 0xf5, 0x02, 0x00, 0xf5, 0x04, 0x00]);
}

#[test]
fn bools_inside_a_list_are_one_byte_each() {
    let bytes = encode(|o| {
        o.write_field_begin(&TFieldIdentifier::new("l", TType::List, 1))
            .unwrap();
        o.write_list_begin(&TListIdentifier::new(TType::Bool, 2))
            .unwrap();
        o.write_bool(true).unwrap();
        o.write_bool(false).unwrap();
        o.write_list_end().unwrap();
        o.write_field_end().unwrap();
    });
    assert_eq!(bytes, [0x19, 0x21, 0x01, 0x02, 0x00]);
}
