//! Canonical form of strict TBinaryProtocol messages: equal exactly when two encodings differ
//! only in the order of set elements and map entries.
use doris_wire::canonical::{binary_message, CanonicalError};
use thrift::protocol::{
    TBinaryOutputProtocol, TFieldIdentifier, TListIdentifier, TMapIdentifier, TMessageIdentifier,
    TMessageType, TOutputProtocol, TSetIdentifier, TStructIdentifier, TType,
};

/// A strict binary call message whose args struct is written by `body`.
fn message(body: impl FnOnce(&mut dyn TOutputProtocol)) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut o = TBinaryOutputProtocol::new(&mut bytes, true);
        o.write_message_begin(&TMessageIdentifier::new("m", TMessageType::Call, 7))
            .unwrap();
        o.write_struct_begin(&TStructIdentifier::new("args"))
            .unwrap();
        body(&mut o);
        o.write_field_stop().unwrap();
        o.write_struct_end().unwrap();
        o.write_message_end().unwrap();
    }
    bytes
}

fn set_and_map(o: &mut dyn TOutputProtocol, set: &[i64], map: &[(&str, i32)]) {
    o.write_field_begin(&TFieldIdentifier::new("s", TType::Set, 1))
        .unwrap();
    o.write_set_begin(&TSetIdentifier::new(TType::I64, set.len() as i32))
        .unwrap();
    for v in set {
        o.write_i64(*v).unwrap();
    }
    o.write_set_end().unwrap();
    o.write_field_end().unwrap();
    o.write_field_begin(&TFieldIdentifier::new("m", TType::Map, 2))
        .unwrap();
    o.write_map_begin(&TMapIdentifier::new(
        TType::String,
        TType::I32,
        map.len() as i32,
    ))
    .unwrap();
    for (k, v) in map {
        o.write_string(k).unwrap();
        o.write_i32(*v).unwrap();
    }
    o.write_map_end().unwrap();
    o.write_field_end().unwrap();
}

fn list(o: &mut dyn TOutputProtocol, id: i16, values: &[i64]) {
    o.write_field_begin(&TFieldIdentifier::new("l", TType::List, id))
        .unwrap();
    o.write_list_begin(&TListIdentifier::new(TType::I64, values.len() as i32))
        .unwrap();
    for v in values {
        o.write_i64(*v).unwrap();
    }
    o.write_list_end().unwrap();
    o.write_field_end().unwrap();
}

#[test]
fn set_elements_and_map_entries_are_sorted() {
    let hash_order = message(|o| set_and_map(o, &[5, 3, 4], &[("b", 1), ("a", 2)]));
    let sorted = message(|o| set_and_map(o, &[3, 4, 5], &[("a", 2), ("b", 1)]));
    assert_ne!(hash_order, sorted);
    assert_eq!(binary_message(&hash_order), binary_message(&sorted));
    // a different element is still a difference
    let other = message(|o| set_and_map(o, &[3, 4, 6], &[("a", 2), ("b", 1)]));
    assert_ne!(binary_message(&other), binary_message(&sorted));
}

#[test]
fn list_order_is_kept() {
    let a = message(|o| list(o, 1, &[5, 3]));
    let b = message(|o| list(o, 1, &[3, 5]));
    assert_eq!(binary_message(&a).unwrap(), a);
    assert_ne!(binary_message(&a), binary_message(&b));
}

#[test]
fn collections_inside_nested_structs_and_lists_are_canonical() {
    let nested = |inner: [i32; 2]| {
        message(|o| {
            o.write_field_begin(&TFieldIdentifier::new("n", TType::Struct, 3))
                .unwrap();
            o.write_struct_begin(&TStructIdentifier::new("n")).unwrap();
            o.write_field_begin(&TFieldIdentifier::new("ls", TType::List, 1))
                .unwrap();
            o.write_list_begin(&TListIdentifier::new(TType::Set, 1))
                .unwrap();
            o.write_set_begin(&TSetIdentifier::new(TType::I32, 2))
                .unwrap();
            for v in inner {
                o.write_i32(v).unwrap();
            }
            o.write_set_end().unwrap();
            o.write_list_end().unwrap();
            o.write_field_end().unwrap();
            o.write_field_stop().unwrap();
            o.write_struct_end().unwrap();
            o.write_field_end().unwrap();
        })
    };
    assert_eq!(
        binary_message(&nested([2, 1])),
        binary_message(&nested([1, 2]))
    );
}

#[test]
fn malformed_input_is_an_error_not_a_panic() {
    let m = message(|o| set_and_map(o, &[5, 3], &[("b", 1)]));
    for cut in 0..m.len() {
        assert!(binary_message(&m[..cut]).is_err(), "prefix of {cut} bytes");
    }
    let mut trailing = m.clone();
    trailing.push(0);
    assert_eq!(
        binary_message(&trailing),
        Err(CanonicalError::TrailingBytes(1))
    );
    // a non-strict (unversioned) header
    assert_eq!(binary_message(&m[4..]), Err(CanonicalError::BadHeader));
}

#[test]
fn nesting_deeper_than_the_limit_is_an_error() {
    // built by hand: the thrift writer itself refuses to nest this deep
    let mut deep = message(|_| {});
    deep.pop(); // the args struct's stop byte
    for _ in 0..100 {
        deep.extend_from_slice(&[0x0c, 0x00, 0x01]); // field 1, type struct
    }
    deep.extend(std::iter::repeat_n(0x00, 101)); // stop every struct, args included
    assert_eq!(binary_message(&deep), Err(CanonicalError::TooDeep));
    // the same shape within the limit is fine
    let mut ok = message(|_| {});
    ok.pop();
    for _ in 0..10 {
        ok.extend_from_slice(&[0x0c, 0x00, 0x01]);
    }
    ok.extend(std::iter::repeat_n(0x00, 11));
    assert_eq!(binary_message(&ok).unwrap(), ok);
}
