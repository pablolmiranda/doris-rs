//! Canonical form of TCompactProtocol structs (the plans fragment requests carry): equal exactly
//! when two encodings differ only in the order of set elements and map entries.
use doris_wire::canonical::{compact_struct, CanonicalError};
use thrift::protocol::{
    TCompactOutputProtocol, TFieldIdentifier, TListIdentifier, TMapIdentifier, TOutputProtocol,
    TSetIdentifier, TStructIdentifier, TType,
};

/// A compact struct: a bool, a set of i64, a map of string to a nested struct holding a set,
/// then a list, in the given orders.
fn plan(set: &[i64], map: &[(&str, &[i64])], list: &[i64]) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut o = TCompactOutputProtocol::new(&mut bytes);
        o.write_struct_begin(&TStructIdentifier::new("p")).unwrap();
        o.write_field_begin(&TFieldIdentifier::new("b", TType::Bool, 1))
            .unwrap();
        o.write_bool(true).unwrap();
        o.write_field_end().unwrap();
        o.write_field_begin(&TFieldIdentifier::new("s", TType::Set, 2))
            .unwrap();
        o.write_set_begin(&TSetIdentifier::new(TType::I64, set.len() as i32))
            .unwrap();
        for v in set {
            o.write_i64(*v).unwrap();
        }
        o.write_set_end().unwrap();
        o.write_field_end().unwrap();
        o.write_field_begin(&TFieldIdentifier::new("m", TType::Map, 20))
            .unwrap();
        o.write_map_begin(&TMapIdentifier::new(
            TType::String,
            TType::Struct,
            map.len() as i32,
        ))
        .unwrap();
        for (k, inner) in map {
            o.write_string(k).unwrap();
            o.write_struct_begin(&TStructIdentifier::new("i")).unwrap();
            o.write_field_begin(&TFieldIdentifier::new("is", TType::Set, 3))
                .unwrap();
            o.write_set_begin(&TSetIdentifier::new(TType::I64, inner.len() as i32))
                .unwrap();
            for v in *inner {
                o.write_i64(*v).unwrap();
            }
            o.write_set_end().unwrap();
            o.write_field_end().unwrap();
            o.write_field_stop().unwrap();
            o.write_struct_end().unwrap();
        }
        o.write_map_end().unwrap();
        o.write_field_end().unwrap();
        o.write_field_begin(&TFieldIdentifier::new("l", TType::List, 21))
            .unwrap();
        o.write_list_begin(&TListIdentifier::new(TType::I64, list.len() as i32))
            .unwrap();
        for v in list {
            o.write_i64(*v).unwrap();
        }
        o.write_list_end().unwrap();
        o.write_field_end().unwrap();
        o.write_field_stop().unwrap();
        o.write_struct_end().unwrap();
    }
    bytes
}

#[test]
fn set_elements_and_map_entries_are_sorted_at_every_depth() {
    let a = plan(&[3, 1, 2], &[("x", &[9, 8]), ("a", &[1])], &[5, 6]);
    let b = plan(&[1, 2, 3], &[("a", &[1]), ("x", &[8, 9])], &[5, 6]);
    assert_ne!(a, b);
    assert_eq!(compact_struct(&a), compact_struct(&b));
}

#[test]
fn list_order_is_kept() {
    let a = plan(&[1], &[], &[5, 6]);
    let b = plan(&[1], &[], &[6, 5]);
    assert_ne!(compact_struct(&a).unwrap(), compact_struct(&b).unwrap());
}

#[test]
fn canonical_of_sorted_input_is_the_input() {
    let a = plan(&[1, 2], &[("a", &[1])], &[2, 1]);
    assert_eq!(compact_struct(&a).unwrap(), a);
}

#[test]
fn truncated_input_is_an_error_not_a_panic() {
    let a = plan(&[1, 2], &[("a", &[1])], &[2, 1]);
    for cut in 0..a.len() {
        assert!(compact_struct(&a[..cut]).is_err(), "cut at {cut}");
    }
    // a field header whose type nibble is not a compact type (14)
    assert_eq!(compact_struct(&[0x1e]), Err(CanonicalError::BadType(14)));
}

#[test]
fn a_varint_longer_than_ten_bytes_is_reported_as_such() {
    // field 1, i32, whose varint never ends
    let mut bytes = vec![0x15];
    bytes.extend([0xff; 11]);
    assert_eq!(compact_struct(&bytes), Err(CanonicalError::VarintTooLong));
}
