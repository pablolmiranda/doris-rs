//! ARRAY, MAP and STRUCT values: their types, their text in a result row and their literals and
//! casts, as the C++ BE prints them (`docs/superpowers/research/2026-11-12-m6-s7b6-arrays.md` §1).
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::row_text::cell_text;
use doris_exec::value::ColType;
use doris_exec::ExecError;
use doris_fixtures::build::{array_literal, array_of, blank, int, map_of, null, over, scalar, slot, string, struct_of, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::{TExpr, TExprNodeType as N};
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn col(t: &TTypeDesc) -> ColType {
    ColType::from_type_desc("c", t, true).expect("a type doris-rs describes")
}

fn text(d: Datum, t: &TTypeDesc) -> String {
    String::from_utf8(cell_text(&d, &col(t)).expect("not null")).unwrap()
}

fn bytes(s: &str) -> Datum {
    Datum::Bytes(s.as_bytes().to_vec())
}

fn varchar(len: i32) -> TTypeDesc {
    scalar(T::VARCHAR, len, 0, 0)
}

#[test]
fn nested_cells_print_as_cpp() {
    assert_eq!(text(Datum::Array(vec![Datum::Null; 3]), &array_of(ty(T::INT))), "[null, null, null]");
    assert_eq!(text(Datum::Array(vec![]), &array_of(ty(T::INT))), "[]");
    assert_eq!(text(Datum::Array(vec![bytes("Hello")]), &array_of(varchar(5))), "[\"Hello\"]");
    assert_eq!(
        text(
            Datum::Array(vec![Datum::Array(vec![]), Datum::Array(vec![])]),
            &array_of(array_of(ty(T::NULL_TYPE)))
        ),
        "[[], []]"
    );
    let dec = array_of(scalar(T::DECIMAL32, -1, 2, 1));
    assert_eq!(
        text(Datum::Array(vec![Datum::Int(5), Datum::Int(7), Datum::Int(10)]), &dec),
        "[0.5, 0.7, 1.0]"
    );
    let m = map_of(varchar(8), ty(T::BIGINT));
    assert_eq!(text(Datum::Map(vec![(bytes("exp_seac"), Datum::Int(6))]), &m), "{\"exp_seac\":6}");
    assert_eq!(text(Datum::Map(vec![]), &m), "{}");
    let m = map_of(varchar(3), array_of(varchar(3)));
    let v = Datum::Array(vec![bytes("ab"), bytes("efg"), Datum::Null]);
    assert_eq!(text(Datum::Map(vec![(bytes("key"), v)]), &m), "{\"key\":[\"ab\", \"efg\", null]}");
    let s = struct_of(vec![
        ("col1", ty(T::TINYINT), false),
        ("col2", varchar(1), false),
        ("col3", ty(T::NULL_TYPE), true),
    ]);
    assert_eq!(
        text(Datum::Struct(vec![Datum::Int(1), bytes("a"), Datum::Null]), &s),
        "{\"col1\":1, \"col2\":\"a\", \"col3\":null}"
    );
    let s = struct_of(vec![("col", ty(T::TINYINT), true), ("col1", scalar(T::DECIMAL32, -1, 7, 2), true)]);
    assert_eq!(
        text(Datum::Struct(vec![Datum::Int(1), Datum::Int(1234524)]), &s),
        "{\"col\":1, \"col1\":12345.24}"
    );
    // a NULL cell is MySQL NULL, whatever its type
    assert_eq!(cell_text(&Datum::Null, &col(&s)), None);
}

#[test]
fn dates_inside_an_array_are_quoted() {
    let at = |s: u64| Datum::Int(((2024u64 << 46) | (1 << 42) | (1 << 37) | (s << 20) | 123456) as i128);
    let t = array_of(scalar(T::DATETIMEV2, -1, 0, 6));
    assert_eq!(
        text(Datum::Array(vec![at(0), at(1)]), &t),
        "[\"2024-01-01 00:00:00.123456\", \"2024-01-01 00:00:01.123456\"]"
    );
    let d = Datum::Int(((2024 << 9) | (1 << 5) | 2) as i128);
    assert_eq!(text(Datum::Array(vec![d]), &array_of(ty(T::DATEV2))), "[\"2024-01-02\"]");
}

#[test]
fn strings_inside_are_not_escaped() {
    assert_eq!(text(Datum::Array(vec![bytes("a\"b, c")]), &array_of(ty(T::STRING))), "[\"a\"b, c\"]");
}

#[test]
fn type_descs_nest() {
    let a = col(&array_of(array_of(ty(T::NULL_TYPE))));
    assert_eq!(a.ptype, T::ARRAY);
    let inner = a.element().unwrap();
    assert_eq!(inner.ptype, T::ARRAY);
    assert_eq!(inner.element().unwrap().ptype, T::NULL_TYPE);
    let m = col(&map_of(varchar(3), array_of(ty(T::STRING))));
    assert_eq!(m.ptype, T::MAP);
    let (k, v) = m.map_types().unwrap();
    assert_eq!((k.ptype, k.len, k.nullable), (T::VARCHAR, 3, true));
    assert_eq!(v.element().unwrap().ptype, T::STRING);
    let s = col(&struct_of(vec![("f1", ty(T::TINYINT), false), ("F2", varchar(2), true)]));
    assert_eq!(s.ptype, T::STRUCT);
    let f: Vec<(String, T, bool)> = s.fields().unwrap().into_iter().map(|(n, t)| (n, t.ptype, t.nullable)).collect();
    assert_eq!(f, vec![("f1".into(), T::TINYINT, false), ("F2".into(), T::VARCHAR, true)]);
    // the old ARRAY<scalar> form still reads
    assert_eq!(col(&array_of(ty(T::INT))).element().unwrap().ptype, T::INT);
}

fn one_row(e: &TExpr) -> Result<Vec<Datum>, ExecError> {
    check_read(e)?;
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    Ok(eval_column(e, &block, &SlotMap::default())?.1)
}

#[test]
fn array_literals_with_null_empty_and_nested_children() {
    assert_eq!(one_row(&array_literal(ty(T::NULL_TYPE), vec![])).unwrap(), vec![Datum::Array(vec![])]);
    let e = array_literal(ty(T::STRING), vec![null(ty(T::STRING)), string("a")]);
    assert_eq!(one_row(&e).unwrap(), vec![Datum::Array(vec![Datum::Null, bytes("a")])]);
    let inner = array_literal(ty(T::TINYINT), vec![int(1, ty(T::TINYINT))]);
    let e = array_literal(array_of(ty(T::TINYINT)), vec![inner, array_literal(ty(T::TINYINT), vec![])]);
    assert_eq!(
        one_row(&e).unwrap(),
        vec![Datum::Array(vec![Datum::Array(vec![Datum::Int(1)]), Datum::Array(vec![])])]
    );
}

#[test]
fn cast_array_of_int_to_array_of_varchar() {
    let arr = array_literal(ty(T::INT), vec![int(1, ty(T::INT)), int(23, ty(T::INT)), null(ty(T::INT))]);
    let e = over(blank(N::CAST_EXPR, array_of(varchar(65533)), 0), vec![arr]);
    assert_eq!(one_row(&e).unwrap(), vec![Datum::Array(vec![bytes("1"), bytes("23"), Datum::Null])]);
    // a NULL array stays NULL
    let e = over(blank(N::CAST_EXPR, array_of(varchar(65533)), 0), vec![null(array_of(ty(T::INT)))]);
    assert_eq!(one_row(&e).unwrap(), vec![Datum::Null]);
}

#[test]
fn a_nested_key_is_refused() {
    for (t, name) in [
        (array_of(ty(T::INT)), "an ARRAY"),
        (map_of(ty(T::INT), ty(T::INT)), "a MAP"),
        (struct_of(vec![("f", ty(T::INT), true)]), "a STRUCT"),
    ] {
        match doris_exec::query::check_keys([&slot(1, t)], "a sort key") {
            Err(ExecError::Unsupported { what }) => assert_eq!(what, format!("{name} as a sort key")),
            other => panic!("{other:?}"),
        }
    }
    doris_exec::query::check_keys([&slot(1, ty(T::INT))], "a sort key").unwrap();
}

/// Review I1: a nested value of an unchanged type keeps its bytes (no text round trip).
#[test]
fn nested_values_keep_their_bytes() {
    let t = col(&array_of(ty(T::STRING)));
    let raw = Datum::Array(vec![Datum::Bytes(vec![0xFF]), bytes("a")]);
    assert_eq!(doris_exec::eval::coerce(raw.clone(), &t, &t).unwrap(), raw);
    let t3 = col(&array_of(varchar(3)));
    let long = Datum::Array(vec![bytes("abcdef")]);
    assert_eq!(doris_exec::eval::coerce(long.clone(), &t3, &t3).unwrap(), long);
    let s = col(&struct_of(vec![("f", ty(T::STRING), true)]));
    let raw = Datum::Struct(vec![Datum::Bytes(vec![0xFF])]);
    assert_eq!(doris_exec::eval::coerce(raw.clone(), &s, &s).unwrap(), raw);
    // the text keeps them too
    assert_eq!(
        cell_text(&Datum::Array(vec![Datum::Bytes(vec![0xFF])]), &t).unwrap(),
        b"[\"\xFF\"]".to_vec()
    );
    let m = col(&map_of(ty(T::STRING), ty(T::STRING)));
    let d = Datum::Map(vec![(Datum::Bytes(vec![0xFE]), Datum::Bytes(vec![0xFF]))]);
    assert_eq!(cell_text(&d, &m).unwrap(), b"{\"\xFE\":\"\xFF\"}".to_vec());
}

/// Review I2: a cast to a nested type from a scalar (C++ parses the text) is refused.
#[test]
fn a_cast_to_a_nested_type_from_a_scalar_is_refused() {
    for to in [array_of(ty(T::INT)), map_of(ty(T::STRING), ty(T::INT))] {
        let e = over(blank(N::CAST_EXPR, to, 0), vec![string("[1,2]")]);
        match check_read(&e) {
            Err(ExecError::Unsupported { what }) => assert!(what.starts_with("a cast from String to "), "{what}"),
            other => panic!("{other:?}"),
        }
    }
    // a STRUCT cast to a different number of fields is refused too
    let from = struct_of(vec![("a", ty(T::INT), true)]);
    let to = struct_of(vec![("a", ty(T::INT), true), ("b", ty(T::INT), true)]);
    let e = over(blank(N::CAST_EXPR, to, 0), vec![slot(1, from)]);
    assert!(matches!(check_read(&e), Err(ExecError::Unsupported { .. })));
}

/// Review M7: values whose shape does not match the types are an error, not NULLs.
#[test]
fn a_shape_mismatch_is_an_error() {
    let one = col(&struct_of(vec![("a", ty(T::INT), true)]));
    let two = col(&struct_of(vec![("a", ty(T::INT), true), ("b", ty(T::INT), true)]));
    assert!(doris_exec::eval::coerce(Datum::Struct(vec![Datum::Int(1)]), &one, &two).is_err());
    let arr = col(&array_of(ty(T::INT)));
    assert!(doris_exec::eval::coerce(Datum::Map(vec![]), &arr, &arr).is_err());
}
