//! `map`, `struct`, `named_struct`, `element_at` on a MAP or STRUCT, and the MAP cast, as the
//! C++ BE computes them (`function_map.cpp`, `function_struct.cpp`, `function_array_element.h`,
//! `column_map.cpp` `deduplicate_keys`; research `2026-11-12-m6-s7b6-arrays.md` §2).
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::row_text::cell_text;
use doris_exec::ExecError;
use doris_fixtures::build::{array_literal, array_of, blank, call, float, int, map_of, null, over, scalar, string, struct_of, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::{TExpr, TExprNodeType as N};
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn eval(e: &TExpr) -> Result<(doris_exec::value::ColType, Datum), ExecError> {
    check_read(e)?;
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    let (t, mut v) = eval_column(e, &block, &SlotMap::default())?;
    Ok((t, v.remove(0)))
}

/// The value's text in a result row ("NULL" for NULL).
fn text(e: &TExpr) -> String {
    let (t, v) = eval(e).expect("eval");
    cell_text(&v, &t).map_or("NULL".into(), |b| String::from_utf8(b).unwrap())
}

fn varchar(len: i32) -> TTypeDesc {
    scalar(T::VARCHAR, len, 0, 0)
}

fn bigint(v: i64) -> TExpr {
    int(v, ty(T::BIGINT))
}

fn map(t: TTypeDesc, args: Vec<TExpr>) -> TExpr {
    call("map", t, args)
}

#[test]
fn map_keeps_the_last_duplicate() {
    let t = map_of(varchar(1), ty(T::BIGINT));
    let m = map(t.clone(), vec![string("a"), bigint(1), string("b"), bigint(2), string("a"), bigint(3)]);
    assert_eq!(text(&m), "{\"b\":2, \"a\":3}");
    assert_eq!(text(&map(t.clone(), vec![])), "{}");
    // NULL keys and values are entries; two NULL keys are the same key
    let m = map(
        t.clone(),
        vec![null(varchar(1)), bigint(1), string("b"), null(ty(T::BIGINT)), null(varchar(1)), bigint(2)],
    );
    assert_eq!(text(&m), "{\"b\":null, null:2}");
    assert_eq!(
        text(&map(map_of(varchar(8), ty(T::BIGINT)), vec![string("exp_seac"), bigint(6)])),
        "{\"exp_seac\":6}"
    );
}

#[test]
fn float_keys_compare_by_bits() {
    let t = map_of(ty(T::DOUBLE), ty(T::BIGINT));
    let m = map(t, vec![float(0.0, ty(T::DOUBLE)), bigint(1), float(-0.0, ty(T::DOUBLE)), bigint(2)]);
    assert_eq!(text(&m), "{0:1, -0:2}");
}

#[test]
fn cast_map_dedups_keys() {
    // two VARCHAR keys equal once cast to INT: the last wins
    let from = map_of(varchar(2), ty(T::TINYINT));
    let m = map(from, vec![string("1"), int(1, ty(T::TINYINT)), string("01"), int(2, ty(T::TINYINT))]);
    let e = over(blank(N::CAST_EXPR, map_of(ty(T::INT), ty(T::BIGINT)), 0), vec![m]);
    assert_eq!(text(&e), "{1:2}");
    let m = map(map_of(varchar(7), ty(T::TINYINT)), vec![string("exp_sea"), int(1, ty(T::TINYINT))]);
    let e = over(blank(N::CAST_EXPR, map_of(ty(T::STRING), ty(T::BIGINT)), 0), vec![m]);
    assert_eq!(text(&e), "{\"exp_sea\":1}");
}

#[test]
fn a_map_of_arrays() {
    let t = map_of(varchar(3), array_of(ty(T::STRING)));
    let arr = array_literal(ty(T::STRING), vec![string("ab"), string("efg"), null(ty(T::STRING))]);
    assert_eq!(text(&map(t, vec![string("key"), arr])), "{\"key\":[\"ab\", \"efg\", null]}");
}

#[test]
fn struct_and_named_struct() {
    let t = struct_of(vec![
        ("col1", ty(T::TINYINT), false),
        ("col2", varchar(1), false),
        ("col3", ty(T::NULL_TYPE), true),
    ]);
    let s = call("struct", t, vec![int(1, ty(T::TINYINT)), string("a"), null(ty(T::NULL_TYPE))]);
    let (st, v) = eval(&s).unwrap();
    assert!(!st.nullable, "struct() is not nullable");
    assert_eq!(cell_text(&v, &st).unwrap(), b"{\"col1\":1, \"col2\":\"a\", \"col3\":null}");
    let t = struct_of(vec![
        ("f1", ty(T::TINYINT), false),
        ("f2", ty(T::TINYINT), false),
        ("f3", ty(T::TINYINT), false),
    ]);
    let n = call(
        "named_struct",
        t,
        vec![
            string("f1"),
            int(1, ty(T::TINYINT)),
            string("f2"),
            int(2, ty(T::TINYINT)),
            string("f3"),
            int(3, ty(T::TINYINT)),
        ],
    );
    assert_eq!(text(&n), "{\"f1\":1, \"f2\":2, \"f3\":3}");
}

fn element_at(container: TExpr, key: TExpr, to: TTypeDesc) -> TExpr {
    call("element_at", to, vec![container, key])
}

#[test]
fn element_at_on_a_map() {
    let t = map_of(varchar(32), ty(T::STRING));
    let m = || {
        map(
            t.clone(),
            vec![
                string("LA"),
                string("x"),
                string("LB"),
                null(ty(T::STRING)),
                null(varchar(32)),
                string("n"),
            ],
        )
    };
    assert_eq!(text(&element_at(m(), string("LA"), ty(T::STRING))), "x");
    assert_eq!(text(&element_at(m(), string("LB"), ty(T::STRING))), "NULL");
    assert_eq!(text(&element_at(m(), string("LC"), ty(T::STRING))), "NULL");
    // a NULL probe finds the NULL key
    assert_eq!(text(&element_at(m(), null(ty(T::STRING)), ty(T::STRING))), "n");
    assert_eq!(text(&element_at(null(t.clone()), string("LA"), ty(T::STRING))), "NULL");
    let big = map(map_of(ty(T::BIGINT), ty(T::BIGINT)), vec![bigint(i64::MAX), bigint(7)]);
    assert_eq!(text(&element_at(big, bigint(i64::MAX), ty(T::BIGINT))), "7");
}

#[test]
fn element_at_on_a_struct() {
    let t = struct_of(vec![("f1", ty(T::TINYINT), false), ("f3", ty(T::BIGINT), false)]);
    let s = || {
        call(
            "named_struct",
            t.clone(),
            vec![string("f1"), int(1, ty(T::TINYINT)), string("f3"), bigint(10_000_000_000)],
        )
    };
    assert_eq!(text(&element_at(s(), string("f1"), ty(T::TINYINT))), "1");
    // names match case-insensitively
    assert_eq!(text(&element_at(s(), string("F3"), ty(T::BIGINT))), "10000000000");
    match eval(&element_at(s(), string("f9"), ty(T::TINYINT))) {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "RUNTIME_ERROR");
            assert_eq!(
                msg,
                "Element not found for function element_at: name f9 not found in Struct(f1:TINYINT, f3:BIGINT)."
            );
        }
        other => panic!("{other:?}"),
    }
}

/// Review M3: a nested MAP key is refused (lookups by it would not match as C++'s compare_at).
#[test]
fn nested_map_keys_are_refused() {
    let t = map_of(array_of(ty(T::INT)), ty(T::BIGINT));
    let m = map(t.clone(), vec![array_literal(ty(T::INT), vec![int(1, ty(T::INT))]), bigint(7)]);
    assert!(matches!(check_read(&m), Err(ExecError::Unsupported { .. })));
    let probe = array_literal(ty(T::INT), vec![int(1, ty(T::INT))]);
    let e = element_at(doris_fixtures::build::slot(1, t), probe, ty(T::BIGINT));
    assert!(matches!(check_read(&e), Err(ExecError::Unsupported { .. })));
}

/// Review M5/M6: element_at on a STRUCT resolves its field at prepare, as C++ does when it works
/// out the return type: an unknown name fails with no rows at all, and the result type must be
/// the named field's.
#[test]
fn element_at_on_a_struct_resolves_at_prepare() {
    let t = struct_of(vec![("a", array_of(ty(T::INT)), true), ("f1", ty(T::TINYINT), false)]);
    let e = element_at(doris_fixtures::build::slot(1, t.clone()), string("zz"), ty(T::TINYINT));
    match check_read(&e) {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "RUNTIME_ERROR");
            assert_eq!(
                msg,
                "Element not found for function element_at: name zz not found in Struct(a:Nullable(Array(Nullable(INT))), f1:TINYINT)."
            );
        }
        other => panic!("{other:?}"),
    }
    // the right name, the wrong result type
    let e = element_at(doris_fixtures::build::slot(1, t.clone()), string("f1"), ty(T::BIGINT));
    assert!(matches!(check_read(&e), Err(ExecError::Unsupported { .. })));
    // a NULL name is refused
    let e = element_at(doris_fixtures::build::slot(1, t), null(ty(T::STRING)), ty(T::TINYINT));
    assert!(check_read(&e).is_err());
}
