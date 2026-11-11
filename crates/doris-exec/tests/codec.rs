//! `encode_as_*`/`decode_as_varchar` (the FE's compressed materialize) and the hashes
//! (`murmur_hash3_32/64`, `xxhash_32/64`) as the C++ BE computes them: see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-strings-hashes.md` §2-3. Expected values are
//! the suites' recorded rows.
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::value::ColType;
use doris_exec::ExecError;
use doris_fixtures::build::{call, int, null, scalar, slot, string, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn vc(len: i32) -> TTypeDesc {
    scalar(T::VARCHAR, len, 0, 0)
}

fn eval(e: TExpr) -> Result<Datum, ExecError> {
    check_read(&e)?;
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    Ok(eval_column(&e, &block, &SlotMap::default())?.1.remove(0))
}

fn encode(name: &str, to: T, s: &str) -> Result<Datum, ExecError> {
    eval(call(name, ty(to), vec![string(s)]))
}

#[test]
fn encode_as_recorded() {
    assert_eq!(encode("encode_as_smallint", T::SMALLINT, "a").unwrap(), Datum::Int(12417));
    assert_eq!(encode("encode_as_int", T::INT, "ab").unwrap(), Datum::Int(816906242));
    assert_eq!(encode("encode_as_bigint", T::BIGINT, "abcd").unwrap(), Datum::Int(3508640225272266756));
    assert_eq!(
        encode("encode_as_largeint", T::LARGEINT, "abcdefgh").unwrap(),
        Datum::Int(64722988298011025234418333488001318920)
    );
    assert_eq!(
        encode("encode_as_largeint", T::LARGEINT, "abcdefghijklmno").unwrap(),
        Datum::Int(64722988298011025238216334405476497295)
    );
    assert_eq!(
        encode("encode_as_largeint", T::LARGEINT, "b").unwrap(),
        Datum::Int(65132171793460877772286545953736884225)
    );
    for (name, t) in [
        ("encode_as_smallint", T::SMALLINT),
        ("encode_as_int", T::INT),
        ("encode_as_bigint", T::BIGINT),
        ("encode_as_largeint", T::LARGEINT),
    ] {
        assert_eq!(encode(name, t, "").unwrap(), Datum::Int(0), "{name}");
    }
}

#[test]
fn decode_round_trips() {
    let decode = |t: T, v: i128| eval(call("decode_as_varchar", vc(65533), vec![int(v as i64, ty(t))]));
    assert_eq!(decode(T::SMALLINT, 8321).unwrap(), Datum::Bytes(b"A".to_vec()));
    assert_eq!(decode(T::INT, 816906242).unwrap(), Datum::Bytes(b"ab".to_vec()));
    assert_eq!(decode(T::BIGINT, 3508640225272266756).unwrap(), Datum::Bytes(b"abcd".to_vec()));
    // the longest string each type holds, a NUL kept by the length byte, and the empty string
    for (enc, t, s) in [
        ("encode_as_int", T::INT, "xyz"),
        ("encode_as_bigint", T::BIGINT, "a\0"),
        ("encode_as_smallint", T::SMALLINT, ""),
    ] {
        let Datum::Int(v) = encode(enc, t, s).unwrap() else { panic!() };
        let back = eval(call("decode_as_varchar", vc(65533), vec![doris_fixtures::build::int(v as i64, ty(t))])).unwrap();
        assert_eq!(back, Datum::Bytes(s.as_bytes().to_vec()), "{enc} {s:?}");
    }
    assert_eq!(eval(call("decode_as_varchar", vc(65533), vec![null(ty(T::INT))])).unwrap(), Datum::Null);
}

#[test]
fn too_long_and_invalid_values_fail() {
    match encode("encode_as_largeint", T::LARGEINT, "abcdefghijklmnop") {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "INTERNAL_ERROR");
            assert_eq!(
                msg,
                "String is too long to encode, input string size 16, max valid string size for encode_as_largeint is 15"
            );
        }
        other => panic!("{other:?}"),
    }
    match eval(call("decode_as_varchar", vc(65533), vec![int(2, ty(T::SMALLINT))])) {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "INVALID_ARGUMENT");
            assert_eq!(
                msg,
                "Invalid input of function decode_as_varchar, input type SMALLINT value 2, string size 2, should not be larger than 2"
            );
        }
        other => panic!("{other:?}"),
    }
}

/// The column's longest value decides, NULL rows included.
#[test]
fn encode_checks_the_whole_column() {
    let c = ColType::from_type_desc("", &vc(10), true).unwrap();
    let block = Block {
        types: vec![c],
        rows: vec![vec![Datum::Bytes(b"a".to_vec())], vec![Datum::Bytes(b"abcd".to_vec())]],
    };
    let e = call("encode_as_int", ty(T::INT), vec![slot(1, vc(10))]);
    check_read(&e).unwrap();
    match eval_column(&e, &block, &SlotMap([(1, 0)].into_iter().collect())) {
        Err(ExecError::Coded { msg, .. }) => assert!(msg.contains("input string size 4"), "{msg}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn hashes_match_reference() {
    let h = |name: &str, t: T, args: &[&str]| eval(call(name, ty(t), args.iter().map(|s| string(s)).collect())).unwrap();
    assert_eq!(h("murmur_hash3_32", T::INT, &["hello"]), Datum::Int(1321743225));
    assert_eq!(h("murmur_hash3_32", T::INT, &["hello", "world"]), Datum::Int(984713481));
    assert_eq!(h("murmur_hash3_64", T::BIGINT, &["hello"]), Datum::Int(-3215607508166160593));
    assert_eq!(h("murmur_hash3_64", T::BIGINT, &["hello", "world"]), Datum::Int(3583109472027628045));
    assert_eq!(h("xxhash_32", T::INT, &["hello"]), Datum::Int(-83855367));
    assert_eq!(h("xxhash_32", T::INT, &["hello", "world"]), Datum::Int(-920844969));
    assert_eq!(h("xxhash_64", T::BIGINT, &["hello"]), Datum::Int(-7685981735718036227));
    assert_eq!(h("xxhash_64", T::BIGINT, &["hello", "world"]), Datum::Int(7001965798170371843));
    assert_eq!(h("murmur_hash3_32", T::INT, &[""]), Datum::Int(-965378730));
    assert_eq!(h("murmur_hash3_64", T::BIGINT, &[""]), Datum::Int(0));
    assert_eq!(h("xxhash_32", T::INT, &[""]), Datum::Int(46947589));
    assert_eq!(h("xxhash_64", T::BIGINT, &[""]), Datum::Int(3244421341483603138));
    // a NULL argument is NULL
    assert_eq!(
        eval(call("xxhash_64", ty(T::BIGINT), vec![string("a"), null(vc(65533))])).unwrap(),
        Datum::Null
    );
}
