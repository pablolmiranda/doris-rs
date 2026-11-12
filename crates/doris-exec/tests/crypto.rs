//! `aes_encrypt`/`aes_decrypt`, `sm4_encrypt`/`sm4_decrypt`, `sm3`/`sm3sum`, `to_base64` and
//! `from_base64` as the C++ BE computes them (`function_encryption.cpp`, `encryption_util.cpp`,
//! `function_string_digest.cpp`): see `docs/superpowers/research/2026-11-12-m6-s7b6-crypto.md`.
//! Expected values are the recorded rows (§5).
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::ExecError;
use doris_fixtures::build::{call, null, scalar, string};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

const K: &str = "F3229A0B371ED2D9441B830D21A390C3";

fn vc() -> TTypeDesc {
    scalar(T::VARCHAR, 65533, 0, 0)
}

fn st() -> TTypeDesc {
    scalar(T::STRING, 2147483643, 0, 0)
}

fn eval(e: TExpr) -> Result<Datum, ExecError> {
    check_read(&e)?;
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    Ok(eval_column(&e, &block, &SlotMap::default())?.1.remove(0))
}

fn text(e: TExpr) -> Option<String> {
    match eval(e).unwrap() {
        Datum::Bytes(b) => Some(String::from_utf8(b).unwrap()),
        Datum::Null => None,
        other => panic!("{other:?}"),
    }
}

fn enc(name: &str, s: TExpr, key: &str, iv: &str, mode: &str) -> TExpr {
    call(name, vc(), vec![s, string(key), string(iv), string(mode)])
}

fn b64(e: TExpr) -> TExpr {
    call("to_base64", st(), vec![e])
}

fn unb64(s: &str) -> TExpr {
    call("from_base64", vc(), vec![string(s)])
}

fn hex(e: TExpr) -> TExpr {
    call("hex", vc(), vec![e])
}

#[test]
fn aes_ecb_and_cbc_as_recorded() {
    let aes = |iv: &str, mode: &str| text(b64(enc("aes_encrypt", string("text"), K, iv, mode)));
    assert_eq!(aes("", "AES_128_ECB").as_deref(), Some("wr2JEDVXzL9+2XtRhgIloA=="));
    // ECB ignores the iv
    assert_eq!(aes("0123456789", "AES_128_ECB").as_deref(), Some("wr2JEDVXzL9+2XtRhgIloA=="));
    assert_eq!(aes("", "AES_256_ECB").as_deref(), Some("BO2vxHeUcw5BQQalSBbo1w=="));
    // CBC: the iv over "DORISDORISDORIS_"
    assert_eq!(aes("", "AES_256_CBC").as_deref(), Some("3dym0E7/+1zbrLIaBVNHSw=="));
    assert_eq!(aes("0123456789", "AES_256_CBC").as_deref(), Some("tsmK1HzbpnEdR2//WhO+MA=="));
    assert_eq!(aes("0123456789ff", "AES_256_CBC").as_deref(), Some("ciacXDLHMNG7CD9Kws8png=="));
    // the mode is matched case-insensitively
    assert_eq!(aes("", "aes_128_ecb").as_deref(), Some("wr2JEDVXzL9+2XtRhgIloA=="));
    let dec = |c: &str, iv: &str, mode: &str| text(enc("aes_decrypt", unb64(c), K, iv, mode));
    assert_eq!(dec("wr2JEDVXzL9+2XtRhgIloA==", "", "AES_128_ECB").as_deref(), Some("text"));
    // a bad final block's padding is NULL
    assert_eq!(dec("mvZT1KJw7N0RJf27aipUpg==", "0123456789", "AES_128_ECB"), None);
    assert_eq!(dec("wr2JEDVXzL9+2XtRhgIloA==", "", "AES_256_ECB"), None);
    assert_eq!(dec("BO2vxHeUcw5BQQalSBbo1w==", "0123456789", "AES_256_ECB").as_deref(), Some("text"));
    assert_eq!(dec("wr2JEDVXzL9+2XtRhgIloA==", "", "AES_256_CBC"), None);
    assert_eq!(dec("tsmK1HzbpnEdR2//WhO+MA==", "0123456789", "AES_256_CBC").as_deref(), Some("text"));
    assert_eq!(dec("ciacXDLHMNG7CD9Kws8png==", "0123456789ff", "AES_256_CBC").as_deref(), Some("text"));
    // the empty string is one padded block; key folding (key_case_sensitive's 'aabbccdd1234')
    assert_eq!(
        text(hex(enc("aes_encrypt", string(""), "securekey456", "", "AES_128_ECB"))).as_deref(),
        Some("1AA197B230C9488519819C2D25AF4CB0")
    );
    assert_eq!(
        text(hex(enc("aes_encrypt", string("text"), "aabbccdd1234", "", "AES_128_ECB"))).as_deref(),
        Some("B1438FAE21B290AD4C639782F3C2FAA6")
    );
}

#[test]
fn aes_gcm_with_aad() {
    let gcm = |name: &str, s: TExpr, key: &str, iv: &str, mode: &str, aad: &str| {
        call(name, vc(), vec![s, string(key), string(iv), string(mode), string(aad)])
    };
    let c = "MTIzNDU2Nzg5MDEyMdXvR41sJqwZ6hnTU8FRTTtXbL8yeChIZA==";
    let e = gcm(
        "aes_encrypt",
        string("Spark SQL"),
        "1234567890abcdef",
        "123456789012",
        "aes_128_gcm",
        "Some AAD",
    );
    assert_eq!(text(b64(e)).as_deref(), Some(c));
    // decrypt reads the iv from the input's first 12 bytes
    let d = gcm("aes_decrypt", unb64(c), "1234567890abcdef", "", "aes_128_gcm", "Some AAD");
    assert_eq!(text(d).as_deref(), Some("Spark SQL"));
    let key = "abcdefghijklmnop12345678ABCDEFGH";
    let aad = "This is an AAD mixed into the input";
    let c = "AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4";
    let e = gcm("aes_encrypt", string("Spark"), key, &"\0".repeat(12), "aes_256_gcm", aad);
    assert_eq!(text(b64(e)).as_deref(), Some(c));
    assert_eq!(text(gcm("aes_decrypt", unb64(c), key, "", "aes_256_gcm", aad)).as_deref(), Some("Spark"));
    // a wrong AAD fails the tag: NULL; input shorter than iv + tag: NULL
    assert_eq!(text(gcm("aes_decrypt", unb64(c), key, "", "aes_256_gcm", "other")), None);
    assert_eq!(text(gcm("aes_decrypt", string("short"), key, "", "aes_256_gcm", aad)), None);
}

#[test]
fn sm4_and_sm3() {
    assert_eq!(
        text(hex(enc("sm4_encrypt", string(""), "securekey456", "", "SM4_128_ECB"))).as_deref(),
        Some("0D56319E329CDA9ABDF5870B9D5ACA57")
    );
    // decrypting to the empty string is '', not NULL
    assert_eq!(
        text(enc("sm4_decrypt", unb64("DVYxnjKc2pq99YcLnVrKVw=="), "securekey456", "", "SM4_128_ECB")).as_deref(),
        Some("")
    );
    let sm3 = |args: Vec<TExpr>| text(call("sm3sum", vc(), args));
    assert_eq!(
        text(call("sm3", vc(), vec![string("abc")])).as_deref(),
        Some("66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0")
    );
    assert_eq!(
        sm3(vec![string("abcd")]).as_deref(),
        Some("82ec580fe6d36ae4f81cae3c73f4a5b3b5a09c943172dc9053c69fd8e18dca1e")
    );
    assert_eq!(
        sm3(vec![string("ab"), string("cd")]).as_deref(),
        Some("82ec580fe6d36ae4f81cae3c73f4a5b3b5a09c943172dc9053c69fd8e18dca1e")
    );
    assert_eq!(sm3(vec![string("ab"), null(vc())]), None);
}

#[test]
fn base64_both_ways() {
    assert_eq!(text(b64(string(""))).as_deref(), Some(""));
    assert_eq!(text(b64(string("hello world"))).as_deref(), Some("aGVsbG8gd29ybGQ="));
    assert_eq!(text(unb64("aGVsbG8gd29ybGQ=")).as_deref(), Some("hello world"));
    assert_eq!(text(unb64("")).as_deref(), Some(""));
    // invalid: outside the alphabet, unpadded, whitespace, data after the padding
    for bad in ["a$==", "aGVsbG8", "aGVs bG8=", "aGVsbG8=aa", "\n"] {
        assert_eq!(text(unb64(bad)), None, "{bad:?}");
    }
    // non-zero trailing bits are accepted
    assert_eq!(text(unb64("aGk=")).as_deref(), Some("hi"));
    assert_eq!(text(unb64("aGl=")).as_deref(), Some("hi"));
}

#[test]
fn nulls_unknown_modes_and_unported_modes() {
    // any NULL argument
    assert_eq!(
        text(call(
            "aes_encrypt",
            vc(),
            vec![string("x"), null(vc()), string(""), string("AES_128_ECB")]
        )),
        None
    );
    // an unknown mode, or an sm4 mode to aes: NULL
    assert_eq!(text(enc("aes_encrypt", string("x"), K, "", "AES_999_XYZ")), None);
    assert_eq!(text(enc("aes_encrypt", string("x"), K, "", "SM4_128_ECB")), None);
    // an empty mode is the default (AES_128_ECB)
    assert_eq!(
        text(b64(enc("aes_encrypt", string("text"), K, "", ""))).as_deref(),
        Some("wr2JEDVXzL9+2XtRhgIloA==")
    );
    // a ciphertext not a multiple of the block: NULL
    assert_eq!(text(enc("aes_decrypt", string("abc"), K, "", "AES_128_ECB")), None);
    // a real mode doris-rs does not port is refused, naming it
    match eval(enc("aes_encrypt", string("x"), K, "", "AES_128_CFB")) {
        Err(ExecError::Unsupported { what }) => assert!(what.contains("AES_128_CFB"), "{what}"),
        other => panic!("{other:?}"),
    }
}

/// test_stream_load_with_aes_encrypt's column mapping `TO_BASE64(AES_ENCRYPT(tmp_data, K))`
/// runs on the load path; the stored values are the recomputed ones (research §5).
#[test]
fn encryption_runs_in_load_column_mappings() {
    use doris_exec::value::Value;
    let e = b64(enc("aes_encrypt", doris_fixtures::build::slot(3, vc()), K, "", "AES_128_ECB"));
    doris_exec::expr::check(&e).unwrap();
    for (plain, stored) in [
        ("hello world", "wcmZBwXgQTCfKZJ6Cg+1tA=="),
        ("doris is great", "2g9bD9lAGe6EWMHwzuEvFQ=="),
        ("test encryption", "i5/NDiJGSkTKyf2QyRKJLg=="),
    ] {
        let slots = [(3, Value::Str(plain.as_bytes().to_vec()))].into_iter().collect();
        assert_eq!(
            doris_exec::expr::eval(&e, &slots).unwrap(),
            Value::Str(stored.as_bytes().to_vec()),
            "{plain}"
        );
    }
    // a mode C++ knows and doris-rs does not port fails the load at check, not row by row
    let cfb = enc("aes_encrypt", doris_fixtures::build::slot(3, vc()), K, "", "AES_128_CFB");
    assert!(doris_exec::expr::check(&cfb).is_err());
    // so does a mode that is not a literal
    let column_mode = call(
        "aes_encrypt",
        vc(),
        vec![
            doris_fixtures::build::slot(3, vc()),
            string(K),
            string(""),
            doris_fixtures::build::slot(4, vc()),
        ],
    );
    assert!(doris_exec::expr::check(&column_mode).is_err());
}
