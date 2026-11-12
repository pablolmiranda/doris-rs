//! URL functions as the C++ BE computes them (`util/url_coding.cpp`, `exprs/function/url/domain.h`):
//! see `docs/superpowers/research/2026-11-10-m6-s7b3-strings.md`. Expected values are the corpus's.
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::ExecError;
use doris_fixtures::build::{call, scalar, string, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

fn eval(name: &str, v: &str) -> Result<Datum, ExecError> {
    let e = call(name, ty(T::STRING), vec![string(v)]);
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    check_read(&e)?;
    let (_, out) = eval_column(&e, &block, &SlotMap::default())?;
    Ok(out.into_iter().next().unwrap())
}

fn text(name: &str, v: &str) -> String {
    match eval(name, v).unwrap() {
        Datum::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn url_encode_escapes_as_cpp() {
    assert_eq!(text("url_encode", "a b~c.d-e*f_g/h"), "a+b%7Ec.d-e*f_g%2Fh");
    assert_eq!(text("url_encode", "中"), "%E4%B8%AD");
    let e = call("url_encode", scalar(T::VARCHAR, 65533, 0, 0), vec![string("x")]);
    check_read(&e).unwrap();
}

#[test]
fn url_decode_quirks() {
    assert_eq!(text("url_decode", "a+b%7Ec%2F"), "a b~c/");
    // istringstream >> hex stops at a non-hex digit but two bytes are consumed
    assert_eq!(eval("url_decode", "%4G!").unwrap(), Datum::Bytes(b"\x04!".to_vec()));
    // leading whitespace is skipped, a sign is read
    assert_eq!(eval("url_decode", "% 4").unwrap(), Datum::Bytes(b"\x04".to_vec()));
    assert_eq!(eval("url_decode", "%-1").unwrap(), Datum::Bytes(vec![0xFF]));
    for bad in ["%GG", "%4", "%"] {
        match eval("url_decode", bad) {
            Err(ExecError::Coded { name, msg, .. }) => assert_eq!((name, msg.as_str()), ("INTERNAL_ERROR", "Decode url failed")),
            other => panic!("{bad}: {other:?}"),
        }
    }
}

#[test]
fn domain_functions_use_the_tld_table() {
    let all = |v: &str| {
        (
            text("top_level_domain", v),
            text("first_significant_subdomain", v),
            text("cut_to_first_significant_subdomain", v),
        )
    };
    let s = |a: &str, b: &str, c: &str| (a.to_string(), b.to_string(), c.to_string());
    assert_eq!(all("www.baidu.com"), s("com", "baidu", "baidu.com"));
    assert_eq!(all("www.google.com.cn"), s("cn", "google", "google.com.cn"));
    assert_eq!(all("http://www.example.com/a/b/c?a=b"), s("com", "example", "example.com"));
    assert_eq!(all("https://news.clickhouse.com.tr/"), s("tr", "clickhouse", "clickhouse.com.tr"));
    for empty in ["invalid url", "", " ", "xxxxxxxx"] {
        assert_eq!(all(empty), s("", "", ""), "{empty:?}");
    }
    // an IPv4 host has no top-level domain; a user part is skipped; a host ending in its dot is no
    // host (`check_and_return_host`), so the trailing-dot branches after it are never reached
    assert_eq!(text("top_level_domain", "http://127.0.0.1/"), "");
    assert_eq!(text("first_significant_subdomain", "http://me@mail.yandex.ru/"), "yandex");
    assert_eq!(text("cut_to_first_significant_subdomain", "www.example.com."), "");
}

#[test]
fn url_decode_0x_prefix_fails() {
    // num_get takes the 0x prefix, then finds no digit
    for bad in ["%0x", "%0X"] {
        assert!(matches!(eval("url_decode", bad), Err(ExecError::Coded { .. })), "{bad}");
    }
}
