//! Parquet and ORC stream-load bodies as the text the C++ BE casts each value to.
use doris_exec::columnar::{orc_rows, parquet_rows};
use doris_exec::ExecError;

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)).unwrap()
}

fn names() -> Vec<String> {
    [
        "id",
        "decimal_col1",
        "decimal_col2",
        "decimal_col3",
        "decimal_col4",
        "decimal_col5",
        "decimal_col6",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// test_load_with_decimal's rows (the reference's `select * order by id`), each decimal at its
/// file column's scale, as DataTypeDecimal prints it.
fn expected() -> Vec<Vec<Option<String>>> {
    let rows: [[&str; 7]; 5] = [
        ["1", "1.1234", "12.123456", "123.123456789876", "12", "1234.123456789", "123"],
        [
            "2",
            "1234.1234",
            "123456789123.123456",
            "12345678912345678912345678.123456789876",
            "123456789",
            "123456789123456789.123456780",
            "987654321",
        ],
        [
            "3",
            "1234.0000",
            "123456789123.000000",
            "12345678912345678912345678.000000000000",
            "123456789",
            "123456789123456789.000000000",
            "987654321",
        ],
        ["4", "", "", "123.123456789876", "12", "", "123"],
        ["5", "1.1234", "12.123456", "", "", "1234.123456789", ""],
    ];
    rows.iter()
        .map(|r| r.iter().map(|v| (!v.is_empty()).then(|| v.to_string())).collect())
        .collect()
}

#[test]
fn the_decimal_parquet_file_reads_as_cpp_text() {
    assert_eq!(parquet_rows(&fixture("test_decimal.parquet"), &names()).unwrap(), expected());
}

#[test]
fn the_decimal_orc_file_reads_as_cpp_text() {
    // decimal_col3 and decimal_col5 store some values at scale 0: they come out at the column's
    assert_eq!(orc_rows(&fixture("test_decimal.orc"), &names()).unwrap(), expected());
}

/// Names match case-insensitively; a name the file lacks reads as NULL (C++ fills it).
#[test]
fn file_columns_match_by_name() {
    let want = vec!["DECIMAL_COL4".to_string(), "missing".to_string(), "id".to_string()];
    for rows in [
        parquet_rows(&fixture("test_decimal.parquet"), &want).unwrap(),
        orc_rows(&fixture("test_decimal.orc"), &want).unwrap(),
    ] {
        assert_eq!(rows[0], vec![Some("12".to_string()), None, Some("1".to_string())]);
    }
}

#[test]
fn a_parquet_file_with_a_nested_column_is_refused() {
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    let schema = parquet::schema::parser::parse_message_type(
        "message m { required int32 id; optional group tags (LIST) { repeated group list { optional binary element (UTF8); } } }",
    )
    .unwrap();
    let mut buf = Vec::new();
    let w = SerializedFileWriter::new(
        &mut buf,
        std::sync::Arc::new(schema),
        std::sync::Arc::new(WriterProperties::builder().build()),
    )
    .unwrap();
    w.close().unwrap();
    let want = vec!["id".to_string(), "tags".to_string()];
    match parquet_rows(&buf, &want) {
        Err(ExecError::Unsupported { what }) => assert!(what.contains("tags"), "{what}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_body_that_is_not_parquet_or_orc_is_an_error() {
    assert!(parquet_rows(b"not a parquet file", &names()).is_err());
    assert!(orc_rows(b"not an orc file", &names()).is_err());
}

/// A Parquet file of one column and no rows, with the schema given in the message syntax.
fn parquet_schema(message: &str) -> Vec<u8> {
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    let schema = parquet::schema::parser::parse_message_type(message).unwrap();
    let mut buf = Vec::new();
    let w = SerializedFileWriter::new(&mut buf, std::sync::Arc::new(schema), std::sync::Arc::new(WriterProperties::builder().build())).unwrap();
    w.close().unwrap();
    buf
}

fn refused(rows: Result<doris_exec::columnar::TextRows, ExecError>, column: &str) {
    match rows {
        Err(ExecError::Unsupported { what }) => assert!(what.contains(column), "{what}"),
        other => panic!("{column}: {other:?}"),
    }
}

/// A column annotated only with a logical type (no converted type: TIMESTAMP(NANOS), as pandas
/// writes datetimes) would read as its raw integer; it is refused instead.
#[test]
fn parquet_logical_types_the_reader_does_not_render_are_refused() {
    let want = vec!["ts".to_string()];
    refused(parquet_rows(&parquet_schema("message m { optional int64 ts (TIMESTAMP(NANOS,true)); }"), &want), "ts");
    refused(parquet_rows(&parquet_schema("message m { optional int64 ts (TIMESTAMP(MICROS,false)); }"), &want), "ts");
    refused(parquet_rows(&parquet_schema("message m { optional int32 ts (DATE); }"), &want), "ts");
    refused(parquet_rows(&parquet_schema("message m { optional int32 ts (INTEGER(32,false)); }"), &want), "ts");
    // signed integers and decimals read
    assert!(parquet_rows(&parquet_schema("message m { optional int32 ts (INTEGER(16,true)); }"), &want).is_ok());
    assert!(parquet_rows(&parquet_schema("message m { optional int32 ts (DECIMAL(9,2)); }"), &want).is_ok());
}

/// Two file columns whose names differ only in case: which one a load wants is ambiguous.
#[test]
fn columns_equal_but_for_case_are_refused() {
    let file = parquet_schema("message m { optional int32 Id; optional int32 id; }");
    refused(parquet_rows(&file, &["id".to_string()]), "id");
}

/// `OrcReader::_decode_string_*`: an ORC CHAR value loses its padding, as C++ reads it.
#[test]
fn an_orc_char_column_reads_without_its_padding() {
    let want = vec!["col3".to_string(), "col1".to_string(), "col9".to_string()];
    let rows = orc_rows(&fixture("char_column.orc"), &want).unwrap();
    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0], vec![Some("doris".to_string()), Some("0".to_string()), Some("doris".to_string())]);
}

/// Only the wanted columns are read: a column of a type the reader refuses does not stop a load
/// that does not want it (col2 is BOOLEAN, col4 DATE).
#[test]
fn unwanted_columns_are_not_read() {
    let rows = orc_rows(&fixture("char_column.orc"), &["col7".to_string()]).unwrap();
    assert_eq!(rows[9], vec![Some("12345".to_string())]);
}
