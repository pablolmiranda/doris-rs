//! information_schema scans: rows built from the FE's metadata RPCs, as the C++ scanners build them.
use std::sync::Mutex;

use doris_exec::schema_scan::{scan_schema, SchemaFe, SchemaSlot};
use doris_exec::value::ColType;
use doris_segment::writer::Datum;
use doris_thrift::gen::frontend_service::{
    TColumnDef, TColumnDesc, TDescribeTablesParams, TDescribeTablesResult, TGetDbsParams, TGetDbsResult, TGetTablesParams, TGetTablesResult,
    TListTableStatusResult, TShowVariableRequest, TShowVariableResult,
};
use doris_thrift::gen::plan_nodes::TSchemaScanNode;
use doris_thrift::gen::types::{TNetworkAddress, TPrimitiveType as T};

#[derive(Default)]
struct Fe {
    tables: Vec<String>,
    variables: Vec<Vec<String>>,
    describes: Mutex<Vec<Vec<String>>>,
}

impl SchemaFe for Fe {
    fn get_db_names(&self, _fe: &TNetworkAddress, _p: TGetDbsParams) -> Result<TGetDbsResult, String> {
        Ok(TGetDbsResult::new(vec!["test_db".to_string()], vec!["internal".to_string()], None, None))
    }
    fn get_table_names(&self, _fe: &TNetworkAddress, _p: TGetTablesParams) -> Result<TGetTablesResult, String> {
        Ok(TGetTablesResult::new(self.tables.clone()))
    }
    fn describe_tables(&self, _fe: &TNetworkAddress, p: TDescribeTablesParams) -> Result<TDescribeTablesResult, String> {
        self.describes.lock().unwrap().push(p.tables_name.clone());
        let col = |name: &str, t: T, len: i32, prec: Option<i32>, nullable: bool| {
            TColumnDef::new(
                TColumnDesc::new(
                    name.to_string(),
                    t,
                    Some(len),
                    prec,
                    prec.map(|_| 0),
                    Some(nullable),
                    Some("DUP".to_string()),
                    None,
                    None,
                ),
                Some(String::new()),
            )
        };
        let mut columns = Vec::new();
        let mut offsets = Vec::new();
        for _ in &p.tables_name {
            columns.push(col("aaa", T::VARCHAR, 170, None, false));
            columns.push(col("bbb", T::VARCHAR, 20, None, true));
            columns.push(col("ccc", T::INT, 10, Some(10), true));
            columns.push(col("ddd", T::SMALLINT, 5, Some(5), true));
            offsets.push(columns.len() as i32);
        }
        Ok(TDescribeTablesResult::new(offsets, columns))
    }
    fn list_table_status(&self, _fe: &TNetworkAddress, _p: TGetTablesParams) -> Result<TListTableStatusResult, String> {
        Ok(TListTableStatusResult::new(vec![]))
    }
    fn show_variables(&self, _fe: &TNetworkAddress, _p: TShowVariableRequest) -> Result<TShowVariableResult, String> {
        Ok(TShowVariableResult::new(self.variables.clone()))
    }
}

fn node(table: &str, db: Option<&str>) -> TSchemaScanNode {
    TSchemaScanNode::new(
        0,
        table.to_string(),
        db.map(str::to_string),
        None,
        None,
        None,
        Some("127.0.0.1".to_string()),
        Some(9020),
        Some(1855),
        None,
        None,
        None,
        Some("internal".to_string()),
        None,
        None,
    )
}

fn slot(name: &str, ptype: T) -> SchemaSlot {
    SchemaSlot {
        name: name.to_string(),
        ty: ColType {
            name: name.to_string(),
            ptype,
            len: -1,
            precision: 0,
            scale: 0,
            nullable: true,
        },
    }
}

fn text(rows: Vec<Vec<Datum>>) -> Vec<String> {
    rows.into_iter()
        .map(|r| {
            r.into_iter()
                .map(|d| match d {
                    Datum::Null => "\\N".to_string(),
                    Datum::Int(v) => v.to_string(),
                    Datum::Bytes(b) => String::from_utf8(b).unwrap(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

#[test]
fn columns_rows_from_describe_tables() {
    let fe = Fe {
        tables: vec!["t1".into()],
        ..Default::default()
    };
    // slots in any case, mapped by name; the order is the tuple's
    let slots = [
        slot("TABLE_CATALOG", T::VARCHAR),
        slot("column_name", T::VARCHAR),
        slot("ORDINAL_POSITION", T::BIGINT),
        slot("DATA_TYPE", T::VARCHAR),
        slot("COLUMN_TYPE", T::VARCHAR),
        slot("COLUMN_SIZE", T::BIGINT),
    ];
    let b = scan_schema(&node("columns", None), &slots, &fe).unwrap();
    assert_eq!(
        text(b.rows),
        vec![
            "internal aaa 1 varchar varchar(170) 170",
            "internal bbb 2 varchar varchar(20) 20",
            "internal ccc 3 int int(11) 10",
            "internal ddd 4 smallint smallint(6) 5"
        ]
    );
}

#[test]
fn describe_tables_batches_of_100() {
    let fe = Fe {
        tables: (0..250).map(|i| format!("t{i}")).collect(),
        ..Default::default()
    };
    let b = scan_schema(
        &node("columns", None),
        &[slot("TABLE_NAME", T::VARCHAR), slot("ORDINAL_POSITION", T::BIGINT)],
        &fe,
    )
    .unwrap();
    let sizes: Vec<usize> = fe.describes.lock().unwrap().iter().map(|t| t.len()).collect();
    assert_eq!(sizes, vec![100, 100, 50]);
    assert_eq!(b.rows.len(), 1000);
    // each row's table, the ordinal restarting per table
    assert_eq!(text(b.rows[996..].to_vec()), vec!["t249 1", "t249 2", "t249 3", "t249 4"]);
}

#[test]
fn static_tables_and_variables() {
    let fe = Fe {
        variables: vec![vec!["wait_timeout".into(), "30000".into(), "28800".into(), "1".into()]],
        ..Default::default()
    };
    let b = scan_schema(
        &node("character_sets", None),
        &[slot("MAXLEN", T::BIGINT), slot("CHARACTER_SET_NAME", T::VARCHAR)],
        &fe,
    )
    .unwrap();
    assert_eq!(text(b.rows), vec!["4 utf8mb4"]);
    let b = scan_schema(&node("collations", None), &[slot("ID", T::BIGINT)], &fe).unwrap();
    assert_eq!(text(b.rows), vec!["309"]);
    let b = scan_schema(
        &node("session_variables", Some("SESSION")),
        &[slot("VARIABLE_NAME", T::VARCHAR), slot("VARIABLE_VALUE", T::VARCHAR)],
        &fe,
    )
    .unwrap();
    assert_eq!(text(b.rows), vec!["wait_timeout 30000"]);
    let b = scan_schema(
        &node("schemata", None),
        &[
            slot("CATALOG_NAME", T::VARCHAR),
            slot("SCHEMA_NAME", T::VARCHAR),
            slot("SQL_PATH", T::VARCHAR),
        ],
        &fe,
    )
    .unwrap();
    assert_eq!(text(b.rows), vec!["internal test_db \\N"]);
}

#[test]
fn variables_need_four_fields() {
    let fe = Fe {
        variables: vec![vec!["wait_timeout".into(), "30000".into(), "28800".into()]],
        ..Default::default()
    };
    let err = scan_schema(&node("session_variables", Some("SESSION")), &[slot("VARIABLE_NAME", T::VARCHAR)], &fe).expect_err("3 fields");
    assert!(err.to_string().contains("expected 4 fields, got 3"), "{err}");
}

#[test]
fn unknown_schema_table_is_refused() {
    let err = scan_schema(&node("files", None), &[slot("FILE_ID", T::BIGINT)], &Fe::default())
        .expect_err("files is not supported")
        .to_string();
    assert!(err.contains("files"), "{err}");
    // a slot the scanner does not have is an error too
    let err = scan_schema(&node("collations", None), &[slot("NO_SUCH", T::BIGINT)], &Fe::default())
        .expect_err("unknown column")
        .to_string();
    assert!(err.contains("NO_SUCH"), "{err}");
}
