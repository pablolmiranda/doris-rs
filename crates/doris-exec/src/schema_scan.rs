//! SCHEMA_SCAN_NODE for the information_schema tables M5 serves ([agg] B.1): CHARSETS and
//! COLLATIONS (static rows), COLUMNS, SCHEMATA and TABLES (from the FE's metadata RPCs), and
//! SESSION/GLOBAL VARIABLES (`showVariables`). Each scanner builds its rows in its own column
//! list (`information_schema/schema_*_scanner.cpp`); the tuple's slots then take their columns
//! by case-insensitive name (`schema_scan_operator.cpp`). Any other schema table is refused.
use std::collections::BTreeSet;

use doris_segment::writer::Datum;
use doris_thrift::gen::frontend_service::{
    TColumnDesc, TDescribeTablesParams, TDescribeTablesResult, TGetDbsParams, TGetDbsResult, TGetTablesParams, TGetTablesResult,
    TListTableStatusResult, TShowVariableRequest, TShowVariableResult,
};
use doris_thrift::gen::plan_nodes::TSchemaScanNode;
use doris_thrift::gen::types::{TNetworkAddress, TPrimitiveType as T, TVarType};

use crate::block::Block;
use crate::value::ColType;
use crate::ExecError;

/// The FE's metadata calls (FrontendService), made to the FE the scan node names.
pub trait SchemaFe: Sync {
    fn get_db_names(&self, fe: &TNetworkAddress, p: TGetDbsParams) -> Result<TGetDbsResult, String>;
    fn get_table_names(&self, fe: &TNetworkAddress, p: TGetTablesParams) -> Result<TGetTablesResult, String>;
    fn describe_tables(&self, fe: &TNetworkAddress, p: TDescribeTablesParams) -> Result<TDescribeTablesResult, String>;
    fn list_table_status(&self, fe: &TNetworkAddress, p: TGetTablesParams) -> Result<TListTableStatusResult, String>;
    fn show_variables(&self, fe: &TNetworkAddress, p: TShowVariableRequest) -> Result<TShowVariableResult, String>;
}

/// A tuple slot of a schema scan.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaSlot {
    pub name: String,
    pub ty: ColType,
}

/// The schema tables doris-rs scans.
pub const SCHEMA_TABLES: &[&str] = &[
    "character_sets",
    "collations",
    "columns",
    "schemata",
    "tables",
    "session_variables",
    "global_variables",
];

pub fn check_schema_table(name: &str) -> Result<(), ExecError> {
    if SCHEMA_TABLES.contains(&name.to_ascii_lowercase().as_str()) {
        Ok(())
    } else {
        Err(ExecError::Unsupported {
            what: format!("the information_schema table {name}"),
        })
    }
}

const CHARSETS: &[&str] = &["CHARACTER_SET_NAME", "DEFAULT_COLLATE_NAME", "DESCRIPTION", "MAXLEN"];
const COLLATIONS: &[&str] = &["COLLATION_NAME", "CHARACTER_SET_NAME", "ID", "IS_DEFAULT", "IS_COMPILED", "SORTLEN"];
const COLUMNS: &[&str] = &[
    "TABLE_CATALOG",
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "COLUMN_NAME",
    "ORDINAL_POSITION",
    "COLUMN_DEFAULT",
    "IS_NULLABLE",
    "DATA_TYPE",
    "CHARACTER_MAXIMUM_LENGTH",
    "CHARACTER_OCTET_LENGTH",
    "NUMERIC_PRECISION",
    "NUMERIC_SCALE",
    "DATETIME_PRECISION",
    "CHARACTER_SET_NAME",
    "COLLATION_NAME",
    "COLUMN_TYPE",
    "COLUMN_KEY",
    "EXTRA",
    "PRIVILEGES",
    "COLUMN_COMMENT",
    "COLUMN_SIZE",
    "DECIMAL_DIGITS",
    "GENERATION_EXPRESSION",
    "SRS_ID",
];
const SCHEMATA: &[&str] = &[
    "CATALOG_NAME",
    "SCHEMA_NAME",
    "DEFAULT_CHARACTER_SET_NAME",
    "DEFAULT_COLLATION_NAME",
    "SQL_PATH",
    "DEFAULT_ENCRYPTION",
];
const TABLES: &[&str] = &[
    "TABLE_CATALOG",
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "TABLE_TYPE",
    "ENGINE",
    "VERSION",
    "ROW_FORMAT",
    "TABLE_ROWS",
    "AVG_ROW_LENGTH",
    "DATA_LENGTH",
    "MAX_DATA_LENGTH",
    "INDEX_LENGTH",
    "DATA_FREE",
    "AUTO_INCREMENT",
    "CREATE_TIME",
    "UPDATE_TIME",
    "CHECK_TIME",
    "TABLE_COLLATION",
    "CHECKSUM",
    "CREATE_OPTIONS",
    "TABLE_COMMENT",
];
const VARIABLES: &[&str] = &["VARIABLE_NAME", "VARIABLE_VALUE", "DEFAULT_VALUE", "CHANGED"];

fn s(v: &str) -> Datum {
    Datum::Bytes(v.as_bytes().to_vec())
}

fn opt_s(v: &Option<String>) -> Datum {
    v.as_deref().map_or(Datum::Null, s)
}

fn opt_i(v: Option<i64>) -> Datum {
    v.map_or(Datum::Null, |v| Datum::Int(v.into()))
}

fn fe_error(e: String) -> ExecError {
    ExecError::Internal(e)
}

/// `SchemaHelper::extract_db_name`: the name after `cluster:`.
fn db_name(full: &str) -> &str {
    full.split_once(':').map_or(full, |(_, n)| n)
}

struct Scan<'a> {
    node: &'a TSchemaScanNode,
    fe: &'a dyn SchemaFe,
    addr: TNetworkAddress,
}

impl Scan<'_> {
    fn dbs(&self, pattern: Option<String>) -> Result<TGetDbsResult, ExecError> {
        let p = TGetDbsParams::new(
            pattern,
            None,
            None,
            self.node.current_user_ident.clone(),
            self.node.catalog.clone(),
            None,
            None,
        );
        let p = self.user(p);
        self.fe.get_db_names(&self.addr, p).map_err(fe_error)
    }

    /// Without a current user identity, the scanners send user and user_ip.
    fn user(&self, mut p: TGetDbsParams) -> TGetDbsParams {
        if p.current_user_ident.is_none() {
            p.user = self.node.user.clone();
            p.user_ip = self.node.user_ip.clone();
        }
        p
    }

    fn tables_params(&self, db: &str, catalog: Option<String>) -> TGetTablesParams {
        let mut p = TGetTablesParams::new(
            Some(db.to_string()),
            None,
            None,
            None,
            self.node.current_user_ident.clone(),
            None,
            catalog,
            None,
            None,
            None,
        );
        if p.current_user_ident.is_none() {
            p.user = self.node.user.clone();
            p.user_ip = self.node.user_ip.clone();
        }
        p
    }

    fn columns(&self) -> Result<Vec<Vec<Datum>>, ExecError> {
        let dbs = self.dbs(self.node.db.clone())?;
        let names = dbs.dbs.clone().unwrap_or_default();
        let mut rows = Vec::new();
        for (i, db) in names.iter().enumerate() {
            let catalog = dbs.catalogs.as_ref().and_then(|c| c.get(i).cloned());
            let mut tp = self.tables_params(db, catalog.clone());
            tp.pattern = self.node.table.clone();
            let tables = self.fe.get_table_names(&self.addr, tp).map_err(fe_error)?.tables.unwrap_or_default();
            for batch in tables.chunks(100) {
                let mut dp = TDescribeTablesParams::new(
                    Some(db.clone()),
                    batch.to_vec(),
                    None,
                    None,
                    self.node.current_user_ident.clone(),
                    // the corpus's calls carry it (schema_scan_operator: show_hidden_cloumns, default false)
                    Some(self.node.show_hidden_cloumns.unwrap_or(false)),
                    catalog.clone(),
                    None,
                );
                if dp.current_user_ident.is_none() {
                    dp.user = self.node.user.clone();
                    dp.user_ip = self.node.user_ip.clone();
                }
                let d = self.fe.describe_tables(&self.addr, dp).map_err(fe_error)?;
                rows.extend(column_rows(catalog.as_deref(), db_name(db), batch, &d)?);
            }
        }
        Ok(rows)
    }

    fn schemata(&self) -> Result<Vec<Vec<Datum>>, ExecError> {
        let dbs = self.dbs(self.node.wild.clone())?;
        Ok(dbs
            .dbs
            .iter()
            .flatten()
            .enumerate()
            .map(|(i, db)| {
                vec![
                    dbs.catalogs.as_ref().map_or(Datum::Null, |c| c.get(i).map_or(Datum::Null, |c| s(c))),
                    s(db_name(db)),
                    s("utf8mb4"),
                    s("utf8mb4_0900_bin"),
                    Datum::Null,
                    s("NO"),
                ]
            })
            .collect())
    }

    fn tables(&self, required: BTreeSet<String>) -> Result<Vec<Vec<Datum>>, ExecError> {
        let dbs = self.dbs(self.node.db.clone())?;
        let mut rows = Vec::new();
        for (i, db) in dbs.dbs.iter().flatten().enumerate() {
            let catalog = dbs.catalogs.as_ref().and_then(|c| c.get(i).cloned());
            let mut p = self.tables_params(db, catalog.clone());
            p.pattern = self.node.wild.clone();
            p.table = self.node.table.clone();
            p.required_columns = Some(required.clone());
            for t in self.fe.list_table_status(&self.addr, p).map_err(fe_error)?.tables {
                let time = |v: Option<i64>| -> Result<Datum, ExecError> {
                    match v {
                        Some(v) if v > 0 => Err(ExecError::Unsupported {
                            what: "information_schema.tables times".into(),
                        }),
                        _ => Ok(Datum::Null),
                    }
                };
                rows.push(vec![
                    catalog.as_deref().map_or(Datum::Null, s),
                    s(db_name(db)),
                    s(&t.name),
                    s(&t.type_),
                    opt_s(&t.engine),
                    Datum::Null,
                    Datum::Null,
                    opt_i(t.rows),
                    opt_i(t.avg_row_length),
                    opt_i(t.data_length),
                    Datum::Null,
                    opt_i(t.index_length),
                    Datum::Null,
                    Datum::Null,
                    time(t.create_time)?,
                    time(t.update_time)?,
                    time(t.last_check_time)?,
                    opt_s(&t.collation),
                    Datum::Null,
                    Datum::Null,
                    s(&t.comment),
                ]);
            }
        }
        Ok(rows)
    }

    fn variables(&self) -> Result<Vec<Vec<Datum>>, ExecError> {
        let global =
            self.node.db.as_deref() == Some("GLOBAL") || (self.node.db.is_none() && self.node.table_name.eq_ignore_ascii_case("global_variables"));
        let p = TShowVariableRequest::new(
            self.node.thread_id.unwrap_or(0),
            if global { TVarType::GLOBAL } else { TVarType::SESSION },
        );
        let r = self.fe.show_variables(&self.addr, p).map_err(fe_error)?;
        if let Some(row) = r.variables.iter().find(|r| r.len() < 4) {
            return Err(ExecError::Internal(format!(
                "Incomplete variable entry from FE (expected 4 fields, got {}), please retry",
                row.len()
            )));
        }
        Ok(r.variables.iter().map(|v| v[..4].iter().map(|x| s(x)).collect()).collect())
    }
}

/// `_to_mysql_data_type_string`.
fn mysql_type(d: &TColumnDesc) -> &'static str {
    match d.column_type {
        T::BOOLEAN | T::TINYINT => "tinyint",
        T::SMALLINT => "smallint",
        T::INT => "int",
        T::BIGINT => "bigint",
        T::LARGEINT => "bigint unsigned",
        T::FLOAT => "float",
        T::DOUBLE => "double",
        T::VARCHAR | T::STRING => "varchar",
        T::CHAR => "char",
        T::DATE | T::DATEV2 => "date",
        T::DATETIME | T::DATETIMEV2 => "datetime",
        T::TIMESTAMPTZ => "timestamp",
        T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I | T::DECIMAL256 | T::DECIMALV2 => "decimal",
        T::HLL => "hll",
        T::BITMAP => "bitmap",
        T::JSONB => "json",
        T::MAP => "map",
        T::ARRAY => "array",
        T::STRUCT => "struct",
        T::IPV4 => "ipv4",
        T::IPV6 => "ipv6",
        T::VARIANT => "variant",
        _ => "unknown",
    }
}

/// `_type_to_string`.
fn column_type(d: &TColumnDesc) -> String {
    let opt = |v: Option<i32>, none: &str| v.map_or(none.to_string(), |v| v.to_string());
    let children = |sep_all: bool| -> String {
        let kids: Vec<String> = d.children.iter().flatten().map(|c| column_type(c)).collect();
        if sep_all {
            kids.join(",")
        } else {
            kids.first().cloned().unwrap_or_default()
        }
    };
    match d.column_type {
        T::BOOLEAN => "tinyint(1)".into(),
        T::TINYINT => "tinyint(4)".into(),
        T::SMALLINT => "smallint(6)".into(),
        T::INT => "int(11)".into(),
        T::BIGINT => "bigint(20)".into(),
        T::LARGEINT => "largeint".into(),
        T::FLOAT => "float".into(),
        T::DOUBLE => "double".into(),
        T::VARCHAR => format!("varchar({})", opt(d.column_length, "20")),
        T::STRING => "string".into(),
        T::CHAR => format!("char({})", opt(d.column_length, "20")),
        T::DATE | T::DATEV2 => "date".into(),
        T::DATETIME => "datetime".into(),
        T::DECIMALV2 => format!("decimal({}, {})", opt(d.column_precision, "27"), opt(d.column_scale, "9")),
        T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I | T::DECIMAL256 => {
            format!("decimalv3({}, {})", opt(d.column_precision, "UNKNOWN"), opt(d.column_scale, "UNKNOWN"))
        }
        T::DATETIMEV2 => match d.column_scale {
            None | Some(0) => "datetime".into(),
            Some(s) => format!("datetime({s})"),
        },
        T::TIMESTAMPTZ => match d.column_scale {
            None | Some(0) => "timestamp".into(),
            Some(s) => format!("timestamp({s})"),
        },
        T::HLL => "hll".into(),
        T::BITMAP => "bitmap".into(),
        T::JSONB => "json".into(),
        T::MAP => format!("map<{}>", children(true)),
        T::ARRAY => format!("array<{}>", children(false)),
        T::STRUCT => format!("struct<{}>", children(true)),
        T::IPV4 => "ipv4".into(),
        T::IPV6 => "ipv6".into(),
        T::VARIANT => "variant".into(),
        _ => "unknown".into(),
    }
}

/// `SchemaColumnsScanner::_fill_block_impl` for one describeTables batch.
fn column_rows(catalog: Option<&str>, db: &str, tables: &[String], d: &TDescribeTablesResult) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::with_capacity(d.columns.len());
    let mut table = 0;
    let mut ordinal = 1;
    for (i, c) in d.columns.iter().enumerate() {
        while d.tables_offset.get(table).is_some_and(|o| *o as usize <= i) {
            table += 1;
            ordinal = 1;
        }
        let name = tables
            .get(table)
            .ok_or_else(|| ExecError::Internal(format!("describeTables answered column {i} past its {} tables", tables.len())))?;
        let cd = &c.column_desc;
        let t = cd.column_type;
        let text = matches!(t, T::VARCHAR | T::CHAR | T::STRING);
        let not_datetime = |v: Option<i32>| if t != T::DATETIMEV2 { opt_i(v.map(i64::from)) } else { Datum::Null };
        rows.push(vec![
            catalog.map_or(Datum::Null, s),
            s(db),
            s(name),
            s(&cd.column_name),
            Datum::Int(ordinal),
            opt_s(&cd.default_value),
            s(if cd.is_allow_null == Some(true) { "YES" } else { "NO" }),
            s(mysql_type(cd)),
            if text { opt_i(cd.column_length.map(i64::from)) } else { Datum::Null },
            if text {
                opt_i(cd.column_length.map(|l| i64::from(l) * 4))
            } else {
                Datum::Null
            },
            not_datetime(cd.column_precision),
            not_datetime(cd.column_scale),
            if matches!(t, T::DATETIMEV2 | T::TIMESTAMPTZ) {
                opt_i(cd.column_scale.map(i64::from))
            } else {
                Datum::Null
            },
            Datum::Null,
            Datum::Null,
            s(&column_type(cd)),
            s(cd.column_key.as_deref().unwrap_or("")),
            s(""),
            s(""),
            s(c.comment.as_deref().unwrap_or("")),
            opt_i(cd.column_length.map(i64::from)),
            not_datetime(cd.column_scale),
            Datum::Null,
            Datum::Null,
        ]);
        ordinal += 1;
    }
    Ok(rows)
}

/// The scanner's rows, as the slots' columns.
pub fn scan_schema(node: &TSchemaScanNode, slots: &[SchemaSlot], fe: &dyn SchemaFe) -> Result<Block, ExecError> {
    let table = node.table_name.to_ascii_lowercase();
    check_schema_table(&table)?;
    let addr = match (&node.ip, node.port) {
        (Some(ip), Some(port)) if port != 0 => TNetworkAddress::new(ip.clone(), port),
        _ => node
            .fe_addr_list
            .as_ref()
            .and_then(|l| l.first().cloned())
            .ok_or_else(|| ExecError::Internal("IP or port doesn't exists".into()))?,
    };
    let scan = Scan { node, fe, addr };
    let (columns, rows): (&[&str], Vec<Vec<Datum>>) = match table.as_str() {
        "character_sets" => (
            CHARSETS,
            vec![vec![s("utf8mb4"), s("utf8mb4_0900_bin"), s("UTF-8 Unicode"), Datum::Int(4)]],
        ),
        "collations" => (
            COLLATIONS,
            vec![vec![
                s("utf8mb4_0900_bin"),
                s("utf8mb4"),
                Datum::Int(309),
                s("Yes"),
                s("Yes"),
                Datum::Int(1),
            ]],
        ),
        "columns" => (COLUMNS, scan.columns()?),
        "schemata" => (SCHEMATA, scan.schemata()?),
        "tables" => (TABLES, scan.tables(slots.iter().map(|s| s.name.to_ascii_uppercase()).collect())?),
        _ => (VARIABLES, scan.variables()?),
    };
    let mut pick = Vec::with_capacity(slots.len());
    for slot in slots {
        let at = columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(&slot.name))
            .ok_or_else(|| ExecError::BadRequest(format!("the information_schema table {table} has no column {}", slot.name)))?;
        pick.push(at);
    }
    let rows = rows.into_iter().map(|r| pick.iter().map(|i| r[*i].clone()).collect()).collect();
    Ok(Block {
        types: slots.iter().map(|s| s.ty.clone()).collect(),
        rows,
    })
}
