//! The segment writer: what it writes reads back, rewriting the corpus's rows gives the
//! reference's values, and its short keys and key bounds are the reference's bytes.
mod common;

use std::collections::BTreeMap;

use common::{assert_rows_match, cells, Cell};
use doris_segment::column::read_column;
use doris_segment::corpus::Manifest;
use doris_segment::writer::{datums, field_type_of, sort_rows, write_segment, ColumnSpec, Datum, SegmentSchema};
use doris_segment::{read_short_keys, SegmentFile};

fn col(unique_id: u32, ty: &str, length: u32, nullable: bool, is_key: bool) -> ColumnSpec {
    ColumnSpec {
        unique_id,
        field_type: field_type_of(ty).unwrap(),
        length,
        precision: 0,
        frac: 0,
        nullable,
        is_key,
        index_length: length.min(if ty == "VARCHAR" || ty == "STRING" { 20 } else { u32::MAX }),
    }
}

fn dec(unique_id: u32, ty: &str, length: u32, precision: u32, frac: u32) -> ColumnSpec {
    ColumnSpec { precision, frac, ..col(unique_id, ty, length, true, false) }
}

/// Every M4 type: keys k INT NOT NULL and c CHAR(8) (two short key columns), then values.
fn every_type_schema() -> SegmentSchema {
    SegmentSchema {
        columns: vec![
            col(0, "INT", 4, false, true),
            col(1, "CHAR", 8, true, true),
            col(2, "BOOLEAN", 1, true, false),
            col(3, "TINYINT", 1, true, false),
            col(4, "SMALLINT", 2, true, false),
            col(5, "BIGINT", 8, true, false),
            col(6, "LARGEINT", 16, true, false),
            col(7, "FLOAT", 4, true, false),
            col(8, "DOUBLE", 8, true, false),
            dec(9, "DECIMAL32", 4, 9, 2),
            dec(10, "DECIMAL64", 8, 18, 4),
            dec(11, "DECIMAL128I", 16, 38, 10),
            col(12, "DATEV2", 4, true, false),
            dec(13, "DATETIMEV2", 8, 0, 3),
            col(14, "VARCHAR", 36, true, false),
            col(15, "STRING", 2147483643, true, false),
            col(16, "INT", 4, true, false), // all NULL
        ],
        num_short_key_columns: 2,
        num_rows_per_block: 1024,
    }
}

fn every_type_row(i: i64) -> Vec<Datum> {
    let null_or = |every: i64, d: Datum| if i % every == 0 { Datum::Null } else { d };
    let date = (2000 + (i % 30) as u32) << 9 | ((1 + i % 12) as u32) << 5 | (1 + i % 28) as u32;
    let datetime = (u64::from(date) << 37) | ((i % 24) as u64) << 32 | ((i % 60) as u64) << 26 | ((i % 60) as u64) << 20 | (i as u64 * 1000 % 1_000_000);
    vec![
        Datum::Int(i128::from(i % 700) - 350),
        null_or(7, Datum::Bytes(format!("c{}", i % 13).into_bytes())),
        null_or(5, Datum::Bool(i % 2 == 0)),
        null_or(6, Datum::Int(i128::from(i % 256) - 128)),
        null_or(8, Datum::Int(i128::from(i) - 1000)),
        null_or(9, Datum::Int(i128::from(i) * 1_000_000_007 - 5)),
        null_or(10, Datum::Int(i128::from(i) * -170_141_183_460_469_231_731_687_303_715_884)),
        null_or(11, Datum::Float(i as f32 * 0.25 - 3.0)),
        null_or(12, Datum::Double(i as f64 / 7.0)),
        null_or(13, Datum::Int(i128::from(i) * 101 - 99_999)),
        null_or(14, Datum::Int(i128::from(i) * -12_345_678)),
        null_or(15, Datum::Int(i128::from(i) * 10_i128.pow(20) + 7)),
        null_or(16, Datum::Int(i128::from(date))),
        null_or(17, Datum::Int(i128::from(datetime))),
        null_or(18, Datum::Bytes(if i % 4 == 0 { vec![] } else { format!("v{i}").into_bytes() })),
        null_or(19, Datum::Bytes(format!("{i:05} {}", "s".repeat(100)).into_bytes())),
        Datum::Null,
    ]
}

/// CHAR is stored padded to its length with zero bytes; the reader strips the padding.
fn stored(spec: &ColumnSpec, d: &Datum) -> Datum {
    match d {
        Datum::Bytes(b) if spec.field_type == field_type_of("CHAR").unwrap() => {
            Datum::Bytes(b.iter().copied().take_while(|c| *c != 0).collect())
        }
        other => other.clone(),
    }
}

#[test]
fn round_trips_every_type() {
    let schema = every_type_schema();
    let mut rows: Vec<Vec<Datum>> = (0..2500).map(every_type_row).collect();
    sort_rows(&schema, &mut rows);
    let seg = write_segment(&schema, &rows).unwrap();
    assert_eq!(seg.num_rows, 2500);
    let file = SegmentFile::from_bytes(seg.bytes).unwrap();
    for (c, spec) in schema.columns.iter().enumerate() {
        let got = datums(&read_column(&file, spec.unique_id).unwrap_or_else(|e| panic!("column {c}: {e}")));
        let want: Vec<Datum> = rows.iter().map(|r| stored(spec, &r[c])).collect();
        assert_eq!(got, want, "column {c}");
    }
    let sk = read_short_keys(&file).unwrap();
    assert_eq!(sk.num_items, 3);
    // the string column spans several pages
    let strings = file.footer.columns.iter().find(|m| m.unique_id == Some(15)).unwrap();
    assert!(doris_segment::ordinal::data_pages(&file, strings).unwrap().len() > 1);
}

#[test]
fn ties_keep_descending_input_order() {
    let schema = SegmentSchema {
        columns: vec![col(0, "INT", 4, true, true), col(1, "VARCHAR", 8, true, false)],
        num_short_key_columns: 1,
        num_rows_per_block: 1024,
    };
    let b = |s: &str| Datum::Bytes(s.as_bytes().to_vec());
    let mut rows = vec![vec![Datum::Int(1), b("a")], vec![Datum::Int(1), b("b")], vec![Datum::Int(0), b("c")], vec![Datum::Null, b("n")]];
    sort_rows(&schema, &mut rows);
    // MemTable::_sort: NULLs first, then by key; equal keys, later input first
    assert_eq!(
        rows,
        vec![vec![Datum::Null, b("n")], vec![Datum::Int(0), b("c")], vec![Datum::Int(1), b("b")], vec![Datum::Int(1), b("a")]]
    );
}

/// The corpus's `tablet_meta.json` (bytes fields are raw, so read as latin-1).
fn tablet_meta(table: &str) -> serde_json::Value {
    let path = Manifest::corpus_dir().join("data").join(table).join("tablet_meta.json");
    let text: String = std::fs::read(&path).unwrap().into_iter().map(char::from).collect();
    serde_json::from_str(&text).unwrap()
}

fn latin1(v: &serde_json::Value) -> Vec<u8> {
    v.as_str().unwrap().chars().map(|c| c as u8).collect()
}

/// The table's columns as the writer sees them, M4 types only.
fn corpus_schema(meta: &serde_json::Value) -> SegmentSchema {
    let s = &meta["schema"];
    let columns = s["column"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| field_type_of(c["type"].as_str().unwrap()).is_some())
        .map(|c| ColumnSpec {
            unique_id: c["unique_id"].as_u64().unwrap() as u32,
            field_type: field_type_of(c["type"].as_str().unwrap()).unwrap(),
            length: c["length"].as_u64().unwrap() as u32,
            precision: c["precision"].as_u64().unwrap_or(0) as u32,
            frac: c["frac"].as_u64().unwrap_or(0) as u32,
            nullable: c["is_nullable"].as_bool().unwrap(),
            is_key: c["is_key"].as_bool().unwrap(),
            index_length: c["index_length"].as_u64().unwrap() as u32,
        })
        .collect();
    SegmentSchema {
        columns,
        num_short_key_columns: s["num_short_key_columns"].as_u64().unwrap() as u32,
        num_rows_per_block: s["num_rows_per_row_block"].as_u64().unwrap() as u32,
    }
}

/// Each corpus segment of a table: its file and its rows (M4 columns) as the writer takes them.
fn corpus_segments(m: &Manifest, table: &str, schema: &SegmentSchema) -> Vec<(String, SegmentFile, Vec<Vec<Datum>>)> {
    m.tables[table]
        .files
        .iter()
        .map(|f| {
            let seg = SegmentFile::open(&m.resolve(&f.path)).unwrap();
            let cols: Vec<Vec<Datum>> = schema.columns.iter().map(|c| datums(&read_column(&seg, c.unique_id).unwrap())).collect();
            let n = seg.footer.num_rows.unwrap() as usize;
            let rows = (0..n).map(|i| cols.iter().map(|c| c[i].clone()).collect()).collect();
            (f.path.file_name().unwrap().to_string_lossy().to_string(), seg, rows)
        })
        .collect()
}

#[test]
fn corpus_rows_rewrite_equal() {
    let m = Manifest::load();
    let mut tables = 0;
    for table in m.tables.keys() {
        let meta = tablet_meta(table);
        let schema = corpus_schema(&meta);
        let mut rows: Vec<Vec<Cell>> = Vec::new();
        for (name, _, seg_rows) in corpus_segments(&m, table, &schema) {
            let w = write_segment(&schema, &seg_rows).unwrap_or_else(|e| panic!("{table}/{name}: {e}"));
            let file = SegmentFile::from_bytes(w.bytes).unwrap();
            let cols: Vec<Vec<Cell>> = schema.columns.iter().map(|c| cells(&read_column(&file, c.unique_id).unwrap())).collect();
            rows.extend((0..seg_rows.len()).map(|i| cols.iter().map(|c| c[i].clone()).collect::<Vec<_>>()));
        }
        rows.sort_by_key(|r| match &r[0] {
            Cell::Text(s) => s.parse::<i64>().unwrap(),
            other => panic!("{other:?}"),
        });
        let names: BTreeMap<u32, String> = meta["schema"]["column"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| (c["unique_id"].as_u64().unwrap() as u32, c["name"].as_str().unwrap().to_string()))
            .collect();
        let columns: Vec<(u32, &str)> = schema.columns.iter().map(|c| (c.unique_id, names[&c.unique_id].as_str())).collect();
        assert_rows_match(table, &columns, &rows);
        tables += 1;
    }
    assert!(tables >= 7, "{tables}");
}

#[test]
fn short_keys_match_the_reference() {
    let m = Manifest::load();
    let mut checked = 0;
    for table in m.tables.keys() {
        let meta = tablet_meta(table);
        let schema = corpus_schema(&meta);
        let segments = corpus_segments(&m, table, &schema);
        for rs in meta["rs_metas"].as_array().unwrap() {
            if rs["num_segments"].as_u64().unwrap_or(0) == 0 {
                continue;
            }
            let id = rs["rowset_id_v2"].as_str().unwrap();
            let mine: Vec<_> = segments.iter().filter(|(n, _, _)| n.starts_with(id)).collect();
            assert_eq!(mine.len() as u64, rs["num_segments"].as_u64().unwrap(), "{table} {id}");
            let mut written = Vec::new();
            for (name, reference, rows) in &mine {
                let w = write_segment(&schema, rows).unwrap();
                let file = SegmentFile::from_bytes(w.bytes.clone()).unwrap();
                assert_eq!(read_short_keys(&file).unwrap().key_bytes, read_short_keys(reference).unwrap().key_bytes, "{table}/{name}");
                written.push(w);
                checked += 1;
            }
            // segments_key_bounds, aggregated over the rowset: the first segment's min key, the
            // last one's max key (segments are in key order, NONOVERLAPPING)
            let bounds = &rs["segments_key_bounds"][0];
            assert_eq!(written.first().unwrap().min_key, latin1(&bounds["min_key"]), "{table} {id} min");
            assert_eq!(written.last().unwrap().max_key, latin1(&bounds["max_key"]), "{table} {id} max");
        }
    }
    assert!(checked >= 10, "{checked}");
}
