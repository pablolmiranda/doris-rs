//! A stream load's rows (`FileScanner` over a `FILE_STREAM`): the body is split into CSV lines or
//! JSON objects, each field becomes a source slot (text, or NULL for `\N` and missing JSON keys),
//! the FE's `expr_of_dest_slot` turns source slots into destination slots (casts), and the
//! fragment's output expressions give the sink's row. In strict mode a field that does not cast
//! filters its row (`column(<name>) value is incorrect while strict mode is true, src value is
//! <value>`); otherwise it becomes NULL.
use std::collections::BTreeMap;

use doris_thrift::gen::exprs::TExprNodeType;
use doris_thrift::gen::plan_nodes::{TFileFormatType, TFileScanRangeParams};

use crate::expr::{check, eval, Slots};
use crate::fragment::{tuple_slots, WriteFragment};
use crate::value::Value;
use crate::{ExecError, RowError};

/// The file scan's parameters (the scan range of the plan's one node).
pub fn scan_params(f: &WriteFragment) -> Result<TFileScanRangeParams, ExecError> {
    let p = &f.params;
    let node_id = p.fragment.as_ref().and_then(|fr| fr.plan.as_ref()).and_then(|pl| pl.nodes.first()).map(|n| n.node_id).unwrap_or(0);
    if let Some(params) = p.file_scan_params.as_ref().and_then(|m| m.get(&node_id)) {
        return Ok(params.clone());
    }
    p.local_params
        .iter()
        .flatten()
        .flat_map(|l| l.per_node_scan_ranges.get(&node_id).into_iter().flatten())
        .find_map(|r| r.scan_range.ext_scan_range.as_ref()?.file_scan_range.as_ref()?.params.clone())
        .ok_or_else(|| ExecError::BadRequest("a file scan without its parameters".into()))
}

/// Checks a stream load's plan uses only what doris-rs runs.
pub fn check_stream(f: &WriteFragment) -> Result<(), ExecError> {
    let params = scan_params(f)?;
    let node = &f.params.fragment.as_ref().and_then(|fr| fr.plan.as_ref()).expect("planned").nodes[0];
    if node.conjuncts.as_ref().is_some_and(|c| !c.is_empty()) || node.vconjunct.is_some() {
        return Err(ExecError::Unsupported { what: "a WHERE clause on a stream load".into() });
    }
    match params.format_type {
        Some(TFileFormatType::FORMAT_CSV_PLAIN) | Some(TFileFormatType::FORMAT_JSON) => {}
        other => return Err(ExecError::Unsupported { what: format!("stream load format {other:?}") }),
    }
    for e in params.expr_of_dest_slot.iter().flat_map(|m| m.values()) {
        check(e)?;
    }
    for e in f.params.fragment.as_ref().and_then(|fr| fr.output_exprs.as_ref()).into_iter().flatten() {
        check(e)?;
    }
    Ok(())
}

/// A source row: its fields, and the line the error log prints for it.
struct SourceRow {
    fields: Vec<Option<String>>,
    line: String,
}

/// The body's rows, split as `CsvReader` does: records end at the line delimiter and fields at
/// the column separator, except inside an enclosed field, where the escape character takes the
/// next character as it is.
fn csv_rows(params: &TFileScanRangeParams, body: &[u8]) -> Vec<Result<SourceRow, RowError>> {
    let attrs = params.file_attributes.clone().unwrap_or_default();
    let text = attrs.text_params.clone().unwrap_or_default();
    let sep = unescape(text.column_separator.as_deref().unwrap_or("\\t"));
    let delim = unescape(text.line_delimiter.as_deref().unwrap_or("\\n"));
    let enclose = text.enclose.filter(|e| *e != 0).map(|e| e as u8 as char);
    let escape = text.escape.filter(|e| *e != 0).map(|e| e as u8 as char);
    let trim_quotes = attrs.trim_double_quotes.unwrap_or(false);
    let width = params.num_of_columns_from_file.unwrap_or(0).max(0) as usize;
    let skip = attrs.skip_lines.unwrap_or(0).max(0) as usize
        + usize::from(matches!(attrs.header_type.as_deref(), Some("csv_with_names")))
        + 2 * usize::from(matches!(attrs.header_type.as_deref(), Some("csv_with_names_and_types")));
    let ignore_redundant = attrs.ignore_csv_redundant_col == Some(true);
    let mismatch = |found: usize| {
        let mut m = format!(
            "Column count mismatch: expected {width}, but found {found} (sep:{} delim:{}",
            sep.replace('\t', "\\t"),
            delim.replace('\n', "\\n")
        );
        if let Some(q) = enclose {
            m.push_str(&format!(" encl:{q}"));
        }
        if let Some(e) = escape {
            m.push_str(&format!(" esc:{e}"));
        }
        m.push(')');
        m
    };
    let validate = attrs.enable_text_validate_utf8.unwrap_or(true);
    let empty_as_null = text.empty_field_as_null.unwrap_or(false);
    let mut out = Vec::new();
    let records = split_records(body, sep.as_bytes(), delim.as_bytes(), enclose.map(|c| c as u8), escape.map(|c| c as u8));
    for (raw, fields) in records.into_iter().skip(skip) {
        if raw.is_empty() {
            continue;
        }
        let raw_text = String::from_utf8_lossy(&raw).into_owned();
        if validate && std::str::from_utf8(&raw).is_err() {
            out.push(Err(RowError { msg: "Invalid file encoding: all CSV files must be UTF-8 encoded".into(), line: raw_text }));
            continue;
        }
        let fields: Vec<Option<String>> = fields
            .into_iter()
            .map(|(f, quoted)| {
                let f = String::from_utf8_lossy(&f).into_owned();
                let f = if trim_quotes && f.len() >= 2 && f.starts_with('"') && f.ends_with('"') { f[1..f.len() - 1].to_string() } else { f };
                if !quoted && (f == "\\N" || (empty_as_null && f.is_empty())) { None } else { Some(f) }
            })
            .collect();
        if fields.len() < width || (fields.len() > width && !ignore_redundant) {
            out.push(Err(RowError { msg: mismatch(fields.len()), line: raw_text }));
            continue;
        }
        let shown = fields.iter().take(width).map(|f| f.clone().unwrap_or_else(|| "\\N".into())).collect::<Vec<_>>().join(" ");
        out.push(Ok(SourceRow { fields: fields.into_iter().take(width).collect(), line: shown }));
    }
    out
}

/// Records of the body, at the byte level: each its raw bytes and its fields (with whether a
/// field was enclosed). The line delimiter is matched before the column separator, which may
/// be its prefix (`|@|` and `|@|\\n`).
/// A record's raw bytes, and its fields with whether each was enclosed.
type Record = (Vec<u8>, Vec<(Vec<u8>, bool)>);

fn split_records(body: &[u8], sep: &[u8], delim: &[u8], enclose: Option<u8>, escape: Option<u8>) -> Vec<Record> {
    let mut records = Vec::new();
    let (mut fields, mut cur, mut raw) = (Vec::new(), Vec::new(), Vec::new());
    let (mut quoted, mut was_quoted) = (false, false);
    let mut i = 0;
    while i < body.len() {
        let c = body[i];
        if quoted && Some(c) == escape && i + 1 < body.len() {
            cur.push(body[i + 1]);
            raw.extend_from_slice(&body[i..i + 2]);
            i += 2;
            continue;
        }
        if Some(c) == enclose && (quoted || cur.is_empty()) {
            quoted = !quoted;
            was_quoted = true;
            raw.push(c);
            i += 1;
            continue;
        }
        if !quoted && !delim.is_empty() && body[i..].starts_with(delim) {
            fields.push((std::mem::take(&mut cur), was_quoted));
            was_quoted = false;
            records.push((std::mem::take(&mut raw), std::mem::take(&mut fields)));
            i += delim.len();
            continue;
        }
        if !quoted && !sep.is_empty() && body[i..].starts_with(sep) {
            fields.push((std::mem::take(&mut cur), was_quoted));
            was_quoted = false;
            raw.extend_from_slice(sep);
            i += sep.len();
            continue;
        }
        cur.push(c);
        raw.push(c);
        i += 1;
    }
    if !raw.is_empty() {
        fields.push((cur, was_quoted));
        records.push((raw, fields));
    }
    records
}

/// `\t`, `\n`, `\x01`-style escapes in separators.
fn unescape(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.peek() {
            Some('t') => {
                chars.next();
                out.push('\t');
            }
            Some('n') => {
                chars.next();
                out.push('\n');
            }
            Some('x') => {
                chars.next();
                let hex: String = chars.by_ref().take(2).collect();
                match u8::from_str_radix(&hex, 16) {
                    Ok(b) => out.push(b as char),
                    Err(_) => out.push_str(&format!("\\x{hex}")),
                }
            }
            _ => out.push('\\'),
        }
    }
    out
}

/// A JSON value as the text the scanner gives a string slot.
fn json_text(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// `$.a.b` / `$['a']` over an object.
fn json_path<'a>(v: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let p = path.trim().strip_prefix('$').unwrap_or(path);
    let mut cur = v;
    for part in p.split('.').filter(|s| !s.is_empty()) {
        let key = part.trim_start_matches("['").trim_end_matches("']");
        cur = cur.get(key)?;
    }
    Some(cur)
}

/// JSON objects as source rows (`NewJsonReader`): each file slot's value by its jsonpath or its
/// column name. A missing key is NULL for a nullable slot and filters the row otherwise; an
/// object where no slot matched is filtered.
fn json_rows(params: &TFileScanRangeParams, names: &[String], nullable: &[bool], body: &[u8]) -> Vec<Result<SourceRow, RowError>> {
    let attrs = params.file_attributes.clone().unwrap_or_default();
    let paths: Option<Vec<String>> = attrs.jsonpaths.as_deref().filter(|s| !s.is_empty()).and_then(|s| serde_json::from_str(s).ok());
    let root = attrs.json_root.clone().filter(|s| !s.is_empty());
    let text = String::from_utf8_lossy(body);
    let mut docs: Vec<Result<serde_json::Value, RowError>> = Vec::new();
    let mut push_doc = |src: &str| match serde_json::from_str::<serde_json::Value>(src) {
        Ok(v) => docs.push(Ok(v)),
        Err(e) => docs.push(Err(RowError { msg: json_error(&e), line: src.to_string() })),
    };
    if attrs.read_json_by_line.unwrap_or(false) {
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            push_doc(line);
        }
    } else {
        push_doc(&text);
    }
    let mut out = Vec::new();
    for doc in docs {
        let doc = match doc {
            Ok(d) => d,
            Err(e) => {
                out.push(Err(e));
                continue;
            }
        };
        let doc = match &root {
            Some(r) => json_path(&doc, r).cloned().unwrap_or(serde_json::Value::Null),
            None => doc,
        };
        let objects: Vec<serde_json::Value> = match doc {
            serde_json::Value::Array(a) if attrs.strip_outer_array.unwrap_or(false) => a,
            other => vec![other],
        };
        for obj in objects {
            let line = obj.to_string();
            let found: Vec<Option<&serde_json::Value>> = match &paths {
                Some(p) => (0..names.len()).map(|i| p.get(i).and_then(|path| json_path(&obj, path))).collect(),
                None => names.iter().map(|n| obj.get(n)).collect(),
            };
            if found.iter().all(Option::is_none) {
                let cols: String = names.iter().map(|n| format!("{n}, ")).collect();
                out.push(Err(RowError {
                    msg: format!(
                        "There is no column matching jsonpaths in the json file, columns:[{cols}], please check columns and jsonpaths:{}",
                        attrs.jsonpaths.clone().unwrap_or_default()
                    ),
                    line,
                }));
                continue;
            }
            if let Some(i) = found.iter().zip(nullable).position(|(v, n)| v.is_none() && !n) {
                out.push(Err(RowError { msg: format!("The column `{}` is not nullable, but it's not found in jsondata.", names[i]), line }));
                continue;
            }
            let fields: Vec<Option<String>> = found.into_iter().map(|v| v.and_then(json_text)).collect();
            // a cast error prints the fields, as the CSV reader does
            let line = fields.iter().map(|f| f.clone().unwrap_or_else(|| "NULL".into())).collect::<Vec<_>>().join(" ");
            out.push(Ok(SourceRow { fields, line }));
        }
    }
    out
}

/// The sink's rows of a stream load body, and how many rows the body held.
pub fn stream_rows(f: &WriteFragment, body: &[u8]) -> Result<Vec<Result<Vec<Value>, RowError>>, ExecError> {
    let params = scan_params(f)?;
    let src_tuple = params.src_tuple_id.unwrap_or(1);
    let dest_tuple = params.dest_tuple_id.unwrap_or(0);
    let src_slots = tuple_slots(&f.params, src_tuple);
    let file_slots: Vec<i32> = params
        .required_slots
        .iter()
        .flatten()
        .filter(|s| s.is_file_slot.unwrap_or(true))
        .filter_map(|s| s.slot_id)
        .collect();
    let file_slots = if file_slots.is_empty() { src_slots.iter().map(|s| s.id).collect() } else { file_slots };
    let names: Vec<String> =
        file_slots.iter().map(|id| src_slots.iter().find(|s| s.id == *id).map(|s| s.col_name.clone()).unwrap_or_default()).collect();
    let nullable: Vec<bool> =
        file_slots.iter().map(|id| src_slots.iter().find(|s| s.id == *id).is_none_or(|s| s.null_indicator_bit >= 0)).collect();
    let dest_slots = tuple_slots(&f.params, dest_tuple);
    let exprs = params.expr_of_dest_slot.clone().unwrap_or_default();
    let direct: BTreeMap<i32, i32> = params.dest_sid_to_src_sid_without_trans.clone().unwrap_or_default();
    let strict = params.strict_mode.unwrap_or(false);
    let rows = match params.format_type {
        Some(TFileFormatType::FORMAT_JSON) => json_rows(&params, &names, &nullable, body),
        _ => csv_rows(&params, body),
    };
    let output = f.params.fragment.as_ref().and_then(|fr| fr.output_exprs.clone()).unwrap_or_default();
    Ok(rows
        .into_iter()
        .map(|row| {
            let row = row?;
            let err = |msg: String| RowError { msg, line: row.line.clone() };
            let mut src = Slots::new();
            for (id, field) in file_slots.iter().zip(&row.fields) {
                src.insert(*id, field.clone().map_or(Value::Null, |t| Value::Str(t.into_bytes())));
            }
            for id in &file_slots[row.fields.len().min(file_slots.len())..] {
                src.insert(*id, Value::Null);
            }
            let mut dest = Slots::new();
            for slot in &dest_slots {
                let v = match (exprs.get(&slot.id), direct.get(&slot.id)) {
                    (Some(e), _) => {
                        let v = eval(e, &src).map_err(|e| err(e.msg))?;
                        // strict mode: a non-NULL field that a cast turned into NULL
                        if strict && v == Value::Null && e.nodes.first().is_some_and(|n| n.node_type == TExprNodeType::CAST_EXPR) {
                            let input = eval(&doris_thrift::gen::exprs::TExpr { nodes: e.nodes[1..].to_vec() }, &src).unwrap_or(Value::Null);
                            if let Value::Str(b) = input {
                                return Err(err(format!(
                                    "column({}) value is incorrect while strict mode is true, src value is {}",
                                    slot.col_name,
                                    String::from_utf8_lossy(&b)
                                )));
                            }
                        }
                        v
                    }
                    (None, Some(s)) => src.get(s).cloned().unwrap_or(Value::Null),
                    (None, None) => Value::Null,
                };
                dest.insert(slot.id, v);
            }
            // FileScanner::_convert_to_output_block: a NULL for a slot that is not nullable
            if let Some(slot) = dest_slots.iter().find(|s| s.null_indicator_bit < 0 && dest.get(&s.id) == Some(&Value::Null)) {
                let shown: Vec<String> = dest_slots.iter().map(|s| render_value(dest.get(&s.id).unwrap_or(&Value::Null))).collect();
                return Err(RowError {
                    msg: format!("column({}) values is null while columns is not nullable", slot.col_name),
                    line: shown.join(" "),
                });
            }
            if output.is_empty() {
                return Ok(dest_slots.iter().map(|s| dest.get(&s.id).cloned().unwrap_or(Value::Null)).collect());
            }
            output.iter().map(|e| eval(e, &dest).map_err(|e| err(e.msg))).collect()
        })
        .collect())
}


fn render_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Bool(b) => u8::from(*b).to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Str(b) => String::from_utf8_lossy(b).into_owned(),
        Value::Decimal(s) | Value::Date(s) => s.clone(),
    }
}

/// A parse failure as simdjson reports it through `NewJsonReader` (the truncated document the
/// corpus shows; other syntax errors get simdjson's generic tape error).
fn json_error(e: &serde_json::Error) -> String {
    let (code, info) = if e.is_eof() {
        (28, "INCOMPLETE_ARRAY_OR_OBJECT: JSON document ended early in the middle of an object or array.")
    } else {
        (3, "TAPE_ERROR: The JSON document has an improper structure: missing or superfluous commas, braces, missing keys, etc.")
    };
    format!("Parse json data failed. code: {code}, error info: {info}")
}
