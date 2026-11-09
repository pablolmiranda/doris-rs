//! A stream load's rows (`FileScanner` over a `FILE_STREAM`): the body is split into CSV lines or
//! JSON objects, each field becomes a source slot (text, or NULL for `\N` and missing JSON keys),
//! the FE's `expr_of_dest_slot` turns source slots into destination slots (casts), and the
//! fragment's output expressions give the sink's row. In strict mode a field that does not cast
//! filters its row (`column(<name>) value is incorrect while strict mode is true, src value is
//! <value>`); otherwise it becomes NULL.
use std::collections::BTreeMap;

use doris_thrift::gen::exprs::{TExpr, TExprNodeType};
use doris_thrift::gen::plan_nodes::{TFileCompressType, TFileFormatType, TFileScanRangeParams};

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
        Some(TFileFormatType::FORMAT_CSV_PLAIN)
        | Some(TFileFormatType::FORMAT_CSV_GZ)
        | Some(TFileFormatType::FORMAT_JSON)
        | Some(TFileFormatType::FORMAT_PARQUET)
        | Some(TFileFormatType::FORMAT_ORC) => {}
        other => return Err(ExecError::Unsupported { what: format!("stream load format {other:?}") }),
    }
    match params.compress_type {
        None | Some(TFileCompressType::PLAIN) | Some(TFileCompressType::UNKNOWN) | Some(TFileCompressType::GZ) => {}
        Some(other) => return Err(ExecError::Unsupported { what: format!("stream load compression {other:?}") }),
    }
    for e in params.expr_of_dest_slot.iter().flat_map(|m| m.values()) {
        check(e)?;
    }
    for e in f.params.fragment.as_ref().and_then(|fr| fr.output_exprs.as_ref()).into_iter().flatten() {
        check(e)?;
    }
    Ok(())
}

/// A source row: its fields, None for NULL.
struct SourceRow {
    fields: Vec<Option<String>>,
}

/// The body's rows, split as `CsvReader` does: records end at the line delimiter and fields at
/// the column separator, except inside an enclosed field (see `split_records`).
fn csv_rows(params: &TFileScanRangeParams, body: &[u8]) -> Vec<Result<SourceRow, RowError>> {
    let attrs = params.file_attributes.clone().unwrap_or_default();
    let text = attrs.text_params.clone().unwrap_or_default();
    let sep = unescape(text.column_separator.as_deref().unwrap_or("\\t"));
    let delim = unescape(text.line_delimiter.as_deref().unwrap_or("\\n"));
    let enclose = text.enclose.filter(|e| *e != 0).map(|e| e as u8 as char);
    let escape = text.escape.filter(|e| *e != 0).map(|e| e as u8 as char);
    let trim_quotes = attrs.trim_double_quotes.unwrap_or(false);
    let width = params.num_of_columns_from_file.unwrap_or(0).max(0) as usize;
    // CsvReader::init_reader: a header type replaces skip_lines
    let skip = match attrs.header_type.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("csv_with_names") => 1,
        Some("csv_with_names_and_types") => 2,
        Some(t) if !t.is_empty() => 0,
        _ => attrs.skip_lines.unwrap_or(0).max(0) as usize,
    };
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
    // CsvReader::_remove_bom: a UTF-8 BOM before the first line is dropped when that line is
    // read, not skipped
    let body = if skip == 0 { body.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(body) } else { body };
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
        out.push(Ok(SourceRow { fields: fields.into_iter().take(width).collect() }));
    }
    out
}

/// Records of the body, at the byte level: each its raw bytes and its fields (with whether a
/// field was enclosed). The line delimiter is matched before the column separator, which may
/// be its prefix (`|@|` and `|@|\\n`).
/// A record's raw bytes, and its fields with whether each was enclosed.
type Record = (Vec<u8>, Vec<(Vec<u8>, bool)>);

fn split_records(body: &[u8], sep: &[u8], delim: &[u8], enclose: Option<u8>, escape: Option<u8>) -> Vec<Record> {
    // EncloseCsvLineReaderCtx finds the boundaries: a field that starts with the enclose runs to
    // an enclose that is neither escaped nor doubled; anywhere else the separator and the line
    // delimiter are found as they are. The field keeps its text as written.
    let mut records = Vec::new();
    let (mut fields, mut cur, mut raw) = (Vec::new(), Vec::new(), Vec::new());
    let mut in_enclose = false;
    let (mut should_escape, mut quote_escape) = (false, false);
    let mut i = 0;
    while i < body.len() {
        let c = body[i];
        if in_enclose {
            if escape != enclose && Some(c) == escape {
                should_escape = !should_escape;
            } else if should_escape {
                should_escape = false;
            } else if quote_escape {
                quote_escape = false;
                if Some(c) != enclose {
                    // the enclosed part ended: this character is read as outside it
                    in_enclose = false;
                    continue;
                }
            } else if Some(c) == enclose {
                quote_escape = true;
            }
            cur.push(c);
            raw.push(c);
            i += 1;
            continue;
        }
        if !delim.is_empty() && body[i..].starts_with(delim) {
            fields.push(field(std::mem::take(&mut cur), enclose, escape));
            records.push((std::mem::take(&mut raw), std::mem::take(&mut fields)));
            i += delim.len();
            continue;
        }
        if !sep.is_empty() && body[i..].starts_with(sep) {
            fields.push(field(std::mem::take(&mut cur), enclose, escape));
            raw.extend_from_slice(sep);
            i += sep.len();
            continue;
        }
        if cur.is_empty() && Some(c) == enclose {
            in_enclose = true;
            (should_escape, quote_escape) = (false, false);
        }
        cur.push(c);
        raw.push(c);
        i += 1;
    }
    if !raw.is_empty() {
        fields.push(field(cur, enclose, escape));
        records.push((raw, fields));
    }
    records
}

/// A field's value, and whether it was enclosed: BaseCsvTextFieldSplitter::_process_value drops
/// the enclose from a field that starts and ends with it; then, with an escape character,
/// escape_string_for_csv drops each escape and the first of each doubled enclose.
fn field(mut f: Vec<u8>, enclose: Option<u8>, escape: Option<u8>) -> (Vec<u8>, bool) {
    let quoted = enclose.is_some_and(|q| f.len() > 1 && f[0] == q && f[f.len() - 1] == q);
    if quoted {
        f.pop();
        f.remove(0);
    }
    if let Some(e) = escape {
        let q = enclose.unwrap_or(0);
        let mut out = Vec::with_capacity(f.len());
        let mut escape_next = false;
        for (k, &c) in f.iter().enumerate() {
            escape_next = if (c == q && f.get(k + 1) == Some(&q)) || c == e { !escape_next } else { false };
            if !escape_next {
                out.push(c);
            }
        }
        f = out;
    }
    (f, quoted)
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
/// A missing key's text (NewJsonReader::_fill_missing_column): its slot's default, computed
/// when first needed, so a default doris-rs cannot compute refuses only a load that needs it.
fn default_text(e: &TExpr) -> Result<Option<String>, ExecError> {
    check(e)?;
    match eval(e, &Slots::new()) {
        Ok(Value::Null) => Ok(None),
        Ok(Value::Str(b)) => Ok(Some(String::from_utf8_lossy(&b).into_owned())),
        Ok(other) => Err(ExecError::Unsupported { what: format!("a load column default of value {other:?}") }),
        Err(e) => Err(ExecError::Unsupported { what: format!("a load column default: {}", e.msg) }),
    }
}

/// `defaults`: each file slot's default (default_value_of_src_slot), None for no default (an
/// empty expression or the NULL literal).
fn json_rows(
    params: &TFileScanRangeParams,
    names: &[String],
    nullable: &[bool],
    defaults: &[Option<TExpr>],
    body: &[u8],
) -> Result<Vec<Result<SourceRow, RowError>>, ExecError> {
    let attrs = params.file_attributes.clone().unwrap_or_default();
    // `[]` is no jsonpaths: fields are found by column name
    let paths: Option<Vec<String>> =
        attrs.jsonpaths.as_deref().filter(|s| !s.is_empty()).and_then(|s| serde_json::from_str::<Vec<String>>(s).ok()).filter(|p| !p.is_empty());
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
            let missing = |i: usize| found[i].is_none() && defaults.get(i).is_none_or(Option::is_none);
            if let Some(i) = (0..names.len()).find(|i| missing(*i) && !nullable[*i]) {
                out.push(Err(RowError { msg: format!("The column `{}` is not nullable, but it's not found in jsondata.", names[i]), line }));
                continue;
            }
            let mut fields = Vec::with_capacity(names.len());
            for (i, v) in found.into_iter().enumerate() {
                fields.push(match (v, defaults.get(i).and_then(Option::as_ref)) {
                    (Some(v), _) => json_text(v),
                    (None, Some(e)) => default_text(e)?,
                    (None, None) => None,
                });
            }
            out.push(Ok(SourceRow { fields }));
        }
    }
    Ok(out)
}

/// The sink's rows of a stream load body, and how many rows the body held.
/// The body as the reader sees it: gunzipped when the scan is FORMAT_CSV_GZ or compress_type GZ
/// (`NewPlainTextLineReader` over a `GzipDecompressor`, which reads concatenated members).
/// The gunzipped bytes stop at `max_bytes`: a body past it fails rather than filling memory.
fn decompressed<'a>(params: &TFileScanRangeParams, body: &'a [u8], max_bytes: u64) -> Result<std::borrow::Cow<'a, [u8]>, ExecError> {
    let gz = params.format_type == Some(TFileFormatType::FORMAT_CSV_GZ) || params.compress_type == Some(TFileCompressType::GZ);
    if !gz {
        return Ok(std::borrow::Cow::Borrowed(body));
    }
    let mut out = Vec::new();
    let limit = max_bytes.saturating_add(1);
    std::io::Read::read_to_end(&mut std::io::Read::take(flate2::read::MultiGzDecoder::new(body), limit), &mut out)
        .map_err(|e| ExecError::BadRequest(format!("the body is not gzip: {e}")))?;
    if out.len() as u64 > max_bytes {
        return Err(ExecError::ExceededLimit(format!(
            "the uncompressed body exceeds the limit of {max_bytes} bytes set by BE's conf streaming_load_max_mb"
        )));
    }
    Ok(std::borrow::Cow::Owned(out))
}

fn columnar_rows(rows: crate::columnar::TextRows) -> Vec<Result<SourceRow, RowError>> {
    rows.into_iter().map(|fields| Ok(SourceRow { fields })).collect()
}

pub fn stream_rows(f: &WriteFragment, body: &[u8], max_bytes: u64) -> Result<Vec<Result<Vec<Value>, RowError>>, ExecError> {
    let params = scan_params(f)?;
    let body = decompressed(&params, body, max_bytes)?;
    let body = &body[..];
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
        Some(TFileFormatType::FORMAT_JSON) => {
            let defaults: Vec<Option<TExpr>> = file_slots
                .iter()
                .map(|id| {
                    params
                        .default_value_of_src_slot
                        .as_ref()
                        .and_then(|m| m.get(id))
                        .filter(|e| e.nodes.first().is_some_and(|n| n.node_type != TExprNodeType::NULL_LITERAL))
                        .cloned()
                })
                .collect();
            json_rows(&params, &names, &nullable, &defaults, body)?
        }
        Some(TFileFormatType::FORMAT_PARQUET) => columnar_rows(crate::columnar::parquet_rows(body, &names)?),
        Some(TFileFormatType::FORMAT_ORC) => columnar_rows(crate::columnar::orc_rows(body, &names)?),
        _ => csv_rows(&params, body),
    };
    let output = f.params.fragment.as_ref().and_then(|fr| fr.output_exprs.clone()).unwrap_or_default();
    Ok(rows
        .into_iter()
        .map(|row| {
            let row = row?;
            // a column's error prints the source block's row (Block::dump_one_line): the file's
            // fields, a NULL as NULL
            let src_line = row.fields.iter().map(|f| f.as_deref().unwrap_or("NULL")).collect::<Vec<_>>().join(" ");
            let err = |msg: String| RowError { msg, line: src_line.clone() };
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
                    (Some(e), _) => eval(e, &src).map_err(|e| err(e.msg))?,
                    (None, Some(s)) => src.get(s).cloned().unwrap_or(Value::Null),
                    (None, None) => Value::Null,
                };
                // FileScanner::_convert_to_output_block, column by column in tuple order: in
                // strict mode, a column mapped straight from a source column whose non-NULL value
                // came out NULL; else a NULL for a column that is not nullable
                if v == Value::Null {
                    if let Some(Value::Str(b)) = direct.get(&slot.id).and_then(|s| src.get(s)).filter(|_| strict) {
                        return Err(err(format!(
                            "column({}) value is incorrect while strict mode is true, src value is {}",
                            slot.col_name,
                            String::from_utf8_lossy(b)
                        )));
                    }
                    if slot.null_indicator_bit < 0 {
                        return Err(err(format!("column({}) values is null while columns is not nullable", slot.col_name)));
                    }
                }
                dest.insert(slot.id, v);
            }
            if output.is_empty() {
                return Ok(dest_slots.iter().map(|s| dest.get(&s.id).cloned().unwrap_or(Value::Null)).collect());
            }
            output.iter().map(|e| eval(e, &dest).map_err(|e| err(e.msg))).collect()
        })
        .collect())
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

#[cfg(test)]
mod tests {
    use super::split_records;

    fn fields(line: &[u8], escape: u8) -> Vec<String> {
        let records = split_records(line, b",", b"\n", Some(b'"'), Some(escape));
        assert_eq!(records.len(), 1);
        records[0].1.iter().map(|(f, _)| String::from_utf8(f.clone()).unwrap()).collect()
    }

    // enclose_with_escape_doublequote.csv row 5 and its .out row (EncloseCsvLineReaderCtx)
    #[test]
    fn a_doubled_enclose_inside_an_enclosed_field_is_one_enclose() {
        let line = br#"5,"abc""de,fg""h","2023-07-17","""abc""def""","2023-07-22 07:00:00","test ""escape"" in enclose""#;
        assert_eq!(
            fields(line, b'\\'),
            ["5", r#"abc"de,fg"h"#, "2023-07-17", r#""abc"def""#, "2023-07-22 07:00:00", r#"test "escape" in enclose"#]
        );
    }

    fn csv_fields(body: &[u8], skip_lines: i32) -> Vec<Vec<Option<String>>> {
        use doris_thrift::gen::plan_nodes::{TFileAttributes, TFileScanRangeParams, TFileTextScanRangeParams};
        let text = TFileTextScanRangeParams { column_separator: Some(",".into()), enclose: Some(b'"' as i8), escape: Some(b'\\' as i8), ..Default::default() };
        let attrs = TFileAttributes { text_params: Some(text), skip_lines: Some(skip_lines), ..Default::default() };
        let params = TFileScanRangeParams { file_attributes: Some(attrs), num_of_columns_from_file: Some(2), ..Default::default() };
        super::csv_rows(&params, body).into_iter().map(|r| r.map(|r| r.fields).unwrap_or_default()).collect()
    }

    // enclose_with_bom.csv: CsvReader::_remove_bom drops a UTF-8 BOM from the first data line
    #[test]
    fn a_bom_before_the_first_line_is_dropped() {
        let body = b"\xEF\xBB\xBF\"14\",a\n15,b\n";
        assert_eq!(csv_fields(body, 0)[0], [Some("14".to_string()), Some("a".to_string())]);
    }

    // with lines to skip, the BOM goes with the first, skipped, line
    #[test]
    fn a_bom_on_a_skipped_line_is_skipped_with_it() {
        let body = b"\xEF\xBB\xBFk,v\n15,b\n";
        assert_eq!(csv_fields(body, 1), [[Some("15".to_string()), Some("b".to_string())]]);
    }

    // enclose_wrong_position.csv row 6 and its .out row: a field is unquoted only when it starts
    // and ends with the enclose (BaseCsvTextFieldSplitter::_process_value)
    #[test]
    fn an_enclose_that_does_not_wrap_the_field_stays() {
        let line = br#"6,ab"c,2023-07-20,d"ef",2023-07-20 05:48:31,"g"hi"#;
        assert_eq!(fields(line, b'\\'), ["6", r#"ab"c"#, "2023-07-20", r#"d"ef""#, "2023-07-20 05:48:31", r#""g"hi"#]);
    }

    // escape_string_for_csv runs only with an escape character: without one, a doubled enclose
    // stays doubled
    #[test]
    fn without_an_escape_a_doubled_enclose_stays() {
        let records = split_records(br#""a""b",c"#, b",", b"\n", Some(b'"'), None);
        let got: Vec<String> = records[0].1.iter().map(|(f, _)| String::from_utf8(f.clone()).unwrap()).collect();
        assert_eq!(got, [r#"a""b"#, "c"]);
    }

    // outside an enclosed field the separator is found as is (NORMAL state); then, with an
    // escape character, escape_string_for_csv drops it from every field
    #[test]
    fn an_escape_outside_an_enclosed_field_splits_and_is_dropped() {
        assert_eq!(fields(br#"a\,b,c"#, b'\\'), ["a", "b", "c"]);
    }

    // when escape and enclose are the same, C++ uses only the doubled-enclose rule
    #[test]
    fn an_escape_that_is_the_enclose_does_not_take_the_next_character() {
        assert_eq!(fields(br#""a""b","",c"#, b'"'), [r#"a"b"#, "", "c"]);
    }
}
