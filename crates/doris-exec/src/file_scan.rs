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
    let escape = escape.map(|c| c as u8);
    let mut records = split_records(body, sep.as_bytes(), delim.as_bytes(), enclose.map(|c| c as u8), escape);
    // CsvReader::_remove_bom: a UTF-8 BOM is dropped from the first line when that line is read,
    // not skipped, after the line was split with it in place
    if let Some((raw, fields)) = records.first_mut().filter(|_| skip == 0) {
        const BOM: &[u8] = b"\xEF\xBB\xBF";
        if raw.starts_with(BOM) {
            raw.drain(..BOM.len());
            // the field splitter runs after the BOM is gone: its enclose may now wrap the field
            if let Some(f) = fields.first_mut().filter(|f| f.starts_with(BOM)) {
                *f = field(f[BOM.len()..].to_vec(), enclose.map(|c| c as u8));
            }
        }
    }
    // CsvReader::_deserialize_nullable_string, in its order: an empty field (empty_field_as_null),
    // then trim_double_quotes, then the null marker \N unless that trimmed, then the escape
    let cell = |f: Vec<u8>| -> Option<String> {
        if empty_as_null && f.is_empty() {
            return None;
        }
        let trimmed = trim_quotes && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"';
        let f = if trimmed { f[1..f.len() - 1].to_vec() } else { f };
        if !trimmed && f == b"\\N" {
            return None;
        }
        let f = match escape {
            Some(e) => unescape_field(&f, e, enclose.map(|c| c as u8).unwrap_or(0)),
            None => f,
        };
        Some(String::from_utf8_lossy(&f).into_owned())
    };
    for (raw, fields) in records.into_iter().skip(skip) {
        if raw.is_empty() {
            continue;
        }
        let raw_text = String::from_utf8_lossy(&raw).into_owned();
        if validate && std::str::from_utf8(&raw).is_err() {
            out.push(Err(RowError { msg: "Invalid file encoding: all CSV files must be UTF-8 encoded".into(), line: raw_text }));
            continue;
        }
        let fields: Vec<Option<String>> = fields.into_iter().map(cell).collect();
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
/// A record's raw bytes, and its fields as the field splitter gives them.
type Record = (Vec<u8>, Vec<Vec<u8>>);

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
            fields.push(field(std::mem::take(&mut cur), enclose));
            records.push((std::mem::take(&mut raw), std::mem::take(&mut fields)));
            i += delim.len();
            continue;
        }
        if !sep.is_empty() && body[i..].starts_with(sep) {
            fields.push(field(std::mem::take(&mut cur), enclose));
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
        fields.push(field(cur, enclose));
        records.push((raw, fields));
    }
    records
}

/// A field as BaseCsvTextFieldSplitter::_process_value gives it: without the enclose when it
/// starts and ends with it.
fn field(mut f: Vec<u8>, enclose: Option<u8>) -> Vec<u8> {
    if enclose.is_some_and(|q| f.len() > 1 && f[0] == q && f[f.len() - 1] == q) {
        f.pop();
        f.remove(0);
    }
    f
}

/// escape_string_for_csv: each escape, and the first of each doubled enclose, is dropped.
fn unescape_field(f: &[u8], escape: u8, enclose: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(f.len());
    let mut escape_next = false;
    for (k, &c) in f.iter().enumerate() {
        escape_next = if (c == enclose && f.get(k + 1) == Some(&enclose)) || c == escape { !escape_next } else { false };
        if !escape_next {
            out.push(c);
        }
    }
    out
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
        // _get_column_default_value keeps get_data_at(0).to_string(): a NULL's is empty
        Ok(Value::Null) => Ok(Some(String::new())),
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
            let no_match = || {
                let cols: String = names.iter().map(|n| format!("{n}, ")).collect();
                RowError {
                    msg: format!(
                        "There is no column matching jsonpaths in the json file, columns:[{cols}], please check columns and jsonpaths:{}",
                        attrs.jsonpaths.clone().unwrap_or_default()
                    ),
                    line: line.clone(),
                }
            };
            let undefaulted = |i: usize| found[i].is_none() && defaults.get(i).is_none_or(Option::is_none);
            if paths.is_some() {
                // _write_columns_by_jsonpath fills each column in order (_fill_missing_column: a
                // missing not-nullable column with no default is an error), then finds the row
                // matched nothing
                if let Some(i) = (0..names.len()).find(|i| undefaulted(*i) && !nullable[*i]) {
                    out.push(Err(RowError { msg: format!("The column `{}` is not nullable, but it's not found in jsondata.", names[i]), line }));
                    continue;
                }
                if found.iter().all(Option::is_none) {
                    out.push(Err(no_match()));
                    continue;
                }
            } else if found.iter().all(Option::is_none) {
                out.push(Err(no_match()));
                continue;
            }
            let mut fields = Vec::with_capacity(names.len());
            for (i, v) in found.into_iter().enumerate() {
                fields.push(match (v, defaults.get(i).and_then(Option::as_ref)) {
                    (Some(v), _) => json_text(v),
                    (None, Some(e)) => default_text(e)?,
                    // by name, a column with no default takes its type's default (insert_default):
                    // NULL, or the empty string for a not-nullable string slot
                    (None, None) => (!nullable[i]).then(String::new),
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
    use doris_thrift::gen::plan_nodes::{TFileAttributes, TFileScanRangeParams, TFileTextScanRangeParams};

    type Rows = Vec<Result<Vec<Option<String>>, String>>;

    /// csv_rows over `body`: `,`-separated, `width` columns, after `adjust` set the options.
    fn csv(body: &[u8], width: i32, adjust: impl FnOnce(&mut TFileTextScanRangeParams, &mut TFileAttributes)) -> Rows {
        let mut text = TFileTextScanRangeParams { column_separator: Some(",".into()), ..Default::default() };
        let mut attrs = TFileAttributes::default();
        adjust(&mut text, &mut attrs);
        attrs.text_params = Some(text);
        let params = TFileScanRangeParams { file_attributes: Some(attrs), num_of_columns_from_file: Some(width), ..Default::default() };
        super::csv_rows(&params, body).into_iter().map(|r| r.map(|r| r.fields).map_err(|e| e.msg)).collect()
    }

    fn enclosed(escape: u8) -> impl FnOnce(&mut TFileTextScanRangeParams, &mut TFileAttributes) {
        move |t, _| {
            t.enclose = Some(b'"' as i8);
            t.escape = Some(escape as i8);
        }
    }

    fn row(values: &[Option<&str>]) -> Result<Vec<Option<String>>, String> {
        Ok(values.iter().map(|v| v.map(String::from)).collect())
    }

    // enclose_with_escape_doublequote.csv row 5 and its .out row (EncloseCsvLineReaderCtx)
    #[test]
    fn a_doubled_enclose_inside_an_enclosed_field_is_one_enclose() {
        let line = br#"5,"abc""de,fg""h","2023-07-17","""abc""def""","2023-07-22 07:00:00","test ""escape"" in enclose""#;
        assert_eq!(
            csv(line, 6, enclosed(b'\\')),
            [row(&[Some("5"), Some(r#"abc"de,fg"h"#), Some("2023-07-17"), Some(r#""abc"def""#), Some("2023-07-22 07:00:00"), Some(r#"test "escape" in enclose"#)])]
        );
    }

    // when escape and enclose are the same, C++ uses only the doubled-enclose rule
    #[test]
    fn an_escape_that_is_the_enclose_does_not_take_the_next_character() {
        assert_eq!(csv(br#""a""b","",c"#, 3, enclosed(b'"')), [row(&[Some(r#"a"b"#), Some(""), Some("c")])]);
    }

    // enclose_wrong_position.csv row 6 and its .out row: a field is unquoted only when it starts
    // and ends with the enclose (BaseCsvTextFieldSplitter::_process_value)
    #[test]
    fn an_enclose_that_does_not_wrap_the_field_stays() {
        let line = br#"6,ab"c,2023-07-20,d"ef",2023-07-20 05:48:31,"g"hi"#;
        assert_eq!(
            csv(line, 6, enclosed(b'\\')),
            [row(&[Some("6"), Some(r#"ab"c"#), Some("2023-07-20"), Some(r#"d"ef""#), Some("2023-07-20 05:48:31"), Some(r#""g"hi"#)])]
        );
    }

    // escape_string_for_csv runs only with an escape character: without one, a doubled enclose
    // stays doubled
    #[test]
    fn without_an_escape_a_doubled_enclose_stays() {
        assert_eq!(csv(br#""a""b",c"#, 2, |t, _| t.enclose = Some(b'"' as i8)), [row(&[Some(r#"a""b"#), Some("c")])]);
    }

    // outside an enclosed field the separator is found as is (NORMAL state); then, with an
    // escape character, escape_string_for_csv drops it from every field
    #[test]
    fn an_escape_outside_an_enclosed_field_splits_and_is_dropped() {
        assert_eq!(csv(br#"a\,b,c"#, 3, enclosed(b'\\')), [row(&[Some("a"), Some("b"), Some("c")])]);
    }

    // _deserialize_nullable_string compares the field with \N before escape_string_for_csv
    #[test]
    fn the_null_marker_is_read_before_escapes() {
        assert_eq!(csv(b"1,\\N", 2, |t, _| t.escape = Some(b'\\' as i8)), [row(&[Some("1"), None])]);
        assert_eq!(csv(b"1,\\N", 2, enclosed(b'\\')), [row(&[Some("1"), None])]);
    }

    // the splitter drops the enclose before the null and empty checks: nothing remembers it
    #[test]
    fn an_enclosed_null_marker_or_empty_field_is_null() {
        assert_eq!(csv(br#""\N",x"#, 2, |t, _| t.enclose = Some(b'"' as i8)), [row(&[None, Some("x")])]);
        let empty_as_null = |t: &mut TFileTextScanRangeParams, _: &mut TFileAttributes| {
            t.enclose = Some(b'"' as i8);
            t.empty_field_as_null = Some(true);
        };
        assert_eq!(csv(br#""",x"#, 2, empty_as_null), [row(&[None, Some("x")])]);
    }

    // trim_double_quotes: the empty check sees the field untrimmed, a trimmed field skips the
    // null check, and the escape runs last
    #[test]
    fn trim_double_quotes_comes_between_the_empty_and_null_checks() {
        let trim = |a: &mut TFileAttributes| a.trim_double_quotes = Some(true);
        assert_eq!(csv(br#""\N",x"#, 2, |_, a| trim(a)), [row(&[Some(r"\N"), Some("x")])]);
        let trim_empty = |t: &mut TFileTextScanRangeParams, a: &mut TFileAttributes| {
            t.empty_field_as_null = Some(true);
            trim(a);
        };
        assert_eq!(csv(br#""",x"#, 2, trim_empty), [row(&[Some(""), Some("x")])]);
        let trim_escape = |t: &mut TFileTextScanRangeParams, a: &mut TFileAttributes| {
            t.escape = Some(b'\\' as i8);
            trim(a);
        };
        assert_eq!(csv(br#"\"a",x"#, 2, trim_escape), [row(&[Some(r#""a""#), Some("x")])]);
    }

    // enclose_with_bom.csv: CsvReader::_remove_bom drops a UTF-8 BOM from the first data line
    #[test]
    fn a_bom_before_the_first_line_is_dropped() {
        assert_eq!(csv(b"\xEF\xBB\xBF\"14\",a\n15,b\n", 2, enclosed(b'\\'))[0], row(&[Some("14"), Some("a")]));
    }

    // the line reader splits with the BOM in place (the load's EncloseCsvLineReaderCtx does not
    // skip it), so an enclose right after it does not open an enclosed field
    #[test]
    fn a_bom_is_split_with_the_line_before_it_is_dropped() {
        let rows = csv(b"\xEF\xBB\xBF\"a,b\",c\n", 2, enclosed(b'\\'));
        assert!(rows[0].as_ref().is_err_and(|e| e.starts_with("Column count mismatch: expected 2, but found 3")), "{rows:?}");
    }

    // with lines to skip, the BOM goes with the first, skipped, line
    #[test]
    fn a_bom_on_a_skipped_line_is_skipped_with_it() {
        assert_eq!(csv(b"\xEF\xBB\xBFk,v\n15,b\n", 2, |_, a| a.skip_lines = Some(1)), [row(&[Some("15"), Some("b")])]);
    }

    /// json_rows over `body` for slots `names` (nullable or not), no defaults, with `jsonpaths`.
    fn json(body: &[u8], names: &[&str], nullable: &[bool], jsonpaths: Option<&str>) -> Rows {
        let attrs = TFileAttributes { jsonpaths: jsonpaths.map(String::from), ..Default::default() };
        let params = TFileScanRangeParams { file_attributes: Some(attrs), ..Default::default() };
        let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        let defaults = vec![None; names.len()];
        super::json_rows(&params, &names, nullable, &defaults, body).unwrap().into_iter().map(|r| r.map(|r| r.fields).map_err(|e| e.msg)).collect()
    }

    // NewJsonReader by column name: a missing key with no default takes the column's default
    // value (insert_default), an empty string for a not-nullable string slot, never an error
    #[test]
    fn a_missing_key_by_name_without_a_default_is_the_column_default() {
        assert_eq!(json(br#"{"a":"1"}"#, &["a", "k"], &[true, false], None), [row(&[Some("1"), Some("")])]);
    }

    // with jsonpaths, each column is filled in order (_fill_missing_column) before the row is
    // found to match nothing
    #[test]
    fn with_jsonpaths_a_missing_not_nullable_column_is_reported_first() {
        assert_eq!(
            json(br#"{"z":1}"#, &["a", "k"], &[true, false], Some(r#"["$.x","$.y"]"#)),
            [Err("The column `k` is not nullable, but it's not found in jsondata.".to_string())]
        );
    }

    // CsvReader::init_reader: a header type, in any case, replaces skip_lines
    #[test]
    fn a_header_type_replaces_skip_lines() {
        let header = |_: &mut TFileTextScanRangeParams, a: &mut TFileAttributes| {
            a.header_type = Some("CSV_WITH_NAMES".into());
            a.skip_lines = Some(3);
        };
        assert_eq!(csv(b"k,v\n1,a\n2,b\n", 2, header), [row(&[Some("1"), Some("a")]), row(&[Some("2"), Some("b")])]);
    }
}
