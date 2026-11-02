//! Helpers shared by the corpus integration tests: the expected rows the reference captured, the
//! reference's MySQL text rendering of decoded values, and row-wise comparison.
//!
//! Rendering lives here, not in the library: it is the MySQL text protocol's job, which a later
//! crate will own. Every rule below is taken from the 4.1.5 BE source and checked against the
//! corpus; rules the corpus does not exercise are marked as such.
#![allow(dead_code)] // each test binary uses a different subset

use doris_segment::column::read_column;
use doris_segment::corpus::Manifest;
use doris_segment::values::{ColumnValues, Date, DateTime, Decimal12};
use doris_segment::SegmentFile;

/// Rows of `expected/<table>.tsv`: tab-separated, NULL as the word NULL, sorted by `k`.
pub fn expected_rows(table: &str) -> Vec<Vec<String>> {
    let path = Manifest::corpus_dir()
        .join("expected")
        .join(format!("{table}.tsv"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    text.split('\n')
        .filter(|l| !l.is_empty())
        .map(|l| l.split('\t').map(str::to_string).collect())
        .collect()
}

/// One decoded value, ready to compare with the reference's text.
#[derive(Debug, Clone)]
pub enum Cell {
    Text(String),
    F32(Option<f32>),
    F64(Option<f64>),
}

impl Cell {
    /// Floats are printed by the reference as shortest round-trip text, so parsing the expected
    /// text must give back the decoded value bit for bit; everything else compares as text.
    pub fn matches(&self, expected: &str) -> bool {
        match self {
            Cell::Text(s) => s == expected,
            Cell::F32(None) | Cell::F64(None) => expected == "NULL",
            Cell::F32(Some(v)) => expected
                .parse::<f32>()
                .is_ok_and(|e| e.to_bits() == v.to_bits()),
            Cell::F64(Some(v)) => expected
                .parse::<f64>()
                .is_ok_and(|e| e.to_bits() == v.to_bits()),
        }
    }
}

fn text<T>(values: &[Option<T>], render: impl Fn(&T) -> String) -> Vec<Cell> {
    values
        .iter()
        .map(|v| Cell::Text(v.as_ref().map_or_else(|| "NULL".to_string(), &render)))
        .collect()
}

/// Render a decoded column the way `mysql --batch --raw` printed the reference's results.
pub fn cells(values: &ColumnValues) -> Vec<Cell> {
    match values {
        ColumnValues::Bool(v) => text(v, |b| if *b { "1" } else { "0" }.to_string()),
        ColumnValues::I8(v) => text(v, ToString::to_string),
        ColumnValues::I16(v) => text(v, ToString::to_string),
        ColumnValues::I32(v) => text(v, ToString::to_string),
        ColumnValues::I64(v) => text(v, ToString::to_string),
        ColumnValues::I128(v) => text(v, ToString::to_string),
        ColumnValues::F32(v) => v.iter().map(|x| Cell::F32(*x)).collect(),
        ColumnValues::F64(v) => v.iter().map(|x| Cell::F64(*x)).collect(),
        ColumnValues::Decimal {
            unscaled, scale, ..
        } => text(unscaled, |u| render_decimal(*u, *scale)),
        ColumnValues::DecimalV2 { values, scale, .. } => {
            text(values, |d| render_decimalv2(*d, *scale))
        }
        ColumnValues::Date(v) => text(v, render_date),
        ColumnValues::DateTime { values, scale } => text(values, |d| render_datetime(d, *scale)),
        ColumnValues::Ipv4(v) => text(v, |x| render_ipv4(*x)),
        ColumnValues::Ipv6(v) => text(v, |x| render_ipv6(*x)),
    }
}

/// DECIMAL32/64/128I: exactly `scale` fraction digits, a leading `-` for negatives.
pub fn render_decimal(unscaled: i128, scale: u32) -> String {
    let digits = unscaled.unsigned_abs().to_string();
    let sign = if unscaled < 0 { "-" } else { "" };
    let s = scale as usize;
    if s == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>width$}", width = s + 1);
    let (int, frac) = padded.split_at(padded.len() - s);
    format!("{sign}{int}.{frac}")
}

/// DECIMALV2: port of `DecimalV2Value::to_string(scale)` (decimalv2_value.cpp). The value is
/// `integer * 10^9 + fraction`; below scale 9 the fraction rounds half up and may carry into the
/// integer part. The corpus exercises scale 9 only.
pub fn render_decimalv2(d: Decimal12, scale: u32) -> String {
    const SCALE_TRIM: [i64; 10] = [
        1_000_000_000,
        100_000_000,
        10_000_000,
        1_000_000,
        100_000,
        10_000,
        1_000,
        100,
        10,
        1,
    ];
    let value = i128::from(d.integer) * 1_000_000_000 + i128::from(d.fraction);
    let mut int_val = (value / 1_000_000_000) as i64;
    let mut frac_val = (value % 1_000_000_000).unsigned_abs() as i64;
    let scale = scale.min(9) as usize;
    if scale < 9 {
        let mut trimmed = frac_val / SCALE_TRIM[scale];
        if frac_val / SCALE_TRIM[scale + 1] % 10 >= 5 {
            trimmed += 1;
            if trimmed >= SCALE_TRIM[9 - scale] {
                trimmed = 0;
                if value >= 0 {
                    int_val += 1;
                } else {
                    int_val -= 1;
                }
            }
        }
        frac_val = trimmed;
    }
    if scale == 0 {
        return int_val.to_string();
    }
    let sign = if value < 0 && int_val == 0 && frac_val != 0 {
        "-"
    } else {
        ""
    };
    format!("{sign}{int_val}.{frac_val:0>scale$}")
}

pub fn render_date(d: &Date) -> String {
    format!("{:04}-{:02}-{:02}", d.year, d.month, d.day)
}

/// DATETIME(scale): seconds always, then exactly `scale` digits of the stored microseconds.
pub fn render_datetime(dt: &DateTime, scale: u32) -> String {
    let base = format!(
        "{} {:02}:{:02}:{:02}",
        render_date(&dt.date),
        dt.hour,
        dt.minute,
        dt.second
    );
    if scale == 0 {
        return base;
    }
    let micros = format!("{:06}", dt.microsecond);
    format!("{base}.{}", &micros[..(scale.min(6) as usize)])
}

/// IPV4 is a host-order u32: `127.0.0.1` is `0x7f000001`.
pub fn render_ipv4(v: u32) -> String {
    let [a, b, c, d] = v.to_be_bytes();
    format!("{a}.{b}.{c}.{d}")
}

/// IPV6: port of `format_ipv6` (exec/common/format_ip.h, APR `inet_ntop6`). The address bytes
/// are the big-endian bytes of the stored u128. The longest run of at least two zero words is
/// compressed (the first wins a tie); `::a.b.c.d` and `::ffff:a.b.c.d` print the embedded IPv4.
pub fn render_ipv6(v: u128) -> String {
    let bytes = v.to_be_bytes();
    let words: Vec<u16> = (0..8)
        .map(|i| u16::from_be_bytes([bytes[2 * i], bytes[2 * i + 1]]))
        .collect();
    let (mut best_base, mut best_len) = (None::<usize>, 0usize);
    let (mut cur_base, mut cur_len) = (None::<usize>, 0usize);
    for (i, w) in words.iter().enumerate() {
        if *w == 0 {
            if cur_base.is_none() {
                cur_base = Some(i);
                cur_len = 1;
            } else {
                cur_len += 1;
            }
        } else if let Some(b) = cur_base.take() {
            if best_base.is_none() || cur_len > best_len {
                best_base = Some(b);
                best_len = cur_len;
            }
        }
    }
    if let Some(b) = cur_base {
        if best_base.is_none() || cur_len > best_len {
            best_base = Some(b);
            best_len = cur_len;
        }
    }
    if best_len < 2 {
        best_base = None;
    }
    let mut out = String::new();
    let mut i = 0;
    while i < 8 {
        if let Some(b) = best_base {
            if i >= b && i < b + best_len {
                if i == b {
                    out.push(':');
                }
                i += 1;
                continue;
            }
        }
        if i != 0 {
            out.push(':');
        }
        if i == 6
            && best_base == Some(0)
            && (best_len == 6 || (best_len == 5 && words[5] == 0xffff))
        {
            out.push_str(&format!(
                "{}.{}.{}.{}",
                bytes[12], bytes[13], bytes[14], bytes[15]
            ));
            break;
        }
        out.push_str(&format!("{:x}", words[i]));
        i += 1;
    }
    if best_base.is_some_and(|b| b + best_len == 8) {
        out.push(':');
    }
    out
}

/// Decode the given columns (unique id, name) of every segment of `table` into cells, one row
/// per table row, sorted by the first column, which must be the INT key `k` (unique id 0).
pub fn decode_rows(m: &Manifest, table: &str, columns: &[(u32, &str)]) -> Vec<Vec<Cell>> {
    assert_eq!(columns[0], (0, "k"), "first column must be the key k");
    let mut rows = Vec::new();
    for f in &m.tables[table].files {
        let seg = SegmentFile::open(&m.resolve(&f.path)).unwrap();
        let n = seg.footer.num_rows.unwrap() as usize;
        let cols: Vec<Vec<Cell>> = columns
            .iter()
            .map(|(uid, name)| {
                let v = read_column(&seg, *uid)
                    .unwrap_or_else(|e| panic!("{table}/{}: {name}: {e}", f.path.display()));
                assert_eq!(v.len(), n, "{table}: column {name} length");
                cells(&v)
            })
            .collect();
        rows.extend((0..n).map(|i| cols.iter().map(|c| c[i].clone()).collect::<Vec<_>>()));
    }
    rows.sort_by_key(|r| match &r[0] {
        Cell::Text(s) => s.parse::<i64>().expect("k is an integer"),
        other => panic!("k must render as text, got {other:?}"),
    });
    rows
}

/// Assert decoded rows equal the expected TSV on the given columns (TSV index = unique id).
pub fn assert_rows_match(table: &str, columns: &[(u32, &str)], rows: &[Vec<Cell>]) {
    let expected = expected_rows(table);
    assert_eq!(rows.len(), expected.len(), "{table}: row count");
    for (row, exp) in rows.iter().zip(&expected) {
        for ((uid, name), cell) in columns.iter().zip(row) {
            let e = &exp[*uid as usize];
            assert!(
                cell.matches(e),
                "{table} k={}: column {name}: expected {e:?}, decoded {cell:?}",
                exp[0]
            );
        }
    }
}

/// `decode_rows` then `assert_rows_match`.
pub fn assert_table_matches(table: &str, columns: &[(u32, &str)]) {
    let m = Manifest::load();
    let rows = decode_rows(&m, table, columns);
    assert_rows_match(table, columns, &rows);
}
