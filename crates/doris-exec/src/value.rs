//! Values as expressions produce them, column types, and the conversion of a value into what
//! the segment writer stores for a destination column (`OlapTableBlockConvertor`'s checks
//! included: NULL into NOT NULL, strings longer than the column).
use doris_segment::writer::Datum;
use doris_thrift::gen::descriptors::TColumn;
use doris_thrift::gen::types::{TPrimitiveType, TTypeDesc};

use crate::RowError;

/// A value as an expression evaluates it. Decimals and dates keep their literal text until the
/// destination column's scale and type decide their stored form.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Str(Vec<u8>),
    /// A DECIMAL literal's text ("2.1000").
    Decimal(String),
    /// A DATE/DATETIME literal's text ("2017-10-01", "2025-01-01 00:00:00.123").
    Date(String),
}

/// A column's type as the FE describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColType {
    pub name: String,
    pub ptype: TPrimitiveType,
    pub len: i32,
    pub precision: i32,
    pub scale: i32,
    pub nullable: bool,
}

impl ColType {
    pub fn from_column(c: &TColumn) -> ColType {
        let t = &c.column_type;
        ColType {
            name: c.column_name.clone(),
            ptype: t.type_,
            len: t.len.unwrap_or(-1),
            precision: t.precision.unwrap_or(0),
            scale: t.scale.unwrap_or(0),
            nullable: c.is_allow_null.unwrap_or(false),
        }
    }

    /// The scalar type of a type descriptor; None for nested types.
    pub fn from_type_desc(name: &str, t: &TTypeDesc, nullable: bool) -> Option<ColType> {
        let node = t.types.as_ref()?.first()?;
        let s = node.scalar_type.as_ref()?;
        if t.types.as_ref()?.len() != 1 {
            return None;
        }
        Some(ColType {
            name: name.to_string(),
            ptype: s.type_,
            len: s.len.unwrap_or(-1),
            precision: s.precision.unwrap_or(0),
            scale: s.scale.unwrap_or(0),
            nullable,
        })
    }

    /// The name the C++ BE prints for the column's type (`IDataType::get_name`).
    pub fn type_name(&self) -> String {
        let p = self.ptype;
        match p {
            TPrimitiveType::BOOLEAN => "BOOL".into(),
            TPrimitiveType::TINYINT => "TINYINT".into(),
            TPrimitiveType::SMALLINT => "SMALLINT".into(),
            TPrimitiveType::INT => "INT".into(),
            TPrimitiveType::BIGINT => "BIGINT".into(),
            TPrimitiveType::LARGEINT => "LARGEINT".into(),
            TPrimitiveType::FLOAT => "FLOAT".into(),
            TPrimitiveType::DOUBLE => "DOUBLE".into(),
            TPrimitiveType::DATEV2 => "DateV2".into(),
            TPrimitiveType::DATETIMEV2 => format!("DateTimeV2({})", self.scale),
            TPrimitiveType::DECIMAL32 | TPrimitiveType::DECIMAL64 | TPrimitiveType::DECIMAL128I => {
                format!("Decimal({}, {})", self.precision, self.scale)
            }
            TPrimitiveType::CHAR | TPrimitiveType::VARCHAR | TPrimitiveType::STRING => "String".into(),
            other => format!("{other:?}"),
        }
    }

    /// Whether the M4 writer stores this type.
    pub fn supported(&self) -> bool {
        matches!(
            self.ptype,
            TPrimitiveType::BOOLEAN
                | TPrimitiveType::TINYINT
                | TPrimitiveType::SMALLINT
                | TPrimitiveType::INT
                | TPrimitiveType::BIGINT
                | TPrimitiveType::LARGEINT
                | TPrimitiveType::FLOAT
                | TPrimitiveType::DOUBLE
                | TPrimitiveType::DATEV2
                | TPrimitiveType::DATETIMEV2
                | TPrimitiveType::DECIMAL32
                | TPrimitiveType::DECIMAL64
                | TPrimitiveType::DECIMAL128I
                | TPrimitiveType::CHAR
                | TPrimitiveType::VARCHAR
                | TPrimitiveType::STRING
        )
    }
}

fn row_err(col: &ColType, what: String) -> RowError {
    RowError::new(format!("column_name[{}], {what}", col.name))
}

/// The value as the column stores it; a row error when the sink must filter the row. Not
/// `strict` (an insert with `enable_insert_strict=false`, a stream load without strict mode),
/// a CHAR/VARCHAR value longer than its column is first cut to the column's length in
/// characters, then checked in bytes.
pub fn to_datum(v: &Value, col: &ColType, strict: bool) -> Result<Datum, RowError> {
    use TPrimitiveType as T;
    if *v == Value::Null {
        if !col.nullable {
            return Err(row_err(col, format!("null value for not null column, type={}", col.type_name())));
        }
        return Ok(Datum::Null);
    }
    let bad = || row_err(col, format!("can not convert {v:?} to {}", col.type_name()));
    Ok(match col.ptype {
        T::CHAR | T::VARCHAR | T::STRING => {
            let b = match v {
                Value::Str(b) => b.clone(),
                Value::Int(i) => i.to_string().into_bytes(),
                Value::Float(f) => f.to_string().into_bytes(),
                Value::Bool(b) => if *b { b"1".to_vec() } else { b"0".to_vec() },
                Value::Decimal(s) | Value::Date(s) => s.as_bytes().to_vec(),
                Value::Null => unreachable!(),
            };
            let b = if !strict && matches!(col.ptype, T::CHAR | T::VARCHAR) && col.len >= 0 && b.len() > col.len as usize {
                String::from_utf8_lossy(&b).chars().take(col.len as usize).collect::<String>().into_bytes()
            } else {
                b
            };
            // OlapTableBlockConvertor::_validate_column: CHAR/VARCHAR length in bytes
            if matches!(col.ptype, T::CHAR | T::VARCHAR) && col.len >= 0 && b.len() > col.len as usize {
                let shown = String::from_utf8_lossy(&b[..b.len().min(32)]).into_owned();
                return Err(row_err(
                    col,
                    format!(
                        "the length of input is too long than schema. first 32 bytes of input str: [{shown}] schema length: {}; actual length: {}; ",
                        col.len,
                        b.len()
                    ),
                ));
            }
            Datum::Bytes(b)
        }
        T::BOOLEAN => match v {
            Value::Bool(b) => Datum::Bool(*b),
            Value::Int(i) => Datum::Bool(*i != 0),
            _ => return Err(bad()),
        },
        T::TINYINT | T::SMALLINT | T::INT | T::BIGINT | T::LARGEINT => match v {
            Value::Int(i) => Datum::Int(*i),
            Value::Bool(b) => Datum::Int(i128::from(*b)),
            _ => return Err(bad()),
        },
        T::FLOAT => match v {
            Value::Float(f) => Datum::Float(*f as f32),
            Value::Int(i) => Datum::Float(*i as f32),
            _ => return Err(bad()),
        },
        T::DOUBLE => match v {
            Value::Float(f) => Datum::Double(*f),
            Value::Int(i) => Datum::Double(*i as f64),
            _ => return Err(bad()),
        },
        T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I => match v {
            Value::Decimal(s) | Value::Date(s) => Datum::Int(parse_decimal(s, col.scale).ok_or_else(bad)?),
            Value::Int(i) => Datum::Int(i * 10i128.pow(col.scale.max(0) as u32)),
            _ => return Err(bad()),
        },
        T::DATEV2 => match v {
            Value::Date(s) => Datum::Int(parse_datev2(s).ok_or_else(bad)?.into()),
            _ => return Err(bad()),
        },
        T::DATETIMEV2 => match v {
            Value::Date(s) => Datum::Int(parse_datetimev2(s, col.scale).ok_or_else(bad)?.into()),
            _ => return Err(bad()),
        },
        _ => return Err(row_err(col, format!("type {} is not supported", col.type_name()))),
    })
}

/// A decimal's text as its unscaled integer at `scale` (extra digits cut, missing ones zero).
pub fn parse_decimal(s: &str, scale: i32) -> Option<i128> {
    let s = s.trim();
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    if int.is_empty() && frac.is_empty() || !int.bytes().all(|b| b.is_ascii_digit()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let scale = scale.max(0) as usize;
    let mut digits = String::from(if int.is_empty() { "0" } else { int });
    let mut f: String = frac.chars().take(scale).collect();
    while f.len() < scale {
        f.push('0');
    }
    digits.push_str(&f);
    let v: i128 = digits.parse().ok()?;
    Some(if neg { -v } else { v })
}

fn date_parts(s: &str) -> Option<(u32, u32, u32, &str)> {
    let s = s.trim();
    let (date, rest) = match s.find([' ', 'T']) {
        Some(i) => (&s[..i], s[i + 1..].trim()),
        None => (s, ""),
    };
    let mut p = date.split('-');
    let y: u32 = p.next()?.parse().ok()?;
    let m: u32 = p.next()?.parse().ok()?;
    let d: u32 = p.next()?.parse().ok()?;
    if p.next().is_some() || !(1..=12).contains(&m) || d == 0 || d > days_in_month(y, m) {
        return None;
    }
    Some((y, m, d, rest))
}

fn days_in_month(y: u32, m: u32) -> u32 {
    match m {
        2 if (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// `YYYY-MM-DD` as a packed DATEV2.
pub fn parse_datev2(s: &str) -> Option<u32> {
    let (y, m, d, _) = date_parts(s)?;
    Some(y << 9 | m << 5 | d)
}

/// `YYYY-MM-DD[ hh:mm:ss[.ffffff]]` as a packed DATETIMEV2, the fraction cut to `scale` digits.
pub fn parse_datetimev2(s: &str, scale: i32) -> Option<u64> {
    let (y, m, d, rest) = date_parts(s)?;
    let (mut hh, mut mi, mut ss, mut us) = (0u64, 0u64, 0u64, 0u64);
    if !rest.is_empty() {
        let (time, frac) = rest.split_once('.').unwrap_or((rest, ""));
        let mut t = time.split(':');
        hh = t.next()?.parse().ok()?;
        mi = t.next().unwrap_or("0").parse().ok()?;
        ss = t.next().unwrap_or("0").parse().ok()?;
        if hh > 23 || mi > 59 || ss > 59 || !frac.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let keep = scale.clamp(0, 6) as usize;
        let mut f: String = frac.chars().take(keep).collect();
        while f.len() < 6 {
            f.push('0');
        }
        us = f.parse().ok()?;
    }
    Some(u64::from(y) << 46 | u64::from(m) << 42 | u64::from(d) << 37 | hh << 32 | mi << 26 | ss << 20 | us)
}

/// A text field (stream load) as a value of the column's type; None when it does not parse
/// (the load then treats it as NULL, or as an error in strict mode).
pub fn parse_text(text: &str, col: &ColType) -> Option<Value> {
    use TPrimitiveType as T;
    let t = text.trim();
    Some(match col.ptype {
        T::CHAR | T::VARCHAR | T::STRING => Value::Str(text.as_bytes().to_vec()),
        T::BOOLEAN => match t.to_ascii_lowercase().as_str() {
            "1" | "true" => Value::Bool(true),
            "0" | "false" => Value::Bool(false),
            _ => return None,
        },
        T::TINYINT | T::SMALLINT | T::INT | T::BIGINT | T::LARGEINT => {
            let v: i128 = t.parse().ok()?;
            let bits = match col.ptype {
                T::TINYINT => 8,
                T::SMALLINT => 16,
                T::INT => 32,
                T::BIGINT => 64,
                _ => 128,
            };
            if bits < 128 && (v < -(1i128 << (bits - 1)) || v >= 1i128 << (bits - 1)) {
                return None;
            }
            Value::Int(v)
        }
        T::FLOAT | T::DOUBLE => Value::Float(t.parse().ok()?),
        T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I => {
            let v = parse_decimal(t, col.scale)?;
            // a value whose integer digits exceed precision - scale does not cast
            if col.precision > 0 && v.unsigned_abs() >= 10u128.pow(col.precision.clamp(1, 38) as u32) {
                return None;
            }
            Value::Decimal(t.to_string())
        }
        T::DATEV2 => {
            parse_datev2(t)?;
            Value::Date(t.to_string())
        }
        T::DATETIMEV2 => {
            parse_datetimev2(t, col.scale)?;
            Value::Date(t.to_string())
        }
        _ => return None,
    })
}
