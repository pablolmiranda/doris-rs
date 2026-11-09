//! Which partition and tablets a row goes to (`VOlapTablePartitionParam::find_partition`,
//! `find_tablets`): the partition by its RANGE or LIST keys, then in each index the tablet at
//! `hash % num_buckets`, the hash a zlib crc32 chain over the distribution columns' values
//! (`RawValue::zlib_crc32`), NULL hashed as four zero bytes.
use std::cmp::Ordering;
use std::collections::HashMap;

use doris_segment::sort::compare_datum;
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::{TExpr, TExprNode};
use doris_thrift::gen::partitions::TPartitionType;
use doris_thrift::gen::types::TPrimitiveType;

use crate::expr::{eval, Slots};
use crate::fragment::WriteFragment;
use crate::value::{to_datum, ColType, Value};
use crate::{ExecError, RowError};

/// A routed row: its partition, the tablet in each index, and its stored values (destination
/// column order).
#[derive(Debug, Clone, PartialEq)]
pub struct Routed {
    pub partition_id: i64,
    pub tablets: Vec<(i64, i64)>,
    pub row: Vec<Datum>,
}

struct Partition {
    id: i64,
    num_buckets: i64,
    load_tablet_idx: Option<i64>,
    /// [start, end) for RANGE; None = unbounded
    start: Option<Vec<Datum>>,
    end: Option<Vec<Datum>>,
    in_keys: Vec<Vec<Datum>>,
    is_default: bool,
    indexes: Vec<(i64, Vec<i64>)>,
}

pub struct Router {
    dest: Vec<ColType>,
    strict: bool,
    kind: TPartitionType,
    partition_cols: Vec<usize>,
    distributed_cols: Vec<usize>,
    partitions: Vec<Partition>,
}

fn key_datums(nodes: &[TExprNode], cols: &[ColType]) -> Result<Vec<Datum>, ExecError> {
    nodes
        .iter()
        .zip(cols)
        .map(|(n, c)| {
            let v = eval(&TExpr { nodes: vec![n.clone()] }, &Slots::new()).map_err(|e| ExecError::BadRequest(e.msg))?;
            let mut c = c.clone();
            c.nullable = true;
            to_datum(&v, &c, true).map_err(|e| ExecError::BadRequest(e.msg))
        })
        .collect()
}

fn compare_keys(a: &[Datum], b: &[Datum]) -> Ordering {
    a.iter().zip(b).map(|(x, y)| compare_datum(x, y)).find(|o| o.is_ne()).unwrap_or(Ordering::Equal)
}

impl Router {
    pub fn new(f: &WriteFragment) -> Result<Router, ExecError> {
        let dest = f.dest_columns().to_vec();
        let pos: HashMap<&str, usize> = dest.iter().enumerate().map(|(i, c)| (c.name.as_str(), i)).collect();
        let find = |names: &[String]| -> Result<Vec<usize>, ExecError> {
            names
                .iter()
                .map(|n| pos.get(n.as_str()).copied().ok_or_else(|| ExecError::BadRequest(format!("column {n} is not written"))))
                .collect()
        };
        let p = &f.sink.partition;
        let partition_names = p.partition_columns.clone().or_else(|| p.partition_column.clone().map(|c| vec![c])).unwrap_or_default();
        let partition_cols = find(&partition_names)?;
        let distributed_cols = find(&p.distributed_columns.clone().unwrap_or_default())?;
        let key_cols: Vec<ColType> = partition_cols.iter().map(|i| dest[*i].clone()).collect();
        let mut partitions = Vec::new();
        for part in &p.partitions {
            let start = match (&part.start_keys, &part.start_key) {
                (Some(k), _) if !k.is_empty() => Some(key_datums(k, &key_cols)?),
                (_, Some(k)) => Some(key_datums(std::slice::from_ref(k), &key_cols)?),
                _ => None,
            };
            let end = match (&part.end_keys, &part.end_key) {
                (Some(k), _) if !k.is_empty() => Some(key_datums(k, &key_cols)?),
                (_, Some(k)) => Some(key_datums(std::slice::from_ref(k), &key_cols)?),
                _ => None,
            };
            let in_keys = part.in_keys.iter().flatten().map(|k| key_datums(k, &key_cols)).collect::<Result<_, _>>()?;
            partitions.push(Partition {
                id: part.id,
                num_buckets: i64::from(part.num_buckets.max(1)),
                load_tablet_idx: part.load_tablet_idx,
                start,
                end,
                in_keys,
                is_default: part.is_default_partition == Some(true),
                indexes: part.indexes.iter().map(|i| (i.index_id, i.tablets.clone())).collect(),
            });
        }
        Ok(Router { dest, strict: f.insert_strict, kind: p.partition_type.unwrap_or(TPartitionType::UNPARTITIONED), partition_cols, distributed_cols, partitions })
    }

    fn find_partition(&self, row: &[Datum]) -> Option<&Partition> {
        if self.partition_cols.is_empty() || self.partitions.len() == 1 && self.kind == TPartitionType::UNPARTITIONED {
            return self.partitions.first();
        }
        let key: Vec<Datum> = self.partition_cols.iter().map(|i| row[*i].clone()).collect();
        if self.kind == TPartitionType::LIST_PARTITIONED {
            return self
                .partitions
                .iter()
                .find(|p| p.in_keys.iter().any(|k| compare_keys(k, &key).is_eq()))
                .or_else(|| self.partitions.iter().find(|p| p.is_default));
        }
        self.partitions.iter().find(|p| {
            p.start.as_ref().is_none_or(|s| compare_keys(s, &key).is_le()) && p.end.as_ref().is_none_or(|e| compare_keys(&key, e).is_lt())
        })
    }

    fn bucket(&self, p: &Partition, row: &[Datum]) -> i64 {
        if self.distributed_cols.is_empty() {
            return p.load_tablet_idx.unwrap_or(0).max(0) % p.num_buckets;
        }
        let mut hash = 0u32;
        for &i in &self.distributed_cols {
            let bytes = hash_bytes(&self.dest[i], &row[i]);
            let mut h = crc32fast::Hasher::new_with_initial(hash);
            h.update(&bytes);
            hash = h.finalize();
        }
        i64::from(hash) % p.num_buckets
    }

    pub fn route(&self, values: &[Value]) -> Result<Routed, RowError> {
        let row = values.iter().zip(&self.dest).map(|(v, c)| to_datum(v, c, self.strict)).collect::<Result<Vec<_>, _>>()?;
        let Some(p) = self.find_partition(&row) else {
            // VOlapTablePartitionParam: the whole row, each column as "name(type)": "value"
            let shown: Vec<String> = self
                .dest
                .iter()
                .zip(&row)
                .map(|(c, d)| {
                    let ty = if c.nullable { format!("Nullable({})", c.type_name()) } else { c.type_name() };
                    format!("\"{}({ty})\":\"{}\"", c.name, render(c, d))
                })
                .collect();
            return Err(RowError::new(format!("no partition for this tuple. tuple=[{{{}}}]", shown.join(","))));
        };
        let b = self.bucket(p, &row) as usize;
        let tablets = p.indexes.iter().filter_map(|(idx, t)| t.get(b).map(|t| (*idx, *t))).collect();
        Ok(Routed { partition_id: p.id, tablets, row })
    }
}

/// A stored value as the C++ BE prints it in messages.
fn render(c: &ColType, d: &Datum) -> String {
    use doris_segment::values::{unpack_datetimev2, unpack_datev2};
    match d {
        Datum::Null => "NULL".into(),
        Datum::Bool(b) => u8::from(*b).to_string(),
        Datum::Int(i) => match c.ptype {
            TPrimitiveType::DATEV2 | TPrimitiveType::DATE => {
                let v = unpack_datev2(*i as u32);
                format!("{:04}-{:02}-{:02}", v.year, v.month, v.day)
            }
            TPrimitiveType::DATETIMEV2 | TPrimitiveType::DATETIME => {
                let t = unpack_datetimev2(*i as u64);
                let mut s = format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", t.date.year, t.date.month, t.date.day, t.hour, t.minute, t.second);
                if c.scale > 0 {
                    s.push_str(&format!(".{:06}", t.microsecond)[..1 + c.scale.min(6) as usize]);
                }
                s
            }
            TPrimitiveType::DECIMAL32 | TPrimitiveType::DECIMAL64 | TPrimitiveType::DECIMAL128I if c.scale > 0 => {
                let scale = c.scale as u32;
                let p = 10i128.pow(scale);
                let sign = if *i < 0 { "-" } else { "" };
                format!("{sign}{}.{:0w$}", (i / p).abs(), (i % p).abs(), w = scale as usize)
            }
            _ => i.to_string(),
        },
        Datum::Float(f) => f.to_string(),
        Datum::Double(f) => f.to_string(),
        Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
    }
}

/// The bytes `RawValue::zlib_crc32` hashes for a value of the column's type.
fn hash_bytes(c: &ColType, d: &Datum) -> Vec<u8> {
    use TPrimitiveType as T;
    let width = match c.ptype {
        T::BOOLEAN | T::TINYINT => 1,
        T::SMALLINT => 2,
        T::INT | T::DATEV2 | T::DECIMAL32 | T::FLOAT => 4,
        T::BIGINT | T::DATETIMEV2 | T::DECIMAL64 | T::DOUBLE => 8,
        _ => 16,
    };
    match d {
        Datum::Null => vec![0; 4],
        // v1 dates hash their text (`VecDateTimeValue::to_buffer`)
        Datum::Int(_) if matches!(c.ptype, T::DATE | T::DATETIME) => render(c, d).into_bytes(),
        Datum::Bool(b) => vec![u8::from(*b)],
        Datum::Int(i) => i.to_le_bytes()[..width].to_vec(),
        Datum::Float(f) => f.to_le_bytes().to_vec(),
        Datum::Double(f) => f.to_le_bytes().to_vec(),
        Datum::Bytes(b) => b.clone(),
    }
}
