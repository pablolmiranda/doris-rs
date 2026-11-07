//! The tablet reader the OLAP scan uses (research note [seg] §0): the rowsets on the version
//! path to `v`, each rowset's segments in order, every row, the scan's columns by unique id.
//! Duplicate-key tablets need no merge. What doris-rs cannot read correctly is refused with an
//! error, never answered with fewer or wrong rows: rowsets carrying a delete predicate,
//! unique- or aggregate-key tablets holding rows, columns a segment does not have, and the
//! encodings and codecs the segment reader does not decode.
use doris_proto::doris::{KeysType, RowsetMetaPb, TabletSchemaPb};
use doris_segment::column::{column_meta, read_column};
use doris_segment::writer::{datums, Datum};
use doris_segment::SegmentFile;
use doris_thrift::gen::types::TPrimitiveType;

use crate::{StorageError, TabletManager};

/// What the reader needs of a column's type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColTypeRef {
    pub ptype: TPrimitiveType,
    pub precision: i32,
    pub scale: i32,
}

/// A column a scan reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanColumn {
    pub unique_id: u32,
    pub ty: ColTypeRef,
}

/// The primitive type of a tablet schema's type name (`ColumnPB.type`).
pub fn primitive_type(name: &str) -> Option<TPrimitiveType> {
    use TPrimitiveType as T;
    Some(match name {
        "BOOLEAN" => T::BOOLEAN,
        "TINYINT" => T::TINYINT,
        "SMALLINT" => T::SMALLINT,
        "INT" => T::INT,
        "BIGINT" => T::BIGINT,
        "LARGEINT" => T::LARGEINT,
        "FLOAT" => T::FLOAT,
        "DOUBLE" => T::DOUBLE,
        "DECIMAL32" => T::DECIMAL32,
        "DECIMAL64" => T::DECIMAL64,
        "DECIMAL128I" => T::DECIMAL128I,
        "DATEV2" => T::DATEV2,
        "DATETIMEV2" => T::DATETIMEV2,
        "CHAR" => T::CHAR,
        "VARCHAR" => T::VARCHAR,
        "STRING" => T::STRING,
        _ => return None,
    })
}

/// Every column of a tablet schema whose type the reader knows, in schema order.
pub fn scan_columns(schema: &TabletSchemaPb) -> Vec<ScanColumn> {
    schema
        .column
        .iter()
        .filter_map(|c| {
            let ptype = primitive_type(&c.r#type)?;
            Some(ScanColumn {
                unique_id: u32::try_from(c.unique_id).ok()?,
                ty: ColTypeRef {
                    ptype,
                    precision: c.precision(),
                    scale: c.frac(),
                },
            })
        })
        .collect()
}

/// The rowsets that make up version `v`: from version 0, at each step the widest rowset that
/// starts there and does not pass `v` (`VersionGraph::capture_consistent_versions`). Of two
/// rowsets with the same versions, the earlier in `rowsets` wins: callers list the visible
/// rowsets before the stale ones. None when no path reaches `v`.
pub fn version_path(rowsets: &[RowsetMetaPb], v: i64) -> Option<Vec<&RowsetMetaPb>> {
    let mut path = Vec::new();
    let mut next = 0;
    while next <= v {
        let (_, step) = rowsets
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                r.start_version() == next && r.end_version() <= v && r.end_version() >= next
            })
            .max_by_key(|(i, r)| (r.end_version(), std::cmp::Reverse(*i)))?;
        path.push(step);
        next = step.end_version() + 1;
    }
    Some(path)
}

fn refuse(tablet_id: i64, what: impl Into<String>) -> StorageError {
    StorageError::UnsupportedRead {
        tablet_id,
        what: what.into(),
    }
}

/// Every row of a tablet at `version`, as the given columns: one row vector per segment, in
/// version-path order, then segment order.
pub fn scan_tablet(
    mgr: &TabletManager,
    tablet_id: i64,
    version: i64,
    columns: &[ScanColumn],
) -> Result<Vec<Vec<Vec<Datum>>>, StorageError> {
    let meta = mgr
        .tablet(tablet_id)
        .ok_or(StorageError::TabletNotFound { tablet_id })?;
    let dir = mgr
        .tablet_path(tablet_id)
        .ok_or(StorageError::TabletNotFound { tablet_id })?;
    let all: Vec<RowsetMetaPb> = meta
        .rs_metas
        .iter()
        .chain(&meta.stale_rs_metas)
        .cloned()
        .collect();
    let path =
        version_path(&all, version).ok_or(StorageError::MissedVersion { tablet_id, version })?;

    let keys = meta
        .schema
        .as_ref()
        .map(|s| s.keys_type())
        .unwrap_or(KeysType::DupKeys);
    if keys != KeysType::DupKeys && path.iter().any(|r| r.num_rows() > 0) {
        return Err(refuse(
            tablet_id,
            format!(
                "{} tablets with rows (they need a merge)",
                keys.as_str_name()
            ),
        ));
    }
    if let Some(r) = path.iter().find(|r| r.delete_predicate.is_some()) {
        return Err(refuse(
            tablet_id,
            format!(
                "rowset {}-{} carries a delete predicate",
                r.start_version(),
                r.end_version()
            ),
        ));
    }

    let mut out = Vec::new();
    for r in path {
        for seg in 0..r.num_segments() {
            let file = dir.join(format!("{}_{seg}.dat", r.rowset_id_v2()));
            let segment = SegmentFile::open(&file).map_err(|e| refuse(tablet_id, e.to_string()))?;
            let rows = segment.footer.num_rows.unwrap_or(0) as usize;
            let mut cols = Vec::with_capacity(columns.len());
            for c in columns {
                if column_meta(&segment, c.unique_id).is_err() {
                    return Err(refuse(
                        tablet_id,
                        format!(
                            "column {} is not in segment {}",
                            c.unique_id,
                            file.display()
                        ),
                    ));
                }
                let values = read_column(&segment, c.unique_id)
                    .map_err(|e| refuse(tablet_id, format!("{}: {e}", file.display())))?;
                let col = datums(&values);
                if col.len() != rows {
                    return Err(refuse(
                        tablet_id,
                        format!("column {} holds {} rows of {rows}", c.unique_id, col.len()),
                    ));
                }
                cols.push(col);
            }
            let mut its: Vec<_> = cols.into_iter().map(Vec::into_iter).collect();
            out.push(
                (0..rows)
                    .map(|_| {
                        its.iter_mut()
                            .map(|c| c.next().unwrap_or(Datum::Null))
                            .collect()
                    })
                    .collect(),
            );
        }
    }
    Ok(out)
}

/// A row's id, private to doris-rs (spec §4.4): it never leaves the BE. 24 bytes: tablet id and
/// version (i64 LE), the segment's position among the tablet's segments at that version, and the
/// row's ordinal in the segment (u32 LE).
pub fn row_id(tablet_id: i64, version: i64, segment: u32, ordinal: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&tablet_id.to_le_bytes());
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(&segment.to_le_bytes());
    out.extend_from_slice(&ordinal.to_le_bytes());
    out
}

fn parse_row_id(id: &[u8]) -> Option<(i64, i64, usize, usize)> {
    if id.len() != 24 {
        return None;
    }
    let i64_at = |at: usize| i64::from_le_bytes(id[at..at + 8].try_into().expect("8 bytes"));
    let u32_at = |at: usize| u32::from_le_bytes(id[at..at + 4].try_into().expect("4 bytes")) as usize;
    Some((i64_at(0), i64_at(8), u32_at(16), u32_at(20)))
}

/// The given columns of the rows the ids name, in the ids' order.
pub fn fetch_by_row_ids(mgr: &TabletManager, ids: &[Vec<u8>], columns: &[ScanColumn]) -> Result<Vec<Vec<Datum>>, StorageError> {
    let mut read: std::collections::HashMap<(i64, i64), Vec<Vec<Vec<Datum>>>> = std::collections::HashMap::new();
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let (tablet, version, segment, ordinal) = parse_row_id(id).ok_or_else(|| refuse(-1, format!("malformed row id of {} bytes", id.len())))?;
        if let std::collections::hash_map::Entry::Vacant(e) = read.entry((tablet, version)) {
            e.insert(scan_tablet(mgr, tablet, version, columns)?);
        }
        let row = read[&(tablet, version)]
            .get(segment)
            .and_then(|s| s.get(ordinal))
            .ok_or_else(|| refuse(tablet, format!("row id {version}/{segment}/{ordinal} names no row")))?;
        out.push(row.clone());
    }
    Ok(out)
}
