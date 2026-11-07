//! Writing a beta rowset as the C++ BE's load path does (`RowsetBuilder`, `BetaRowsetWriter`):
//! one segment file `<rowset_id>_0.dat` in the tablet directory when there are rows, none
//! otherwise, and the `RowsetMetaPB` `BetaRowsetWriter::build` fills in, in state COMMITTED
//! until a publish makes it visible. Pending and visible rowsets outside a header live under
//! `rst_<tablet_uid>_<rowset_id>` in `meta/` (`RowsetMetaManager`).

use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use doris_proto::doris::{
    KeyBoundsPb, PUniqueId, RowsetMetaPb, RowsetStatePb, RowsetTypePb, SegmentsOverlapPb, TabletMetaPb, TabletSchemaPb,
};
use doris_segment::writer::{sort_rows, write_segment, ColumnSpec, Datum, SegmentSchema};

use crate::error::{IoContext, StorageError};

/// `RowsetMetaManager` key prefix (`rowset_meta_manager.h:42`).
pub const ROWSET_PREFIX: &str = "rst_";

/// `segments_key_bounds_truncation_threshold` (config.cpp:1822).
const KEY_BOUNDS_TRUNCATION: usize = 36;

/// `rst_<tablet_uid>_<rowset_id>`, the uid as `%016x-%016x` of its halves (`uid_util.h:90-96`).
pub fn rowset_meta_key(tablet_uid: &PUniqueId, rowset_id: &str) -> String {
    format!("{ROWSET_PREFIX}{:016x}-{:016x}_{rowset_id}", tablet_uid.hi as u64, tablet_uid.lo as u64)
}

/// The segment writer's view of a tablet schema.
pub fn segment_schema(schema: &TabletSchemaPb) -> Result<SegmentSchema, StorageError> {
    let columns = schema
        .column
        .iter()
        .map(|c| {
            ColumnSpec::from_pb(c).map_err(|_| StorageError::Unsupported {
                what: format!("column {} of type {}", c.name.as_deref().unwrap_or("?"), c.r#type),
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(SegmentSchema {
        columns,
        num_short_key_columns: schema.num_short_key_columns.unwrap_or(0).max(0) as u32,
        num_rows_per_block: schema.num_rows_per_row_block.unwrap_or(1024).max(1) as u32,
    })
}

#[derive(Debug, Clone)]
pub struct WrittenRowset {
    pub meta: RowsetMetaPb,
}

fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// Writes `rows` (schema order, any order) as a COMMITTED rowset of `tablet` into `dir`, the
/// tablet's directory. The segment file is synced before this returns.
pub fn write_rowset(
    dir: &Path,
    tablet: &TabletMetaPb,
    rowset_id: &str,
    txn_id: i64,
    load_id: &PUniqueId,
    mut rows: Vec<Vec<Datum>>,
) -> Result<WrittenRowset, StorageError> {
    let schema_pb = tablet.schema.clone().ok_or_else(|| StorageError::Unsupported { what: "a tablet without a schema".into() })?;
    let started = now_secs();
    let mut meta = RowsetMetaPb {
        rowset_id: 0,
        partition_id: tablet.partition_id,
        tablet_id: tablet.tablet_id,
        txn_id: Some(txn_id),
        tablet_schema_hash: tablet.schema_hash,
        rowset_type: Some(RowsetTypePb::BetaRowset as i32),
        rowset_state: Some(RowsetStatePb::Committed as i32),
        load_id: Some(*load_id),
        tablet_uid: tablet.tablet_uid,
        rowset_id_v2: Some(rowset_id.to_string()),
        newest_write_timestamp: Some(started),
        // RowsetWriterContext: segments start OVERLAPPING; one segment with key bounds is not
        segments_overlap_pb: Some(SegmentsOverlapPb::Overlapping as i32),
        segments_key_bounds_truncated: Some(false),
        segments_key_bounds_aggregated: Some(false),
        schema_version: Some(schema_pb.schema_version.unwrap_or(0)),
        has_variant_type_in_schema: Some(false),
        tablet_schema: Some(schema_pb.clone()),
        ..Default::default()
    };
    let (mut num_rows, mut data_size) = (0i64, 0i64);
    if !rows.is_empty() {
        let schema = segment_schema(&schema_pb)?;
        sort_rows(&schema, &mut rows);
        let seg = write_segment(&schema, &rows).map_err(|e| StorageError::Unsupported { what: format!("writing a segment: {e}") })?;
        let path = dir.join(format!("{rowset_id}_0.dat"));
        let mut f = std::fs::File::create(&path).at(&path)?;
        f.write_all(&seg.bytes).at(&path)?;
        f.sync_all().at(&path)?;
        // LocalFileWriter::close: the directory too, so the new file's entry survives a crash
        std::fs::File::open(dir).and_then(|d| d.sync_all()).at(dir)?;
        num_rows = i64::from(seg.num_rows);
        data_size = seg.bytes.len() as i64;
        let truncate = |mut k: Vec<u8>, cut: &mut bool| {
            if k.len() > KEY_BOUNDS_TRUNCATION {
                k.truncate(KEY_BOUNDS_TRUNCATION);
                *cut = true;
            }
            k
        };
        let mut cut = false;
        let bounds = KeyBoundsPb { min_key: truncate(seg.min_key, &mut cut), max_key: truncate(seg.max_key, &mut cut) };
        meta.segments_key_bounds = vec![bounds];
        meta.segments_key_bounds_aggregated = Some(true);
        meta.segments_key_bounds_truncated = Some(cut);
        meta.segments_overlap_pb = Some(SegmentsOverlapPb::Nonoverlapping as i32);
        meta.num_segment_rows = vec![i64::from(seg.num_rows)];
        meta.num_segments = Some(1);
    } else {
        meta.num_segments = Some(0);
    }
    meta.num_rows = Some(num_rows);
    meta.total_disk_size = Some(data_size);
    meta.data_disk_size = Some(data_size);
    meta.index_disk_size = Some(0);
    meta.empty = Some(num_rows == 0);
    meta.creation_time = Some(now_secs());
    Ok(WrittenRowset { meta })
}
