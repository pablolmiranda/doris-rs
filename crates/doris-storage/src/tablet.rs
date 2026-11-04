//! Builds a new tablet's header (`TabletMetaPB`) from the FE's `TCreateTabletReq` the way the C++
//! BE does, in two steps:
//!
//! 1. the `TabletMeta` constructor (`be/src/storage/tablet/tablet_meta.cpp:145`) fills a
//!    `TabletMetaPB` from the request, with `init_column_from_tcolumn` for the columns;
//! 2. `init_from_pb` then `to_meta_pb` (`tablet_meta.cpp:749,874`, `TabletSchema` and
//!    `TabletColumn` `init_from_pb`/`to_schema_pb` in `tablet_schema.cpp`) re-emit it, which is
//!    what is stored and what `/api/meta/header` prints: every field those functions set is
//!    present, unset ones take the proto or member default.
//!
//! The initial rowset (versions `[0, request.version]`, `Tablet::create_initial_rowset`) is added
//! in step 2 with the fields `RowsetMeta::to_rowset_pb` writes for an empty rowset.

use doris_proto::doris::{
    BinlogConfigPb, ColumnPb, CompressKind, DeleteBitmapPb, EncryptionAlgorithmPb, IndexType, InvertedIndexStorageFormatPb,
    KeysType, PUniqueId, PatternTypePb, RowsetMetaPb, RowsetStatePb, RowsetTypePb, SegmentsOverlapPb, SortType,
    TabletIndexPb, TabletMetaPb, TabletSchemaPb, TabletStatePb, TabletStorageFormatPb, TabletTypePb,
};
use doris_proto::segment_v2::CompressionTypePb;
use doris_thrift::gen::agent_service::{TCreateTabletReq, TStorageFormat, TTabletSchema};
use doris_thrift::gen::descriptors::{TColumn, TOlapTableIndex};
use doris_thrift::gen::types::{TPrimitiveType, TStorageMedium};

/// What the C++ BE generates when it creates a tablet: ids, times and the shard.
#[derive(Debug, Clone)]
pub struct NewTablet {
    pub shard_id: i32,
    pub tablet_uid: PUniqueId,
    pub rowset_id_v2: String,
    pub creation_time: i64,
}

/// `config::default_num_rows_per_column_file_block`.
const NUM_ROWS_PER_ROW_BLOCK: i32 = 1024;

pub fn build_tablet_meta(req: &TCreateTabletReq, new: &NewTablet) -> TabletMetaPb {
    let mut meta = construct(req, new);
    let schema = normalize_schema(meta.schema.take().unwrap_or_default());
    let rowset = initial_rowset(&meta, &schema, req.version.unwrap_or(1), new);
    meta.schema = Some(schema);
    normalize_meta(meta, rowset)
}

/// Step 1: `TabletMeta::create` and the `TabletMeta` constructor.
fn construct(req: &TCreateTabletReq, new: &NewTablet) -> TabletMetaPb {
    let ts = &req.tablet_schema;
    let mut schema = TabletSchemaPb {
        num_short_key_columns: Some(i32::from(ts.short_key_column_count)),
        num_rows_per_row_block: Some(NUM_ROWS_PER_ROW_BLOCK),
        sequence_col_idx: Some(ts.sequence_col_idx.unwrap_or(-1)),
        keys_type: KeysType::from_str_name(thrift_keys_type(ts)).map(|k| k as i32),
        compress_kind: Some(CompressKind::CompressLz4 as i32),
        compression_type: Some(compression(req) as i32),
        inverted_index_storage_format: Some(inverted_index_format(req) as i32),
        sort_type: Some(if ts.sort_type.map(|s| s.0) == Some(1) { SortType::Zorder } else { SortType::Lexical } as i32),
        sort_col_num: Some(ts.sort_col_num.unwrap_or(0)),
        cluster_key_uids: ts.cluster_key_uids.clone().unwrap_or_default(),
        ..Default::default()
    };
    let indexes = ts.indexes.clone().unwrap_or_default();
    let mut has_bf_columns = false;
    for (ordinal, tcolumn) in ts.columns.iter().enumerate() {
        let unique_id = match tcolumn.col_unique_id.unwrap_or(-1) {
            id if id >= 0 => id,
            _ => ordinal as i32,
        };
        let mut column = column_from_tcolumn(unique_id, tcolumn);
        if column.is_bf_column() {
            has_bf_columns = true;
        }
        for index in &indexes {
            let bf = matches!(index.index_type.map(|t| t.0), Some(2) | Some(3)); // BLOOMFILTER, NGRAM_BF
            let first = index.columns.as_ref().and_then(|c| c.first());
            if bf && first.is_some_and(|c| c.eq_ignore_ascii_case(&tcolumn.column_name)) {
                column.is_bf_column = Some(true);
                break;
            }
        }
        schema.column.push(column);
    }
    for index in &indexes {
        let pb = index_pb(index, &schema.column);
        schema.index.push(pb);
    }
    schema.next_column_unique_id = Some(ts.columns.len() as u32);
    if has_bf_columns {
        if let Some(fpp) = ts.bloom_filter_fpp {
            schema.bf_fpp = Some(fpp.0);
        }
    }
    schema.is_in_memory = ts.is_in_memory.or(schema.is_in_memory);
    schema.disable_auto_compaction = ts.disable_auto_compaction.or(schema.disable_auto_compaction);
    schema.enable_variant_flatten_nested = ts.variant_enable_flatten_nested.or(schema.enable_variant_flatten_nested);
    schema.delete_sign_idx = ts.delete_sign_idx.or(schema.delete_sign_idx);
    schema.store_row_column = ts.store_row_column.or(schema.store_row_column);
    schema.row_store_page_size = ts.row_store_page_size.or(schema.row_store_page_size);
    schema.storage_page_size = ts.storage_page_size.or(schema.storage_page_size);
    schema.storage_dict_page_size = ts.storage_dict_page_size.or(schema.storage_dict_page_size);
    schema.skip_write_index_on_load = ts.skip_write_index_on_load.or(schema.skip_write_index_on_load);
    if let Some(cids) = &ts.row_store_col_cids {
        schema.row_store_column_unique_ids = cids.clone();
    }
    if req.storage_format == Some(TStorageFormat::V3) {
        schema.storage_format = Some(TabletStorageFormatPb::TabletStorageFormatV3 as i32);
    }

    let binlog_config = req.binlog_config.as_ref().map(|b| {
        let d = BinlogConfigDefaults::default();
        BinlogConfigPb {
            enable: Some(b.enable.unwrap_or(d.enable)),
            ttl_seconds: Some(b.ttl_seconds.unwrap_or(d.ttl_seconds)),
            max_bytes: Some(b.max_bytes.unwrap_or(d.max_bytes)),
            max_history_nums: Some(b.max_history_nums.unwrap_or(d.max_history_nums)),
        }
    });
    TabletMetaPb {
        table_id: req.table_id,
        partition_id: req.partition_id,
        tablet_id: Some(req.tablet_id),
        replica_id: Some(req.replica_id.unwrap_or(0)),
        schema_hash: Some(ts.schema_hash),
        shard_id: Some(new.shard_id),
        creation_time: Some(new.creation_time),
        cumulative_layer_point: Some(-1),
        tablet_state: Some(TabletStatePb::PbRunning as i32),
        tablet_uid: Some(new.tablet_uid),
        tablet_type: Some(
            if req.tablet_type.map(|t| t.0) == Some(1) { TabletTypePb::TabletTypeMemory } else { TabletTypePb::TabletTypeDisk }
                as i32,
        ),
        enable_unique_key_merge_on_write: Some(req.enable_unique_key_merge_on_write.unwrap_or(false)),
        storage_policy_id: Some(req.storage_policy_id.unwrap_or(-1)),
        compaction_policy: Some(req.compaction_policy.clone().unwrap_or_else(|| "size_based".into())),
        time_series_compaction_goal_size_mbytes: Some(req.time_series_compaction_goal_size_mbytes.unwrap_or(1024)),
        time_series_compaction_file_count_threshold: Some(req.time_series_compaction_file_count_threshold.unwrap_or(1000)),
        time_series_compaction_time_threshold_seconds: Some(req.time_series_compaction_time_threshold_seconds.unwrap_or(3600)),
        time_series_compaction_empty_rowsets_threshold: Some(req.time_series_compaction_empty_rowsets_threshold.unwrap_or(5)),
        time_series_compaction_level_threshold: Some(req.time_series_compaction_level_threshold.unwrap_or(1)),
        vertical_compaction_num_columns_per_group: Some(req.vertical_compaction_num_columns_per_group.unwrap_or(5)),
        in_restore_mode: Some(false),
        binlog_config,
        encryption_algorithm: Some(
            match req.tde_algorithm.map(|t| t.0) {
                Some(1) => EncryptionAlgorithmPb::Aes256Ctr,
                Some(2) => EncryptionAlgorithmPb::Sm4128Ctr,
                _ => EncryptionAlgorithmPb::Plaintext,
            } as i32,
        ),
        schema: Some(schema),
        ..Default::default()
    }
}

/// `BinlogConfig` member defaults (`be/src/storage/binlog_config.h`).
struct BinlogConfigDefaults {
    enable: bool,
    ttl_seconds: i64,
    max_bytes: i64,
    max_history_nums: i64,
}

impl Default for BinlogConfigDefaults {
    fn default() -> Self {
        BinlogConfigDefaults { enable: false, ttl_seconds: i64::MAX, max_bytes: i64::MAX, max_history_nums: i64::MAX }
    }
}

fn thrift_keys_type(ts: &TTabletSchema) -> &'static str {
    match ts.keys_type.0 {
        1 => "DUP_KEYS",
        2 => "UNIQUE_KEYS",
        3 => "AGG_KEYS",
        _ => "",
    }
}

/// `TabletMeta::create`: `compression_type`, default LZ4F.
fn compression(req: &TCreateTabletReq) -> CompressionTypePb {
    match req.compression_type.map(|c| c.0).unwrap_or(5) {
        2 => CompressionTypePb::NoCompression,
        3 => CompressionTypePb::Snappy,
        4 => CompressionTypePb::Lz4,
        6 => CompressionTypePb::Zlib,
        7 => CompressionTypePb::Zstd,
        _ => CompressionTypePb::Lz4f,
    }
}

/// `TabletMeta::create`: the deprecated `inverted_index_storage_format` overrides the file
/// format (default V2); then the constructor maps it, anything else becoming V3.
fn inverted_index_format(req: &TCreateTabletReq) -> InvertedIndexStorageFormatPb {
    let mut file_format = req.inverted_index_file_storage_format.map(|f| f.0).unwrap_or(2);
    match req.inverted_index_storage_format.map(|f| f.0) {
        Some(1) => file_format = 1,
        Some(2) => file_format = 2,
        _ => {}
    }
    match file_format {
        1 => InvertedIndexStorageFormatPb::V1,
        2 => InvertedIndexStorageFormatPb::V2,
        _ => InvertedIndexStorageFormatPb::V3,
    }
}

/// `TPrimitiveType` names (`EnumToString`, gensrc/thrift/Types.thrift).
fn primitive_name(t: TPrimitiveType) -> &'static str {
    match t.0 {
        0 => "INVALID_TYPE",
        1 => "NULL_TYPE",
        2 => "BOOLEAN",
        3 => "TINYINT",
        4 => "SMALLINT",
        5 => "INT",
        6 => "BIGINT",
        7 => "FLOAT",
        8 => "DOUBLE",
        9 => "DATE",
        10 => "DATETIME",
        11 => "BINARY",
        12 => "DECIMAL_DEPRACTED",
        13 => "CHAR",
        14 => "LARGEINT",
        15 => "VARCHAR",
        16 => "HLL",
        17 => "DECIMALV2",
        19 => "BITMAP",
        20 => "ARRAY",
        21 => "MAP",
        22 => "STRUCT",
        23 => "STRING",
        24 => "ALL",
        25 => "QUANTILE_STATE",
        26 => "DATEV2",
        27 => "DATETIMEV2",
        28 => "TIMEV2",
        29 => "DECIMAL32",
        30 => "DECIMAL64",
        31 => "DECIMAL128I",
        32 => "JSONB",
        33 => "UNSUPPORTED",
        34 => "VARIANT",
        35 => "LAMBDA_FUNCTION",
        36 => "AGG_STATE",
        37 => "DECIMAL256",
        38 => "IPV4",
        39 => "IPV6",
        40 => "UINT32",
        41 => "UINT64",
        42 => "FIXED_LENGTH_OBJECT",
        43 => "VARBINARY",
        44 => "TIMESTAMPTZ",
        _ => "",
    }
}

/// `TAggregationType` names.
fn aggregation_name(t: i32) -> &'static str {
    match t {
        0 => "SUM",
        1 => "MAX",
        2 => "MIN",
        3 => "REPLACE",
        4 => "HLL_UNION",
        5 => "NONE",
        6 => "BITMAP_UNION",
        7 => "REPLACE_IF_NOT_NULL",
        8 => "QUANTILE_UNION",
        _ => "",
    }
}

/// `TabletColumn::get_field_type_by_string` then `get_string_by_field_type`: the type name a
/// header stores after the round trip.
fn stored_type_name(name: &str) -> &str {
    match name {
        "TINYINT" | "SMALLINT" | "INT" | "BIGINT" | "LARGEINT" | "UNSIGNED_TINYINT" | "UNSIGNED_SMALLINT" | "UNSIGNED_INT"
        | "UNSIGNED_BIGINT" | "IPV4" | "IPV6" | "FLOAT" | "DISCRETE_DOUBLE" | "DOUBLE" | "CHAR" | "DATE" | "DATEV2"
        | "DATETIMEV2" | "DATETIME" | "TIMESTAMPTZ" | "DECIMAL32" | "DECIMAL64" | "DECIMAL128I" | "DECIMAL256"
        | "DECIMAL" | "VARCHAR" | "STRING" | "JSONB" | "VARIANT" | "BOOLEAN" | "HLL" | "STRUCT" | "MAP" | "ARRAY"
        | "QUANTILE_STATE" | "AGG_STATE" => name,
        "LIST" => "ARRAY",
        "OBJECT" | "BITMAP" => "OBJECT",
        _ => "UNKNOWN",
    }
}

/// `TabletColumn::get_field_length_by_type`.
fn field_length(t: TPrimitiveType, len: i32) -> i32 {
    let len = len as u32;
    let n: u32 = match t.0 {
        3 | 2 => 1,
        4 => 2,
        5 | 38 | 26 | 7 | 29 => 4,
        6 | 10 | 27 | 44 | 8 | 30 => 8,
        14 | 39 | 25 | 19 | 31 => 16,
        9 => 3,
        13 => len,
        15 | 16 | 36 => len.wrapping_add(2),
        23 | 34 | 32 => len.wrapping_add(4),
        20..=22 => 65535,
        37 => 32,
        17 => 12,
        _ => 0,
    };
    n as i32
}

/// `TabletMeta::init_column_from_tcolumn`.
fn column_from_tcolumn(unique_id: i32, tcolumn: &TColumn) -> ColumnPb {
    let ty = &tcolumn.column_type;
    let length = field_length(ty.type_, ty.len.unwrap_or(0));
    let mut c = ColumnPb {
        unique_id,
        name: Some(tcolumn.column_name.clone()),
        is_auto_increment: Some(tcolumn.is_auto_increment.unwrap_or(false)),
        is_on_update_current_timestamp: tcolumn.is_on_update_current_timestamp,
        r#type: primitive_name(ty.type_).to_string(),
        length: Some(length),
        index_length: Some(length),
        precision: Some(ty.precision.unwrap_or(0)),
        frac: Some(ty.scale.unwrap_or(0)),
        result_is_nullable: tcolumn.result_is_nullable,
        be_exec_version: tcolumn.be_exec_version,
        ..Default::default()
    };
    if matches!(ty.type_.0, 15 | 23) {
        c.index_length = Some(ty.index_len.unwrap_or(10));
    }
    if tcolumn.is_key.unwrap_or(false) {
        c.is_key = Some(true);
        c.aggregation = Some("NONE".into());
    } else {
        c.is_key = Some(false);
        c.aggregation = Some(match &tcolumn.aggregation {
            Some(a) => a.clone(),
            None => aggregation_name(tcolumn.aggregation_type.map(|a| a.0).unwrap_or(0)).to_string(),
        });
    }
    c.is_nullable = Some(tcolumn.is_allow_null.unwrap_or(false));
    c.default_value = tcolumn.default_value.clone().map(String::into_bytes);
    c.is_bf_column = tcolumn.is_bloom_filter_column;
    c.visible = tcolumn.visible;
    for child in tcolumn.children_column.iter().flatten() {
        c.children_columns.push(column_from_tcolumn(child.col_unique_id.unwrap_or(-1), child));
    }
    c.variant_max_subcolumns_count = ty.variant_max_subcolumns_count;
    c.pattern_type = tcolumn.pattern_type.map(|p| {
        (if p.0 == 1 { PatternTypePb::MatchName } else { PatternTypePb::MatchNameGlob }) as i32
    });
    c.variant_enable_typed_paths_to_sparse = tcolumn.variant_enable_typed_paths_to_sparse;
    c.variant_max_sparse_column_statistics_size = tcolumn.variant_max_sparse_column_statistics_size;
    c.variant_sparse_hash_shard_count = tcolumn.variant_sparse_hash_shard_count;
    c.variant_enable_doc_mode = ty.variant_enable_doc_mode;
    c.variant_doc_materialization_min_rows = tcolumn.variant_doc_materialization_min_rows;
    c.variant_doc_hash_shard_count = tcolumn.variant_doc_hash_shard_count;
    c.variant_enable_nested_group = tcolumn.variant_enable_nested_group;
    c
}

/// The index part of the `TabletMeta` constructor.
fn index_pb(index: &TOlapTableIndex, columns: &[ColumnPb]) -> TabletIndexPb {
    let mut pb = TabletIndexPb { index_id: index.index_id, index_name: index.index_name.clone(), ..Default::default() };
    for name in index.columns.iter().flatten() {
        for c in columns {
            if c.name().eq_ignore_ascii_case(name) {
                pb.col_unique_id.push(c.unique_id);
            }
        }
    }
    pb.index_type = index
        .index_type
        .map(|t| match t.0 {
            0 => IndexType::Bitmap,
            1 => IndexType::Inverted,
            2 => IndexType::Bloomfilter,
            3 => IndexType::NgramBf,
            _ => IndexType::Ann,
        } as i32);
    if let Some(props) = &index.properties {
        pb.properties = props.clone().into_iter().collect();
    }
    pb
}

/// Step 2 for the schema: `TabletSchema::init_from_pb` then `to_schema_pb`.
pub(crate) fn normalize_schema(s: TabletSchemaPb) -> TabletSchemaPb {
    let storage_format = if s.storage_format.is_some() {
        s.storage_format()
    } else if s.is_external_segment_column_meta_used() || s.integer_type_default_use_plain_encoding() {
        TabletStorageFormatPb::TabletStorageFormatV3
    } else {
        TabletStorageFormatPb::TabletStorageFormatV2
    };
    let v3 = storage_format == TabletStorageFormatPb::TabletStorageFormatV3;
    TabletSchemaPb {
        cluster_key_uids: s.cluster_key_uids.clone(),
        keys_type: Some(s.keys_type() as i32),
        column: s.column.iter().map(normalize_column).collect(),
        index: s.index.clone(),
        num_short_key_columns: Some(s.num_short_key_columns()),
        num_rows_per_row_block: Some(s.num_rows_per_row_block()),
        compress_kind: Some(s.compress_kind() as i32),
        bf_fpp: s.bf_fpp,
        next_column_unique_id: Some(s.next_column_unique_id()),
        is_in_memory: Some(s.is_in_memory()),
        disable_auto_compaction: Some(s.disable_auto_compaction()),
        store_row_column: Some(s.store_row_column()),
        skip_write_index_on_load: Some(s.skip_write_index_on_load()),
        delete_sign_idx: Some(s.delete_sign_idx()),
        sequence_col_idx: Some(s.sequence_col_idx()),
        sort_type: Some(s.sort_type() as i32),
        sort_col_num: Some(s.sort_col_num()),
        schema_version: Some(s.schema_version()),
        compression_type: Some(s.compression_type() as i32),
        row_store_page_size: Some(s.row_store_page_size()),
        storage_page_size: Some(s.storage_page_size()),
        storage_dict_page_size: Some(s.storage_dict_page_size()),
        version_col_idx: Some(s.version_col_idx()),
        skip_bitmap_col_idx: Some(s.skip_bitmap_col_idx()),
        inverted_index_storage_format: Some(
            if s.inverted_index_storage_format.is_some() { s.inverted_index_storage_format() } else { InvertedIndexStorageFormatPb::V1 }
                as i32,
        ),
        row_store_column_unique_ids: s.row_store_column_unique_ids.clone(),
        enable_variant_flatten_nested: Some(s.enable_variant_flatten_nested()),
        storage_format: Some(storage_format as i32),
        is_external_segment_column_meta_used: v3.then_some(true),
        integer_type_default_use_plain_encoding: v3.then_some(true),
        binary_plain_encoding_default_impl: v3.then_some(1), // BINARY_PLAIN_ENCODING_V2
        ..Default::default()
    }
}

/// `TabletColumn::init_from_pb` then `to_schema_pb`, with the `TabletColumn` member defaults.
fn normalize_column(c: &ColumnPb) -> ColumnPb {
    let type_name = stored_type_name(&c.r#type).to_string();
    let agg_state = type_name == "AGG_STATE";
    let is_decimal = c.precision.is_some();
    ColumnPb {
        unique_id: c.unique_id,
        name: Some(c.name().to_string()),
        r#type: type_name,
        is_key: Some(c.is_key()),
        is_nullable: Some(c.is_nullable()),
        is_auto_increment: Some(c.is_auto_increment()),
        is_on_update_current_timestamp: Some(c.is_on_update_current_timestamp.unwrap_or(false)),
        default_value: c.default_value.clone(),
        precision: is_decimal.then(|| c.precision()),
        frac: is_decimal.then(|| c.frac.unwrap_or(0)),
        length: Some(c.length()),
        index_length: Some(c.index_length()),
        is_bf_column: c.is_bf_column.filter(|b| *b),
        aggregation: c.aggregation.clone(),
        result_is_nullable: Some(if agg_state { c.result_is_nullable() } else { false }),
        be_exec_version: Some(if agg_state { c.be_exec_version() } else { -1 }),
        visible: Some(c.visible.unwrap_or(true)),
        children_columns: c.children_columns.iter().map(normalize_column).collect(),
        // VariantParams member defaults (tablet_schema.h), each kept only when the PB sets it
        variant_max_subcolumns_count: Some(c.variant_max_subcolumns_count.unwrap_or(-1)),
        pattern_type: Some(c.pattern_type.unwrap_or(PatternTypePb::MatchNameGlob as i32)),
        variant_enable_typed_paths_to_sparse: Some(c.variant_enable_typed_paths_to_sparse.unwrap_or(false)),
        variant_max_sparse_column_statistics_size: Some(c.variant_max_sparse_column_statistics_size.unwrap_or(-1)),
        variant_sparse_hash_shard_count: Some(c.variant_sparse_hash_shard_count.unwrap_or(-1)),
        variant_enable_doc_mode: Some(c.variant_enable_doc_mode.unwrap_or(false)),
        variant_doc_materialization_min_rows: Some(c.variant_doc_materialization_min_rows.unwrap_or(0)),
        variant_doc_hash_shard_count: Some(c.variant_doc_hash_shard_count.unwrap_or(128)),
        variant_enable_nested_group: Some(c.variant_enable_nested_group.unwrap_or(false)),
        ..Default::default()
    }
}

/// The empty rowset `Tablet::create_initial_rowset` writes, as `RowsetMeta::to_rowset_pb` emits it.
fn initial_rowset(meta: &TabletMetaPb, schema: &TabletSchemaPb, version: i64, new: &NewTablet) -> RowsetMetaPb {
    RowsetMetaPb {
        rowset_id: 0,
        partition_id: meta.partition_id,
        tablet_id: meta.tablet_id,
        tablet_schema_hash: meta.schema_hash,
        rowset_type: Some(RowsetTypePb::BetaRowset as i32),
        rowset_state: Some(RowsetStatePb::Visible as i32),
        start_version: Some(0),
        end_version: Some(version),
        num_rows: Some(0),
        total_disk_size: Some(0),
        data_disk_size: Some(0),
        index_disk_size: Some(0),
        empty: Some(true),
        creation_time: Some(new.creation_time),
        tablet_uid: Some(new.tablet_uid),
        num_segments: Some(0),
        rowset_id_v2: Some(new.rowset_id_v2.clone()),
        newest_write_timestamp: Some(new.creation_time),
        tablet_schema: Some(schema.clone()),
        segments_overlap_pb: Some(SegmentsOverlapPb::OverlapUnknown as i32),
        segments_key_bounds_truncated: Some(false),
        segments_key_bounds_aggregated: Some(false),
        schema_version: Some(schema.schema_version()),
        has_variant_type_in_schema: Some(false),
        ..Default::default()
    }
}

/// Step 2 for the tablet: `TabletMeta::init_from_pb` then `to_meta_pb`.
fn normalize_meta(m: TabletMetaPb, rowset: RowsetMetaPb) -> TabletMetaPb {
    let mow = m.enable_unique_key_merge_on_write();
    let binlog = m.binlog_config.unwrap_or_default();
    let d = BinlogConfigDefaults::default();
    TabletMetaPb {
        table_id: Some(m.table_id()),
        index_id: Some(m.index_id()),
        partition_id: Some(m.partition_id()),
        tablet_id: Some(m.tablet_id()),
        replica_id: Some(m.replica_id()),
        schema_hash: Some(m.schema_hash()),
        shard_id: Some(m.shard_id()),
        creation_time: Some(m.creation_time()),
        cumulative_layer_point: Some(m.cumulative_layer_point()),
        tablet_uid: m.tablet_uid,
        tablet_type: Some(if m.tablet_type.is_some() { m.tablet_type() } else { TabletTypePb::TabletTypeDisk } as i32),
        ttl_seconds: Some(m.ttl_seconds()),
        tablet_state: Some(m.tablet_state() as i32),
        rs_metas: vec![rowset],
        schema: m.schema.clone(),
        in_restore_mode: Some(m.in_restore_mode()),
        // _preferred_rowset_type defaults to BETA_ROWSET and is written when it is BETA
        preferred_rowset_type: Some(RowsetTypePb::BetaRowset as i32),
        storage_policy_id: m.storage_policy_id.filter(|id| *id > 0),
        enable_unique_key_merge_on_write: Some(mow),
        delete_bitmap: mow.then(DeleteBitmapPb::default),
        binlog_config: Some(BinlogConfigPb {
            enable: Some(binlog.enable.unwrap_or(d.enable)),
            ttl_seconds: Some(binlog.ttl_seconds.unwrap_or(d.ttl_seconds)),
            max_bytes: Some(binlog.max_bytes.unwrap_or(d.max_bytes)),
            max_history_nums: Some(binlog.max_history_nums.unwrap_or(d.max_history_nums)),
        }),
        compaction_policy: Some(m.compaction_policy().to_string()),
        time_series_compaction_goal_size_mbytes: Some(m.time_series_compaction_goal_size_mbytes()),
        time_series_compaction_file_count_threshold: Some(m.time_series_compaction_file_count_threshold()),
        time_series_compaction_time_threshold_seconds: Some(m.time_series_compaction_time_threshold_seconds()),
        time_series_compaction_empty_rowsets_threshold: Some(m.time_series_compaction_empty_rowsets_threshold()),
        time_series_compaction_level_threshold: Some(m.time_series_compaction_level_threshold()),
        vertical_compaction_num_columns_per_group: Some(m.vertical_compaction_num_columns_per_group()),
        encryption_algorithm: Some(m.encryption_algorithm() as i32),
        ..Default::default()
    }
}

/// The storage medium a tablet reports: the data dir's (HDD for M2's roots).
pub(crate) fn storage_medium() -> TStorageMedium {
    TStorageMedium::HDD
}
