CREATE TABLE `lz4f_big` (
  `k` int NOT NULL,
  `c_bool` boolean NULL,
  `c_mod` int NULL,
  `c_const` int NULL,
  `c_bigint` bigint NULL,
  `c_varchar` varchar(32) NULL
) ENGINE=OLAP
DUPLICATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V3",
"compression" = "LZ4F",
"light_schema_change" = "true",
"disable_auto_compaction" = "true",
"binlog.enable" = "false",
"binlog.ttl_seconds" = "86400",
"binlog.max_bytes" = "9223372036854775807",
"binlog.max_history_nums" = "9223372036854775807",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);
