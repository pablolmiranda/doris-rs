CREATE TABLE `strings_fallback` (
  `k` int NOT NULL,
  `c_char` char(20) NULL,
  `c_varchar` varchar(64) NULL,
  `c_string` text NULL
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
"light_schema_change" = "true",
"storage_dict_page_size" = "4096",
"disable_auto_compaction" = "true",
"binlog.enable" = "false",
"binlog.ttl_seconds" = "86400",
"binlog.max_bytes" = "9223372036854775807",
"binlog.max_history_nums" = "9223372036854775807",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);
