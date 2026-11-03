CREATE TABLE `scalar_more` (
  `k` int NOT NULL,
  `c_dec32` decimal(9,2) NULL,
  `c_dec128` decimal(38,10) NULL,
  `c_dt0` datetime NULL,
  `c_dt3` datetime(3) NULL,
  `c_ipv4` ipv4 NULL,
  `c_ipv6` ipv6 NULL,
  `c_datev1` date NULL,
  `c_datetimev1` datetime NULL,
  `c_decv2` decimal(27,9) NULL
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
"disable_auto_compaction" = "true",
"binlog.enable" = "false",
"binlog.ttl_seconds" = "86400",
"binlog.max_bytes" = "9223372036854775807",
"binlog.max_history_nums" = "9223372036854775807",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);
