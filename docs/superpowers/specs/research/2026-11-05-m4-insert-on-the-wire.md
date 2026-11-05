# M4 research: the INSERT / stream-load write loop on the wire

Sources: `.cache/doris-src` (4.1.5-release), paths relative to it. Corpus: `oracle/epic1/traffic/*.bin.gz`,
decoded with a throwaway Rust tool built against `doris-wire`/`doris-thrift`/`doris-proto`:
`<scratchpad>/m4tool` (modes `dump <suite.bin.gz> [idx,..]`, `scan`, `values`). Pretty printer:
`<scratchpad>/pp.py`. Full decoded example: `<scratchpad>/having.txt` (INSERT) and `<scratchpad>/sl.txt`
(stream load). (The scratch tools were throwaway and are not kept.)

Corpus totals: 702 of 1456 suites send at least one fragment containing `OLAP_TABLE_SINK` (7907 plan
requests). **652 suites hold "pure VALUES" inserts** (6267 requests): exactly one fragment whose plan is
a single `UNION_NODE` with no children (no scan, no exchange). Insert-select makes up the rest.

---

## 1. How an INSERT VALUES arrives

### 1.1 RPC sequence (worked example: `correctness_p0.test_constant_having`)

SQL: `CREATE TABLE test_constant_having_t0(c0 VARCHAR(19) NOT NULL) DISTRIBUTED BY HASH(c0)` (10 buckets,
duplicate key by default) then `INSERT INTO test_constant_having_t0 (c0) VALUES (1),(2);`.

| # | dir | message |
|---|-----|---------|
| 23/22 | FE→BE brpc 8060 | `PBackendService.exec_plan_fragment` req 1517 B / resp 25 B |
| 24/25 | BE→FE thrift 9020 | `FrontendService.reportExecStatus` call 540 B / reply 44 B (`TReportExecStatusResult{status OK}`) |
| 27/26 | FE→BE brpc | `PBackendService.cancel_plan_fragment` (status FINISHED=76 "Finished") / resp `{status 0}` (4 B) |
| 30/31 | FE→BE thrift 9060 | `BackendService.submit_tasks` PUBLISH_VERSION txn 7149, partition version 2 |
| 28/29 | BE→FE thrift | `FrontendService.finishTask` PUBLISH_VERSION (succ_tablets, delta rows) |

(Records in one ms are not ordered reliably; the publish `submit_tasks` precedes its `finishTask`.)
The capture has no `begin`/txn RPC to the BE: the FE begins the txn itself and puts `txn_id` in the sink.

### 1.2 Which brpc method

* `proto`: `gensrc/proto/internal_service.proto:1233-1237`: "If #fragments of a query is < 3, use
  exec_plan_fragment directly. If >=3, use exec_plan_fragment_prepare + exec_plan_fragment_start".
* Corpus: every one of the 6267 VALUES inserts uses **`exec_plan_fragment`** (1 fragment). Of all 7907
  sink requests, 6467 use `exec_plan_fragment` and 1440 use `exec_plan_fragment_prepare` (INSERT…SELECT
  with 2-9 fragments, followed by `exec_plan_fragment_start{query_id}`).
* The `PExecPlanFragmentRequest` (`internal_service.proto:288-292`) has `request` (bytes) = 1,
  `compact` (bool) = 2 and `version` (enum) = 3. In all 7907 requests, `compact=true` and `version=3` (VERSION_3, a
  `TPipelineFragmentParamsList`). FE: `fe/fe-core/.../rpc/BackendServiceProxy.java:177-203`
  (`TCompactProtocol` when `Config.use_compact_thrift_rpc`, `setVersion(VERSION_3)`).
* BE: `be/src/service/internal_service.cpp:343-388` (`exec_plan_fragment` → `_exec_plan_fragment_in_pthread`),
  `:565-625` (`_exec_plan_fragment_impl`: CHECKs version==3, `deserialize_thrift_msg(buf,&len,compact,..)`
  from `util/thrift_util.h:131`, errors when `params_list` is empty, then `fragment_mgr()->exec_plan_fragment`
  for each fragment). `exec_plan_fragment_start` is at `:406-427`.
* doris-rs already decodes this: `crates/doris-wire/src/roundtrip.rs:89-102` (`carries_plan`,
  `fragment_plan`) and `needs.rs:97-103`: `TPipelineFragmentParamsList::read_from_in_protocol(TCompactInputProtocol)`.

### 1.3 Reply: `PExecPlanFragmentResult` (`internal_service.proto:298-305`)

Captured (25 bytes): `{status: PStatus{status_code:0, error_msgs:[]}, received_time: 1793679296274,
execution_time: …274, execution_done_time: …274}` (all in ms since the epoch). The reply comes back
**after prepare, before execution finishes**. Execution is async, and its completion goes to the FE via
`reportExecStatus` (report at t+6 ms here). On a prepare failure the BE sets `status` to the error and
sends no report. Today `crates/doris-be/src/brpc.rs:32-53` answers every method with
NOT_IMPLEMENTED_ERROR (3).

### 1.4 Decoded `TPipelineFragmentParamsList` (key fields)

List level: `params_list=[1 fragment]`, `runtime_filter_info{merge_addr 127.0.0.1:8060}`. Nothing else
is set at list level here (`desc_tbl`/`query_options`/`coord` are per fragment in this FE).

`TPipelineFragmentParams` (gen: `crates/doris-thrift/src/gen/palo_internal_service.rs:5517`):
* `protocol_version=0`, `query_id{hi,lo}`, `fragment_id=0`, `per_exch_num_senders={}`, `destinations=[]`, `num_senders=1`
* `desc_tbl`: three tuples. **Tuple 0** is the union output: slot 0 VARCHAR(19), `primitive_type=INVALID_TYPE`. **Tuple 1** is the projection
  output: slot 1. **Tuple 2** is the sink destination: slot 2 VARCHAR(19), `col_unique_id=0`, `is_key=true`,
  `primitive_type=VARCHAR`, `table_id=-1618351866`. `table_descriptors=[{id=<table_id>, OLAP_TABLE,
  num_cols 1, table_name}]`. Nullability is expressed by `null_indicator_bit` (-1 = not nullable).
* `coord = 127.0.0.1:9020`: **reportExecStatus goes to this address** (and `current_connect_fe` is the same here).
* `query_globals{now_string "2026-11-03 04:14:56", timestamp_ms, time_zone "UTC", load_zero_tolerance true, nano_seconds, lc_time_names}`
* `query_options` that matter: `query_type=LOAD(1)` (it is echoed in the report),
  `is_report_success=true` (makes it a load and enables periodic reports), `query_timeout/execution_timeout=14400`,
  `batch_size=8160`, `be_exec_version=13`, `enable_insert_strict=true` (5977/6267; false in 290),
  `enable_strict_cast=false` (6101; true in 166), `enable_memtable_on_sink_node=true` (6229; false in 38),
  `truncate_char_or_varchar_columns=false`, `mem_limit`. `max_errors` is unset (the BE defaults to 100,
  `runtime/runtime_state.cpp:279-282`).
* `fragment_num_on_host=1`, `backend_id=<this BE id>`, `need_wait_execution_trigger=false`, `is_nereids=true`,
  `load_stream_per_node=2`, `total_load_streams=2`, `num_local_sink=1`, `total_instances=1`,
  `workload_groups=[{id}]`, `resource_info{user root}`.
* `local_params=[TPipelineInstanceParams{fragment_instance_id = {query.hi, query.lo+1}, per_node_scan_ranges={}, sender_id=0, backend_num=0}]`
  (one instance in every VALUES insert).
* `txn_conf` and `import_label` are unset (they are only for txn-insert and group commit).

`TPlanFragment`:
* `plan.nodes=[TPlanNode{node_id 0, UNION_NODE, num_children 0, limit -1, row_tuples [0], nullable_tuples [false],
  union_node: TUnionNode{tuple_id 0, result_expr_lists [], const_expr_lists [[TExpr],[TExpr]] (one list per row,
  one TExpr per column), first_materialized_child_idx 0}, projections [TExpr SLOT_REF(slot0,tuple0)],
  output_tuple_id 1, nereids_id 14}]`.
  Each row here is `STRING_LITERAL{value "1"}` with type VARCHAR(65533) and `is_nullable=false`: the FE
  already folded the int→varchar cast.
* `output_exprs=[SLOT_REF(slot 1, tuple 1, label "c0")]`. These feed the sink and give the column order of the
  destination tuple.
* `output_sink: TDataSink{type OLAP_TABLE_SINK, olap_table_sink: TOlapTableSink{…}}` and `partition{UNPARTITIONED}`.

`TOlapTableSink` (gen `data_sinks.rs:2779`):
* `load_id` = query_id, `txn_id=7149`, `db_id`, `table_id`, `tuple_id=2`, `num_replicas=1`, `need_gen_rollup=false`
* `schema: TOlapTableSchemaParam{db_id, table_id, version 0, slot_descs [dest slots with col_name, col_unique_id, is_key],
  tuple_desc{id 2}, indexes [TOlapTableIndexSchema{id=<index_id>, columns ["c0"], schema_hash 1402679966,
  columns_desc [TColumn{column_name, column_type{VARCHAR len 19 index_len 20}, aggregation_type NONE, is_key,
  is_allow_null false, visible, col_unique_id 0, cluster_key_id -1 …}], indexes_desc []}], is_partial_update false,
  is_strict_mode false, inverted_index_file_storage_format V3, unique_key_update_mode UPSERT}`
* `partition: TOlapTablePartitionParam{db_id, table_id, version 0, distributed_columns ["c0"],
  partitions [TOlapTablePartition{id=<partition_id>, num_buckets 10, indexes [{index_id, tablets [10 ids, bucket order]}],
  is_mutable true, total_replica_num 1, load_required_replica_num 1}], enable_automatic_partition false,
  partition_type UNPARTITIONED(0)}`. The keys of RANGE(3)/LIST(4) partitions arrive as literal TExprNodes
  (DATE_LITERAL/INT_LITERAL in the sink: 3489 date and 3979 int nodes across the corpus).
* `location: TOlapTableLocationParam{tablets [TTabletLocation{tablet_id, node_ids [backend_id]}…]}`
* `nodes_info: TPaloNodesInfo{nodes [TNodeInfo{id=<be id>, host 127.0.0.1, async_internal_port 8060}]}`
* `load_channel_timeout_s 14400`, `send_batch_parallelism 1`, `load_to_single_tablet false`,
  `write_single_replica false`, `txn_timeout_s 14400`, `write_file_cache true`, `base_schema_version 0`.
  `max_filter_ratio` and `group_commit_mode` are unset in all VALUES inserts.

Sink features across the 6267 VALUES inserts: 318 have more than one partition (RANGE 320, LIST 29); 8 use
auto partition; 99 use partial update (unique-key MoW); 100 have 2-3 indexes (rollup/sync MV); 141 have
empty `distributed_columns` (RANDOM distribution → `load_tablet_idx`).

### 1.5 Row → tablet routing (must match C++ for later reads and publish row counts)

`be/src/storage/tablet_info.h:236-270` (`find_tablets`): for each distributed column, in order,
`hash = RawValue::zlib_crc32(bytes, len, type, hash)` with seed 0, NULL → `HashUtil::zlib_crc_hash_null`
(`util/hash_util.hpp:120`). The tablet is `tablets[hash % num_buckets]`. Per-type bytes are listed in
`util/raw_value.h:47-110`: strings use raw bytes; INT uses 4 LE bytes; DATEV2 uses the 4-byte packed value;
DATETIMEV2 uses 8 bytes; legacy DATE/DATETIME use the text form; DECIMALV2 uses int+frac. Random distribution
uses `load_tablet_idx % num_buckets`. **Verified on the corpus**: zlib `crc32("1")%10=3` and
`crc32("2")%10=7`, and the publish `finishTask` reports delta rows exactly on tablets[3] (…870) and
tablets[7] (…878).

---

## 2. reportExecStatus for that INSERT

### 2.1 Decoded call (#24), `TReportExecStatusParams`

```
protocol_version 0, query_id {hi,lo} (= load_id), backend_num -1, fragment_instance_id {0,0},
fragment_id 0, status TStatus{OK}, done true, query_type LOAD(1), backend_id <be id>,
loaded_rows 0, loaded_bytes 0,                       # fragment-level state: zero
load_counters {"dpp.abnorm.ALL":"0","dpp.norm.ALL":"2","unselected.rows":"0"},
fragment_instance_reports [{fragment_instance_id {hi,lo+1}, num_finished_range 0, loaded_rows 2, loaded_bytes 10}],
commit_infos [10 × TTabletCommitInfo{tablet_id, backend_id}]   # all 10 tablets, though only 2 got rows
```

Not set: profile, error_log, delta_urls, tracking_url, first_error_msg, txn_id/label (group commit/WAL
only), error_tablet_infos. Reply: `TReportExecStatusResult{status OK}`.

### 2.2 How the BE builds it (C++)

* `be/src/exec/pipeline/pipeline_fragment_context.cpp:2248-2298` `send_report(done)`: for a non-load
  query (`!_is_report_success`) it reports only errors. It always uses `fragment_instance_id=TUniqueId()`
  and `backend_num=-1`, and sets `done = done || !status.ok()`.
* `:1881-1890` `_close_fragment_instance` → `send_report(true)` once all tasks finish: **one final report**.
* `:608-680` periodic reports (`config::pipeline_status_report_interval`=10 s, `common/config.cpp:318`)
  carry `done=false` and no counters or commit infos. The corpus has 646 of them on VALUES queries (long
  loads only). The FE ignores them for commit (`SingleFragmentPipelineTask.java:61-67`). **doris-rs can skip them.**
* `:1992-2240` `_coordinator_callback`: field filling. `loaded_rows/bytes` come from the fragment state
  (0 for a pipeline). `load_counters` and `fragment_instance_reports` are set when any task state has
  rows, filtered rows or finished ranges. The three dpp counters are always emplaced. `tracking_url`/`first_error_msg`
  are set when non-empty. `commitInfos` comes from `runtime_state->tablet_commit_infos()` (filled by the sink at close:
  `exec/sink/writer/vtablet_writer_v2.cpp:778-780`, `_create_commit_info` `:1076-1094`). `errorTabletInfos` is
  the same, and `backend_id` is set when known. The BE retries once on transport error and cancels the
  query if the RPC fails.

Corpus stats over all 7907 sink queries' reports: `reports_per_query {1: 5796, 2: 1782, 3: 279, …}`
(2+ = multi-fragment insert-select, one final report per fragment, plus periodic ones). Final
statuses: OK 10363, CANCELLED 5, INVALID_ARGUMENT 6, INTERNAL_ERROR 10.

### 2.3 Which tablets get commit infos

`vtablet_writer_v2.cpp:1049-1073` `_calc_tablets_to_commit`: **every tablet of every index of every
partition that received at least 1 row** (`_tablet_finder->partition_ids()`). Untouched partitions get
nothing. `load/channel/load_stream.cpp:409-445` `IndexStream::close` opens a tablet stream even for
zero-row tablets, so they get an (empty) committed rowset and a version bump at publish. Corpus check
(VALUES, OK, done): single partition → commit infos == all location tablets in 5919/5935; multi-partition
→ subset in 196 and all in 114; 0 commit infos when every row was filtered ("no partition").

### 2.4 FE side: commit

* `fe/.../qe/runtime/LoadProcessor.java:165-260` `doProcessReportExecStatus` runs only when
  `done=true`. It accumulates `load_counters`, `tracking_url`, `first_error_msg`, `commit_infos` and
  `error_tablet_infos`, then counts down the latch keyed **(fragment_id, backend_id)**, so both must be right.
* `nereids/.../insert/AbstractInsertExecutor.java:207-252`: waits on `coordinator.join(timeout)`. A non-OK
  exec status → `ERR_FAILED_WHEN_INSERT` with the BE's message. Then
  `loadedRows = load_counters["dpp.norm.ALL"]` and `filteredRows = load_counters["dpp.abnorm.ALL"]`.
  `checkStrictModeAndFilterRatio`: strict → fail if filtered > 0; non-strict → fail if
  filtered > insert_max_filter_ratio × total.
* `OlapInsertExecutor.java:219-240` `onComplete`: `commitAndPublishTransaction(db, [table], txnId,
  TabletCommitInfo.fromThrift(commitInfos), insert_visible_timeout_ms, attachment)`. On error it
  aborts the txn, and `setErrorState` (`:315-327`) appends `first_error_msg` and the `url` to the client error.
* `transaction/DatabaseTransactionMgr.java:468-660` `checkCommitStatus`: the partitions involved are
  derived from the commit infos' tablets. **Every tablet of each involved partition/index needs ≥
  load_required_replica_num commit infos from its replica BEs**, or the commit fails with "Failed to commit txn …
  succ replica num". With 0 commit infos (all rows filtered) there is nothing to commit.
* After the commit the FE sends `cancel_plan_fragment{query_id, cancel_status{76 FINISHED,"Finished"},
  cancel_reason InternalError, finst_id 0/0}`. The BE answers `{status{0}}`
  (`be/src/service/internal_service.cpp:627-674`). This is in 693/702 suites, so it must be accepted (and is a no-op).
* Then `submit_tasks[PUBLISH_VERSION{transaction_id, partition_version_infos [{partition_id, version 2,
  version_hash 0}], base_tablet_ids {10 ids}}]` (signature = txn id). The BE answers with `finishTask{PUBLISH_VERSION,
  signature txn, task_status OK, report_version, error_tablet_ids [], succ_tablets {tablet: 0 …all 10},
  table_id_to_tablet_id_to_delta_num_rows {table: {…870:1, …878:1}}}` (only tablets with rows)
  (`be/src/agent/task_worker_pool.cpp:2120-2241`, `storage/task/engine_publish_version_task.cpp:274-299,393`).
  PUBLISH_VERSION appears in 765 suites.

---

## 3. Other brpc around an INSERT; what the C++ BE does internally

FE↔BE methods present in suites with sink fragments: submit_tasks/finishTask (702), reportExecStatus
(700), exec_plan_fragment (698), cancel_plan_fragment (693), fetch_data (660, the SELECTs), prepare/start
(602, multi-fragment), update_cache (141), streamLoadPut/loadTxnBegin/loadTxnCommit + http.stream_load
(~87), get_wal_queue_size (36, schema-change waits on WAL), fetch_cache (26).
**No `tablet_writer_open`/`tablet_writer_add_block`/`open_load_stream` appears**, because those are
BE→BE (here BE→itself) and the capture is FE↔BE only.

C++ internals (single BE, single replica):
* Sink choice: `exec/pipeline/pipeline_fragment_context.cpp:1092-1105`. `enable_memtable_on_sink_node`
  (99% here), with no inverted-index-v1 or partial update and not cloud mode → `OlapTableSinkV2OperatorX`
  (VTabletWriterV2: memtable + segment writing in the sink, `open_load_stream` brpc streams to the
  replica BEs, `LoadStream`/`TabletStream` on the receiver, `load/channel/load_stream.cpp`). Otherwise
  `OlapTableSinkOperatorX` (VTabletWriter: `tablet_writer_open/add_block` to `LoadChannelMgr` →
  `TabletsChannel` → `DeltaWriter`, `internal_service.cpp:320,513`).
* Block path: union source evaluates `const_expr_lists` (`exec/operator/union_source_operator.cpp`) →
  projections → sink `output_exprs` → `OlapTableBlockConvertor::validate_and_convert_block`
  (`exec/sink/vtablet_block_convertor.cpp:70-113`: auto-inc fill, `_validate_data`, filter map) →
  partition/tablet finder (`vtablet_finder.cpp`, `tablet_info.h`) → per-tablet memtable → segment →
  rowset written under `txn_id`/`load_id` → `TxnManager::commit_txn` saves the COMMITTED rowset meta to
  OlapMeta (`storage/txn/txn_manager.cpp:279-379`, key `rst_<tablet_uid>_<rowset_id>`,
  `storage/rowset/rowset_meta_manager.cpp:45`). PUBLISH later gives the rowset version [v,v] and adds it to
  the tablet meta (`txn_manager.cpp:459-581`).

**What doris-rs can short-circuit** (single BE, replica on self, every `node_ids` == own backend_id):
no load streams, no load channels, no brpc to self. Evaluate the rows, validate them, route them to
tablets, and write one rowset (one segment) per touched tablet, plus empty rowsets for the zero-row tablets of
touched partitions. Record a pending (committed-txn) rowset per tablet, then emit commit infos for all of them
in the final report. Memtable sort/merge for DUP_KEYS is just a sort by key columns (stable).

---

## 4. Expression subset for UNION_NODE constant rows

Scan of the 6267 VALUES inserts (`m4tool values`). **Root node of each const TExpr** (one per cell):
INT_LITERAL 85979, STRING_LITERAL 66324, DATE_LITERAL 17965, NULL_LITERAL 9708, LARGE_INT_LITERAL 7427,
DECIMAL_LITERAL 6223, ARRAY_LITERAL 2434, FLOAT_LITERAL 1780, BOOL_LITERAL 1476, IPV6 552, IPV4 534,
FUNCTION_CALL 151, CAST_EXPR 88, MAP_LITERAL 51, ARITHMETIC_EXPR 1.

The FE folds casts: a literal's type already equals the union slot's type. The only mismatch is
`STRING_LITERAL` VARCHAR(65533) into a STRING slot (41459 cells), and both have the same column representation.

Literal kind : type (counts) and how each is encoded:
* `INT_LITERAL` TINYINT 5179, SMALLINT 4673, INT 61847, BIGINT 14280 → `int_literal.value: i64`
* `LARGE_INT_LITERAL` LARGEINT 7427 → `large_int_literal.value: String` ("1")
* `STRING_LITERAL` VARCHAR(65533) 66324 (serves CHAR/VARCHAR/STRING) → `string_literal.value`
* `BOOL_LITERAL` BOOLEAN 1476 → `bool_literal.value`
* `FLOAT_LITERAL` DOUBLE 1259, FLOAT 521 → `float_literal.value: f64` (6.6 for a FLOAT)
* `DECIMAL_LITERAL` DECIMAL32 1025, DECIMAL64 2800, DECIMAL128I 1849, DECIMAL256 277, DECIMALV2 272 →
  `decimal_literal.value: String` already scaled to the type ("2.1000" for (18,4), "1.000000000"),
  precision/scale in `TScalarType`
* `DATE_LITERAL` DATEV2 6127 ("2017-10-01"), DATETIMEV2 9337 ("2025-01-01 00:00:00", scale in
  `TScalarType.scale`), TIMESTAMPTZ 2390 ("0000-01-01 00:00:00+00:00"), DATE 50, DATETIME 61 → `date_literal.value: String`
* `NULL_LITERAL` typed with the target type (INT 1909, BIGINT 1494, STRING 1426, VARCHAR 1036, DATEV2 923,
  BOOLEAN 751, TINYINT 666, LARGEINT 654, DATETIMEV2 318, DOUBLE 82, …)
* IPV4 (`value: i64`), IPV6 (`value: String`); ARRAY/MAP literals have child nodes (nested types are out of M4 scope)

Non-literal const roots (expression shape, count):
`FUNCTION_CALL unhex(STRING_LITERAL)` 131; `CAST_EXPR(array/struct of NULLs)` 34 (nested types);
`CAST_EXPR<DECIMAL128I>(DECIMAL_LITERAL<DECIMAL256>)` 9; `CAST_EXPR<STRING>(BOOL_LITERAL)` 4;
`to_ipv4/to_ipv6(STRING_LITERAL)` 8; `bitmap_empty()` 3; `concat(…char(int)…)` 2; decimal arithmetic
(add/subtract/multiply/divide/mod/greatest/least) ≈10.

**Projections** on the union node: SLOT_REF 15676, INT_LITERAL 6290, FUNCTION_CALL 2181 (**all
`substring(slot, 1, len)`**, signature `substring(text,int,int)`, added by the FE to truncate CHAR/VARCHAR
when `enable_insert_strict=false`), STRING_LITERAL 149, DATE_LITERAL 18, NULL_LITERAL 6, CAST_EXPR 3.
INT_LITERAL projections are constant columns, such as hidden columns like `__DORIS_DELETE_SIGN__`=0 for unique
tables. Sink `output_exprs` are SLOT_REFs.

Sink destination column types across VALUES inserts: VARCHAR 10868, INT 9619, BIGINT 4972, TINYINT 3278,
DATETIMEV2 2997, STRING 2819, DATEV2 1904, LARGEINT 1809, SMALLINT 1245, CHAR 1047, DECIMAL64 1037,
DOUBLE 735, DECIMAL128I 552, DECIMAL32 462, BOOLEAN 419, FLOAT 384, TIMESTAMPTZ 273, DECIMALV2 133,
DECIMAL256 89, DATE 50, DATETIME 51, IPV4/6 ~30 each, BITMAP 3.

**Minimal evaluator for M4**: SLOT_REF; the literals INT, LARGE_INT, STRING, BOOL, FLOAT, DECIMAL, DATE
(DATEV2/DATETIMEV2 plus legacy DATE/DATETIME) and NULL; `substring(str,int,int)` in projections. Optional:
`unhex` and CAST(bool→string). Over all sink plans (including INSERT…SELECT) the corpus also has BINARY_PRED
4849, CAST 630, FUNCTION_CALL 2774, etc. Those belong to the query engine, not M4.

---

## 5. Error paths suites exercise (summary)

The BE does **not** fail the fragment for bad rows. It filters them, counts them, writes an error log, and reports
OK. The FE decides from the counters (`AbstractInsertExecutor.checkStrictModeAndFilterRatio`, §2.4).
Value-out-of-range and bad casts are mostly handled at the FE: with `enable_strict_cast=true` the FE errors
before sending a plan ("can't cast"), and with non-strict casts it folds the bad value to NULL. The BE then
sees a NULL headed for a NOT NULL column. The three BE-side filters seen in the corpus (49 reports with `first_error_msg`):

1. **NULL into NOT NULL column**: `"column_name[dt], null value for not null column, type=DateTimeV2(0)"`
   (`exec/sink/vtablet_block_convertor.cpp:563-578`). Example: `insert_p0.test_insert_docs_demo`:
   counters `{abnorm 1, norm 4}`, commit_infos 10, `fragment_instance_reports[{loaded_rows 5, loaded_bytes 116}]`.
2. **String longer than the schema** (CHAR/VARCHAR len): `"column_name[f2], the length of input is too long than
   schema. first 32 bytes of input str: [123456] schema length: 3; actual length: 6; "` (`:290-315`).
3. **No partition for the row**: `"no partition for this tuple. tuple=[{\"(Nullable(INT))\":\"81\",…}]"`
   (`exec/sink/vtablet_finder.cpp:62`).

On a filtered row the report has status OK, `load_counters["dpp.abnorm.ALL"]` = filtered,
`"dpp.norm.ALL"` = loaded, `first_error_msg = <msg> + ". Src line: "` (`runtime/runtime_state.cpp:386-421`;
at most 50 lines are printed), and `tracking_url = "http://<be_ip>:<http_port>/api/_load_error_log?file=__shard_<n>/error_log_insert_stmt_<query_id>_<inst hi>_<inst lo>"`
(file under the load error path, `runtime_state.cpp:352-380`; the BE HTTP route is `service/http_service.cpp:160`, used
by 3 suites). The FE shows it to the user ("Insert has filtered data in strict mode … url"). Note that
`loaded_rows` in `fragment_instance_reports` counts filtered rows too (10 rows, 3 filtered → loaded_rows 10).

Hard tablet errors, such as partial update without defaults (`E-207`, `E-7003`), report
`status INTERNAL_ERROR(6), error_msgs ["(127.0.0.1)[INTERNAL_ERROR]tablet error: …, host: 127.0.0.1"]`,
`done=true` and no commit_infos. The FE aborts the txn. These are MoW/partial-update cases, outside M4.

---

## 6. Stream load (for completeness; 149 suites, 959 loads)

Example `load_p0.stream_load.test_stream_load_include_where_expr` (DUPLICATE KEY(a), JSON, 1 row):
1. Client `PUT /api/{db}/{table}/_stream_load` to BE http 8040 with headers `label`, `columns`, `format: json`,
   `where`, and `Expect: 100-Continue`. The BE replies `100 Continue`, then `200` with a gzip JSON body
   `{TxnId, Label, Comment, TwoPhaseCommit "false", Status "Success", Message "OK", NumberTotalRows 1,
   NumberLoadedRows 1, NumberFilteredRows 0, NumberUnselectedRows 0, LoadBytes 39, LoadTimeMs, BeginTxnTimeMs,
   StreamLoadPutTimeMs, ReadDataTimeMs, WriteDataTimeMs, ReceiveDataTimeMs, CommitAndPublishTimeMs}`.
2. BE→FE `loadTxnBegin{user root, passwd "", db, tbl, user_ip, label, timestamp, request_id=load_id,
   backend_id, use_table_group_commit_mode true}` → `{status OK, txn_id 18600, db_id}`
   (`be/src/service/http/action/stream_load.cpp:360-369`, `load/stream_load/stream_load_executor.cpp:181`).
3. BE→FE `streamLoadPut{db, tbl, load_id, txn_id, file_type FILE_STREAM, format_type FORMAT_JSON, columns,
   where, merge_type APPEND, read_json_by_line, compress_type PLAIN, backend_id, …}` → `TStreamLoadPutResult
   {status, pipeline_params: TPipelineFragmentParams}` (`stream_load.cpp:797`). The plan is
   `FILE_SCAN_NODE` (with `conjuncts` for `where`) → `OLAP_TABLE_SINK`, and the scan range is `TFileScanRangeParams{FILE_STREAM,
   src_tuple_id 1, dest_tuple_id 0, required_slots, expr_of_dest_slot {CAST_EXPR…}, strict_mode false, load_id}`.
   The BE runs it locally; the FE never sends exec_plan_fragment.
4. BE→FE `loadTxnCommit{user, passwd, db, tbl, user_ip, txn_id, sync true, commit_infos [10 tablets],
   thrift_rpc_timeout_ms 180000, db_id, tbls []}` (`stream_load_executor.cpp:339-353`). The FE publishes
   (PUBLISH_VERSION + finishTask) **before** it replies OK. There is no reportExecStatus for stream loads.
Corpus header stats: format CSV for 901 loads, JSON 50, other 8. `column_separator` is set in 891 (`,` 665, `|` 161);
`group_commit: async_mode` in 468 (that is a different path: WAL + group commit, so defer it); `compress_type GZ` in 154;
`max_filter_ratio` in 114; `strict_mode` in 106; `partial_columns` in 101; `two_phase_commit` in 23. Response statuses:
Success 866, Fail 91. 87 suites with a sink (and 149 overall) use stream load.

---

## What doris-rs must build (M4)

1. **brpc dispatch** in `crates/doris-be/src/brpc.rs`: route by method. Handle `exec_plan_fragment` and
   `_prepare/_start` (decode `PExecPlanFragmentRequest` with compact=true and version=3 → `TPipelineFragmentParamsList`,
   reply `PExecPlanFragmentResult{status, received/execution/execution_done_time}` right away and run async).
   Accept `cancel_plan_fragment` (OK, cancel if running; FINISHED is a no-op). Every other method keeps NOT_IMPLEMENTED.
2. **Fragment registry and executor** for the VALUES shape: a UNION_NODE with const_expr_lists → projections →
   output_exprs → OLAP_TABLE_SINK, single instance. Reject other shapes with a clear status (via report or reply).
3. **Expression evaluator** (§4): SLOT_REF and typed literals (INT/LARGEINT/STRING/BOOL/FLOAT/DECIMAL/DATE*/NULL),
   plus `substring(s,1,n)`. Columnar values per dest type, with nullability from the slot descriptors.
4. **Sink**: validation (null-in-not-null, string length) with filter counting, first_error_msg and an error log
   file plus `/api/_load_error_log`. Partition lookup (unpartitioned, then RANGE/LIST via the literal keys). Bucket
   routing with the zlib-crc32 chain (§1.5). Per-tablet sort by key columns (DUP_KEYS). Segment and rowset write
   tagged with txn_id/load_id. Empty rowsets for the zero-row tablets of touched partitions. Persist
   COMMITTED rowset metas in OlapMeta (C++ key format).
5. **FE client `reportExecStatus`** to `fragment.coord`: one final report with `done=true`, `backend_num=-1`,
   `fragment_instance_id 0/0`, `fragment_id`, `query_type LOAD`, `backend_id`, `loaded_rows/bytes 0`, the three
   `load_counters`, `fragment_instance_reports[{instance id, 0, rows incl. filtered, bytes}]`,
   `commit_infos` (all tablets of touched partitions × all indexes), plus `tracking_url`/`first_error_msg` when
   rows were filtered. Errors → status ≠ OK, done, no commit_infos.
6. **PUBLISH_VERSION agent task**: make the committed rowsets visible at `version` per partition and update
   the tablet meta/max version. Send `finishTask` with `succ_tablets{tablet:0}`, `error_tablet_ids []`, `report_version`,
   and `table_id_to_tablet_id_to_delta_num_rows` (tablets with rows only).
7. (Stream load, possibly M4b) HTTP `PUT …/_stream_load` with 100-continue, loadTxnBegin → streamLoadPut →
   run FILE_SCAN(CSV/JSON on the stream) → sink → loadTxnCommit (sync) → the JSON result. This reuses 3-6 and adds a
   CSV/JSON reader and the `expr_of_dest_slot` CAST exprs.
8. Test oracle: replay the corpus fragments (6267 VALUES requests in 652 suites) through the decoder and
   evaluator. Golden check: routing reproduces `delta_num_rows` per tablet from the publish `finishTask`s,
   and the report fields match the captured `reportExecStatus` (minus ids and timestamps).
