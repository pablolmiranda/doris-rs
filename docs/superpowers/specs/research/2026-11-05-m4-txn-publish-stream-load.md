# M4 research: transaction lifecycle, stream load, BE-side load metadata

Scope: single BE, single replica, duplicate-key tables. INSERT VALUES (fragment ending in
OLAP_TABLE_SINK) and stream load. Paths below are relative to `.cache/doris-src/` (Doris
4.1.5) unless they start with `crates/` or `oracle/`. Corpus examples were decoded from
`oracle/epic1/traffic/*.bin.gz` with a throwaway IDL-driven Thrift decoder (scratchpad
`tdec.py`, `rec.py`; not in the repo).

Corpus caveat: the traffic corpus holds FE<->BE messages only. There are **no**
`tablet_writer_open/add_block/close` or `open_load_stream` messages anywhere in it (the sink
writes to its own BE, and those BE-to-self brpc calls weren't captured). It also has no
`FrontendService.report` or heartbeat messages (they're filtered as background traffic). The
M2 fixtures (`crates/doris-be/tests/fixtures/m2/messages`) do have `report` calls, but only for
empty tablets.

---------------------------------------------------------------------------------------------

## 1. Transaction lifecycle on the BE (single replica)

### 1.1 Write: prepare, then a pending rowset
- `RowsetBuilder::init` (`be/src/storage/rowset_builder.cpp:189-240`):
  - `prepare_txn(partition_id, txn_id, load_id)` (`:185`, which calls `TxnManager::prepare_txn`,
    `be/src/storage/txn/txn_manager.cpp:93-130`). It fails if the tablet is SHUTDOWN. It records
    `(partition_id, txn_id) -> {TabletInfo(tablet_id, tablet_uid) -> TabletTxnInfo(load_id)}` in
    memory only.
  - It builds a `RowsetWriterContext` (`:221-234`): `txn_id`, `load_id`, `rowset_state=PREPARED`,
    `segments_overlap=OVERLAPPING`, `newest_write_timestamp=UnixSeconds()`, `tablet_schema`
    (the request-level schema), `index_id`, `write_type=TYPE_DIRECT`.
  - `Tablet::create_rowset_writer` (`be/src/storage/tablet/tablet.cpp:1895`) sets
    `context.rowset_id = engine.next_rowset_id()`. `_init_context_common_fields` (`:1945-1962`)
    adds `tablet_uid`, `tablet_id`, `partition_id`, `tablet_schema_hash`,
    `rowset_type=preferred_rowset_type` (BETA), `tablet_path`, `data_dir`, `encrypt_algorithm`.
  - `_pending_rs_guard = engine.pending_local_rowsets().add(rowset_id)` (`:236`) keeps path GC
    away from the files while they're being written. This is in memory only.
- `BaseBetaRowsetWriter::init` (`be/src/storage/rowset/beta_rowset_writer.cpp:355-381`) copies
  the context into `RowsetMeta`. For PREPARED/COMMITTED it sets `txn_id` and `load_id` and
  leaves version unset. Otherwise it would set `version` and `newest_write_timestamp`.
- `BetaRowsetWriter::build` (`:975-1015`) calls `_build_rowset_meta` (`:1025-1098`), then sets
  `rowset_state = COMMITTED` (pending) and `tablet_schema`. Section 4 has the field list.

### 1.2 Commit (BE-local), at sink close
- `RowsetBuilder::commit_txn` (`rowset_builder.cpp:321-357`) calls `TxnManager::commit_txn`
  (`txn_manager.cpp:279-457`):
  - It rejects `partition_id<1 || txn_id<1 || tablet_id<1`. If the rowset pb has no
    partition_id, it fills it in.
  - If the same (txn, tablet) is already committed with the same rowset_id, it returns OK
    (idempotent). With a different rowset_id it returns `PUSH_TRANSACTION_ALREADY_EXIST`.
  - **Persisted:** `RowsetMetaManager::save(meta, tablet_uid, rowset_id, rowset_pb)`
    (`:379`). The key is `rst_<tablet_uid>_<rowset_id>` in the `meta` column family
    (`be/src/storage/rowset/rowset_meta_manager.cpp:113-124`, prefix
    `rowset_meta_manager.h:42`). The value is the serialized `RowsetMetaPB` with
    `rowset_state=COMMITTED`, `txn_id`, `load_id`, `partition_id` and the full `tablet_schema`
    (`RowsetMeta::_serialize_to_pb`, `rowset_meta.cpp:256-265`). Nothing else is written. The
    tablet header is not touched.
  - `tablet_uid.to_string()` is `%016x-%016x` of hi/lo (`be/src/util/uid_util.h:90-96`;
    negative values print as two's complement). `rowset_id.to_string()` is the 48-hex v2 id.
  - In memory: `TabletTxnInfo{load_id, rowset, pending_rs_guard}` goes into the txn map, plus
    `txn_partition_map[txn] += partition`.
- The sink then reports `TTabletCommitInfo{tabletId, backendId}` for every tablet it opened.
  For INSERT this goes in `reportExecStatus.params.commitInfos`. For stream load it goes in
  `RuntimeState::tablet_commit_infos()`, which becomes the BE's `loadTxnCommit.commitInfos`
  (section 3).
- **Every tablet of every touched partition/index gets a rowset**, an empty one if no rows
  were routed to it. Decoded corpus example (`test_stream_load_cast`, txn 18612): 1 row loaded
  into a 32-bucket table, and `loadTxnCommit.commitInfos` lists all 32 tablets.
  - This is required. The FE's `DatabaseTransactionMgr.checkCommitStatus`
    (`fe/fe-core/.../transaction/DatabaseTransactionMgr.java:468-640`) derives the involved
    partitions from the commitInfos' tablets. It then requires a successful replica for
    **every** tablet of every index of those partitions (`:560-640`). Omitting the empty
    tablets fails the commit.

Decoded INSERT `reportExecStatus` with commitInfos (`insert_p0.insert.insert`, 6 rows):
```
{'backend_id': 1793677838735, 'backend_num': -1, 'done': True, 'fragment_id': 1,
 'commitInfos': [{'backendId': 1793677838735, 'tabletId': 1793678005533}],
 'fragment_instance_reports': [{'fragment_instance_id': {...}, 'loaded_bytes': 405, 'loaded_rows': 6, 'num_finished_range': 0}],
 'load_counters': {'dpp.abnorm.ALL': '0', 'dpp.norm.ALL': '6', 'unselected.rows': '0'},
 'loaded_bytes': 0, 'loaded_rows': 0, 'query_type': 'LOAD', 'status': {'status_code': 'OK'}, ...}
```

### 1.3 PUBLISH_VERSION
`TPublishVersionRequest` (`gensrc/thrift/AgentService.thrift:478-485`) has these fields:
`1 transaction_id`, `2 partition_version_infos: list<TPartitionVersionInfo{partition_id,
version, version_hash(=0)}>`, `3 strict_mode` (never set in the corpus), and
`4 base_tablet_ids: set<i64>` (base-index tablets, used for delta-row stats).

Corpus statistics (9011 PUBLISH tasks): `signature == transaction_id` every time. 416 had
empty `partition_version_infos` (an INSERT that wrote nothing; the BE answers with empty
maps). 7825 of the submit_tasks calls carried exactly one task.

Decoded stream-load publish (`load_p0.stream_load.test_stream_load_cast`, txn 18612):
```
submit_tasks: [{'protocol_version': 'V1', 'signature': 18612, 'task_type': 'PUBLISH_VERSION',
  'publish_version_req': {'transaction_id': 18612,
     'partition_version_infos': [{'partition_id': 1793678058858, 'version': 2, 'version_hash': 0}],
     'base_tablet_ids': [1793678058923, 1793678058921, ... all 32 tablets]}}]
reply: {'snapshot_version': 1, 'status': {'status_code': 'OK'}}
finishTask: {'backend': {'be_port': 9060, 'brpc_port': 8060, 'host': '127.0.0.1', 'http_port': 8040, 'id': 1793677838735},
  'task_type': 'PUBLISH_VERSION', 'signature': 18612, 'task_status': {'status_code': 'OK'},
  'report_version': 179367784181230, 'error_tablet_ids': [],
  'succ_tablets': {'1793678058861': 0, '1793678058863': 0, ... all 32 -> 0},
  'table_id_to_tablet_id_to_delta_num_rows': {'1793678058859': {'1793678058907': 1}}}
```
INSERT publish (`insert_p0.insert.insert`, txn 9260): publish `{partition 1793678005530,
version 2}`, `base_tablet_ids [1793678005533]`. The finishTask has
`succ_tablets {'1793678005533': 0}` and `delta {'1793678005531': {'1793678005533': 6}}`.
An `UPDATE_VISIBLE_VERSION {'1793678005530': 2}` (signature -1) follows about 24 ms later.

**Worker** `PublishVersionWorkerPool::publish_version_callback`
(`be/src/agent/task_worker_pool.cpp:2114-2244`):
- It runs `EnginePublishVersionTask` with up to 3 retries. `PUBLISH_VERSION_NOT_CONTINUOUS`
  (MoW only) gets requeued.
- On success it may trigger cumulative compaction (`:2192-2222`).
- finishTask fields are set at `:2232-2242`: `task_status`, `backend`, `task_type`,
  `signature`, `report_version` (current `s_report_version`; publish does **not** increment
  it), and three fields **always set even when empty**: `succ_tablets`, `error_tablet_ids`
  and `table_id_to_tablet_id_to_delta_num_rows`.
  - No `finish_tablet_infos`. No `table_id_to_delta_num_rows` (field 18).
  - All 9011 corpus finishTasks have exactly the key set {backend, error_tablet_ids,
    report_version, signature, succ_tablets, table_id_to_tablet_id_to_delta_num_rows,
    task_status, task_type}.
- Task status is OK even with error tablets: 742 corpus finishes have non-empty
  `error_tablet_ids` and status OK, almost all from schema-change suites.

**EnginePublishVersionTask::execute** (`be/src/storage/task/engine_publish_version_task.cpp:98-345`):
1. For each partition: `tablet_manager.get_partition_related_tablets(partition_id)` (`:148`),
   which needs a **partition -> tablets index** (`tablet_manager.cpp:1470-1477`). Then
   `txn_manager.get_txn_related_tablets(txn, partition)` (`:157`).
2. For each (tablet, rowset): a null rowset adds the tablet to error_tablets
   (`PUSH_ROWSET_NOT_FOUND`, `:181`). A missing tablet also goes to error_tablets. The MoW
   continuity check doesn't apply to duplicate keys. If `base_tablet_ids.contains(tablet)`, it
   records `num_rows` for delta stats (`:243`). Then it submits a `TabletPublishTxnTask`.
3. `publish_version_and_add_rowset` (`:422-462`):
   - `TxnManager::publish_txn` (`txn_manager.cpp:459-623`):
     - `rowset->make_visible({v,v})` (`:523`; `rowset.cpp:87-98`) sets `start_version =
       end_version = v`, `rowset_state = VISIBLE` and `creation_time = UnixSeconds()` (the
       visible time).
     - It then **re-saves the same `rst_<uid>_<rowset_id>` key** with the VISIBLE pb
       (`:581`).
     - It removes the txn entry from memory.
   - `tablet->add_inc_rowset(rowset)` (`tablet.cpp:692-711`): `_contains_version` check,
     `tablet_meta.add_rs_meta` (in memory), `_rs_version_map[v]`, version tracker,
     `++_newly_created_rowset_num`. **It does not save the tablet header.**
4. Then, for every tablet related to the partition: if it isn't an error tablet and
   `check_version_exist({v,v})`, it sets `succ_tablets[tablet] = 0`, **always 0** (`:299`).
   Otherwise the tablet goes to error_tablets.
5. `_calculate_tbl_num_delta_rows` (`:384-397`) fills `table_id -> tablet_id -> num_rows`
   only where `num_rows > 0`.

**Header persistence:** the header (`tabletmeta_<id>_<hash>`) is rewritten by
`Tablet::do_tablet_meta_checkpoint` (`tablet.cpp:1473-1530`). That runs when
`_newly_created_rowset_num >= tablet_meta_checkpoint_min_new_rowsets_num (10)` or every
`tablet_meta_checkpoint_min_interval_secs (600)` (`common/config.cpp:868-869`). The rowsets'
`rst_` keys are deleted once the header holds them. Between publish and checkpoint, the
VISIBLE rowset lives only in `rst_`. The `cumulative_layer_point` isn't touched by publish;
it stays -1 until compaction.

FE side (`fe/.../master/MasterImpl.java finishPublishVersion`): it uses `succ_tablets`,
`error_tablet_ids` and `report_version`. Delta rows go only if the status is OK. A non-OK
status leaves the task queued and the FE retries.

### 1.4 CLEAR_TRANSACTION_TASK
- Request: `TClearTransactionTaskRequest{1 transaction_id, 2 partition_id: list}`
  (`AgentService.thrift:512-515`).
- Callback: `clear_transaction_task_callback` (`task_worker_pool.cpp:2246-2278`). If
  `txn_id > 0`, it calls `engine.clear_transaction_task(txn[, partitions])`
  (`storage_engine.cpp:800-836`). An empty list means it looks up the partitions in the txn
  map. Then, for each related tablet, `TxnManager::delete_txn` (`txn_manager.cpp:695-741`):
  - A pending (COMMITTED) rowset has its `rst_` key removed and its files go to the unused
    rowsets for GC.
  - An already VISIBLE rowset gets "TRANSACTION_ALREADY_COMMITTED"; only the memory entry is
    dropped.
- finishTask: only `task_status`, `backend`, `task_type`, `signature` (**no report_version**).
  The status is always OK.
- Decoded example (`datatype_p0...test_timestamptz_stream_load`, after `loadTxnRollback` of
  txn 17510, reason `[DATA_QUALITY_ERROR]too many filtered rows`):
  ```
  task: {'clear_transaction_task_req': {'partition_id': [], 'transaction_id': 17510},
         'protocol_version': 'V1', 'signature': 17510, 'task_type': 'CLEAR_TRANSACTION_TASK'}
  finishTask: {'backend': {...}, 'signature': 17510, 'task_status': {'status_code': 'OK'},
               'task_type': 'CLEAR_TRANSACTION_TASK'}
  ```
- 94 tasks in 21 suites. The FE also sends these for expired txns that the BE reports in
  `TTabletInfo.transaction_ids` (`ReportHandler.java:657,1394-1402`). The BE fills that from
  `TxnManager::build_expire_txn_map` (`pending_data_expire_time_sec=1800`,
  `tablet_manager.cpp:1083-1098`).

---------------------------------------------------------------------------------------------

## 2. Reports after a load

`Tablet::build_tablet_report_info` (`tablet.cpp:1574-1700`):

| TTabletInfo field | value |
|---|---|
| row_count | `TabletMeta::num_rows()`, the sum of `num_rows` over rs_metas (`tablet_meta.h:711`) |
| data_size | `tablet_local_size()`, the sum of `total_disk_size` of local rowsets (`:727`) |
| version | the **max continuous version from 0** (`cversion.second`), not the max version |
| version_hash | 0 |
| version_miss | set to true only if a gap persists for 60 s (consecutive-miss check in the full report) |
| used | false if the max version's rowset is missing, the version crosses, or the dir is unused |
| total_version_count | `tablet_meta.version_count()` (the number of rs_metas) |
| visible_version_count | the number of rs_metas inside `[0, visible_version]` once UPDATE_VISIBLE_VERSION has set a visible version; otherwise equal to total |
| local_index_size | sum of `index_disk_size` |
| local_segment_size | sum of `data_disk_size` |
| path_hash, partition_id, storage_medium, is_in_memory, replica_id, remote_data_size=0 | unchanged |
| transaction_ids | expired txns, set only when there are some |

M2 fixture report entry for an empty tablet (`crates/doris-be/tests/fixtures/m2/messages/thrift/0079-report-call.bin`):
`{data_size 0, row_count 0, version 1, total_version_count 1, visible_version_count 1,
local_index_size 0, local_segment_size 0, ...}`. After one INSERT at version 2 the same tablet
must report `version 2`, `total_version_count 2`, `row_count = rows`, `data_size = total
bytes`, `local_segment_size = data bytes`.

The FE's `ReportHandler.sync` (`fe/.../master/ReportHandler.java:870-935`) adopts the BE's
version when `metaVersion < backendVersion`. If the BE reports a version lower than the FE's
replica version (the FE bumps it on publish), the replica is later treated as
missing-version and cloned. So **the report must reflect published versions.**

What doris-rs does today (`crates/doris-storage/src/tablet_manager.rs:307-365`):
- `report_info` uses `version = max(end_version)`, which is correct only without gaps; the C++
  BE uses max continuous.
- `total_version_count = visible_version_count = rs_metas.len()`.
- `row_count = Σ num_rows`, `data_size = Σ total_disk_size`, `local_segment_size =
  Σ total_disk_size` (C++ uses Σ `data_disk_size`), `local_index_size = 0` (C++ uses Σ
  `index_disk_size`).
- It reads only `t.meta.rs_metas`, so published rowsets must be added to the in-memory meta.
- `data_used` (disk report `data_used_capacity`) = Σ total_disk_size per root. That matches
  the C++ `TabletManager::update_root_path_info` (`tablet_manager.cpp:1451-1468`).
- `report.rs:204-217` always sends `tablet_max_compaction_score: 0`. The C++ BE reports the
  real max score, which becomes > 0 once loads add rowsets. The FE uses it for load
  back-pressure only, so 0 is acceptable.
- `get_tablet_stat` comes from the cache refreshed at each tablet report, like C++
  (`tablet_manager.cpp:1100-1118`). After a load the FE sees new row counts only after the
  next report (or the C++ BE's next stat refresh).
- `agent.rs:140-157`: PUBLISH_VERSION and CLEAR_TRANSACTION_TASK currently return
  "not implemented" via finishTask.
- `backend_service.rs:119-121`: `get_stream_load_record` throws an exception.

---------------------------------------------------------------------------------------------

## 3. Stream load end to end

### 3.1 HTTP request (regression framework)
`regression-test/framework/.../StreamLoadAction.groovy`:
- It sends `PUT http://<fe>:8030/api/{db}/{table}/_stream_load` and expects **307**
  (`:520-532`).
- It then re-sends to the `Location` from the 307 (the BE's `http_port`), expecting 200
  (`:535-550`).
- With `directToBe` it goes straight to the BE.
- It always sends `Authorization: Basic` and `Expect: 100-Continue` (`:444-449`).
- In the corpus every BE request has `Host: 127.0.0.1:8040`. The FE redirect is FE-only
  logic. The BE needs only `PUT /api/{db}/{table}/_stream_load`, registered at
  `be/src/service/http_service.cpp:141`.

Decoded request (`test_stream_load_cast`):
```
PUT /api/regression_test_load_p0_stream_load/test_stream_load_cast/_stream_load HTTP/1.1
Authorization: Basic cm9vdDo=          (root:)
label: 1fd93631-edf9-40a5-a8bc-fd461f92d00d
column_separator: ,
columns: k0,k1,k2,k3,k4
strict_mode: false
Expect: 100-Continue
Content-Length: 24
Host: 127.0.0.1:8040
Connection: Keep-Alive
User-Agent: Apache-HttpClient/4.5.13 (Java/17.0.20.1)
Accept-Encoding: gzip,deflate

3.12,3.12,3.12,3.12,3.12
```
Corpus totals: 959 stream-load requests in 149 suites.
- Formats (from the `format:` header):
  - no header (csv): 709 requests in 108 suites
  - `csv`: 192 in 28 suites
  - `json`: 50 in 17 suites
  - `orc`: 3, `arrow`: 2, `parquet`: 1, `csv_with_names`: 1, `csv_with_names_and_types`: 1
  - **So CSV is ~94%.**
- `group_commit: async_mode` appears on 468 requests, but in only 5 suites
  (`test_group_commit_stream_load_high_concurrency_async` has 400 of them). Non-group-commit
  traffic is ~484 requests.
- Other common headers, by request count:
  - column_separator 891, content-type 475, label 457, columns 281, format 250
  - compress_type 154, transfer-encoding (chunked) 145, max_filter_ratio 114
  - strict_mode 106, partial_columns 101, line_delimiter 67, timeout 50
  - function_column.sequence_col 47, read_json_by_line 43, enclose/escape 38
  - merge_type 35, skip_lines 31, trim_double_quotes 30, two_phase_commit 23
  - strip_outer_array 18, jsonpaths 11
- Responses: 866 Success, 91 Fail, 1 "Publish Timeout". The Fail messages include 28
  `[DATA_QUALITY_ERROR]too many filtered rows` and ~45 MEM_LIMIT ones (reference-side noise).

### 3.2 BE flow (`be/src/service/http/action/stream_load.cpp`)
1. `on_header` (`:210-263`):
   - label = `label` header, or a generated uuid.
   - `two_phase_commit`, `_handle_group_commit`.
   - `_on_header` (`:265-370`) parses basic auth, format/compress (unknown gives
     `DATA_FILE_TYPE_ERROR`), Content-Length vs chunked (exactly one of the two is required),
     the size limits (`streaming_load_max_mb`, `streaming_load_json_max_mb`), `timeout` and
     `comment`.
   - **`begin_txn` (loadTxnBegin) runs before the body is read** (`:360-363`).
   - Then `_process_put`.
   - On error it rolls back if needed and replies with JSON immediately.
2. `StreamLoadExecutor::begin_txn` (`be/src/load/stream_load/stream_load_executor.cpp:181-243`)
   builds `TLoadTxnBeginRequest`. If the reply carries `table_group_commit_mode`
   async/sync, the load switches to group commit. Otherwise it sets `txn_id`, `db_id` and
   `need_rollback = true`.
   ```
   loadTxnBegin call: {'user': 'root', 'passwd': '', 'db': '...', 'tbl': 'test_stream_load_cast',
     'label': '1fd93631-...', 'timestamp': 1793682792312841 (µs), 'request_id': {hi,lo} (= load id),
     'backend_id': 1793677838735, 'use_table_group_commit_mode': True, 'user_ip': '127.0.0.1'}
   reply: {'db_id': 1793678057657, 'status': {'status_code': 'OK'}, 'txnId': 18612}
   ```
3. `_process_put` (`:428-841`):
   - For streaming formats (`LoadUtil::is_format_support_streaming`: csv/json) it creates a
     `StreamLoadPipe` and registers it as `new_load_stream_mgr().put(load_id, ctx)`, with
     `fileType = FILE_STREAM`.
   - Other formats (parquet/orc) are saved to a local file, with `FILE_LOCAL` and `path`.
   - It copies the headers into `TStreamLoadPutRequest` and calls `streamLoadPut` on the
     master FE.
   - It then forces `query_options.enable_strict_cast=false` and
     `enable_insert_strict=strictMode`.
   - It executes `put_result.pipeline_params` locally (`execute_plan_fragment`) **while the
     body is still streaming in** (`on_chunk_data`, `:372-412`, appends 128 KiB buffers to the
     pipe).
   ```
   streamLoadPut call: {'txnId': 18612, 'loadId': {hi,lo}, 'db', 'tbl', 'user', 'passwd', 'user_ip',
     'backend_id', 'fileType': 'FILE_STREAM', 'formatType': 'FORMAT_CSV_PLAIN', 'compress_type': 'PLAIN',
     'columnSeparator': ',', 'columns': 'k0,k1,k2,k3,k4', 'strictMode': False, 'merge_type': 'APPEND',
     'header_type': '', 'negative': False, 'fuzzy_parse': False, 'num_as_string': False,
     'read_json_by_line': True, 'strip_outer_array': False, 'thrift_rpc_timeout_ms': 60000}
   reply (TStreamLoadPutResult): {'status': OK, 'db_id', 'table_id': 1793678058859, 'base_schema_version': 0,
     'pipeline_params': TPipelineFragmentParams{
        'query_id' = 'fragment_instance_id' = load id, 'protocol_version': 'V1', 'num_senders': 1,
        'coord'/'current_connect_fe': 127.0.0.1:9020, 'destinations': [], 'is_mow_table': False,
        'load_stream_per_node': 2, 'total_load_streams': 2, 'num_local_sink': 1, 'table_name',
        'query_globals': {'now_string','timestamp_ms','nano_seconds','time_zone': 'UTC','load_zero_tolerance': True},
        'fragment': {'plan': {'nodes': [{'node_type': 'FILE_SCAN_NODE', 'node_id': 0, 'row_tuples': [0],
                                         'file_scan_node': {'tuple_id': 0, 'table_name': ...}}]},
                     'output_sink': {'type': 'OLAP_TABLE_SINK', 'olap_table_sink': {
                         'load_id', 'txn_id': 18612, 'db_id', 'table_id', 'tuple_id': 2, 'num_replicas': 1,
                         'schema': TOlapTableSchemaParam{slot_descs, indexes, tuple_desc, ...},
                         'partition': {'partition_type': 'UNPARTITIONED', 'distributed_columns': ['k1'],
                             'partitions': [{'id': 1793678058858, 'num_buckets': 32, 'indexes': [{'index_id', 'tablets': [32 ids]}],
                                             'is_mutable': True, 'load_required_replica_num': 1, 'total_replica_num': 1}]},
                         'location': {'tablets': [{'tablet_id', 'node_ids': [be_id]} x32]},
                         'nodes_info': {...}, 'load_channel_timeout_s': 259200, 'txn_timeout_s': 259200,
                         'send_batch_parallelism': 1, 'write_file_cache': True, 'write_single_replica': False}}},
        'local_params': [{'fragment_instance_id': load id, 'sender_id': 0,
           'per_node_scan_ranges': {'0': [{'scan_range': {'ext_scan_range': {'file_scan_range': {
              'params': {'file_type': 'FILE_STREAM', 'format_type': 'FORMAT_CSV_PLAIN', 'compress_type': 'PLAIN',
                 'src_tuple_id': 1, 'dest_tuple_id': 0, 'num_of_columns_from_file': 5, 'strict_mode': False,
                 'load_id', 'required_slots': [{slot_id 12..16, is_file_slot}], 'expr_of_dest_slot', 'default_value_of_src_slot',
                 'dest_sid_to_src_sid_without_trans', 'file_attributes': {'text_params': {column_separator, line_delimiter,
                    enclose 0, escape 0, empty_field_as_null}, 'read_by_column_def': True, 'skip_lines': 0, 'header_type': '',
                    'trim_double_quotes': False, 'enable_text_validate_utf8', 'ignore_csv_redundant_col': False}, 'properties': {}},
              'ranges': [{'load_id', 'size': -1, 'file_size': -1, 'modification_time': 0}]}}}}]}}],
        'query_options': {...}, 'desc_tbl': {...}, 'workload_groups': [...], 'resource_info': {'user': 'root'}}}
   ```
   The file scan finds its byte source by `load_id` in `new_load_stream_mgr`; that's the
   streaming pipe.
4. Fragment done callback (`stream_load_executor.cpp:87-180`):
   - It collects `commit_infos = state->tablet_commit_infos()` and the row counters
     (`num_rows_load_total/success/filtered/unselected`), plus `error_url` and
     `first_error_msg` on failure.
   - **max_filter_ratio:** if `filtered / (total - unselected) > max_filter_ratio`, the status
     becomes `DataQualityError("too many filtered rows")` (`:112-121`).
5. `_handle` (`stream_load.cpp:171-208`):
   - It checks `receive_bytes == body_bytes`, otherwise `NETWORK_ERROR`, then waits for the
     fragment.
   - Then `commit_txn` (or `pre_commit_txn` for 2PC). `commit_txn`
     (`stream_load_executor.cpp:315-374`) sends `loadTxnCommit` with
     `{auth, db, db_id, tbl, txnId, sync: True, commitInfos, thrift_rpc_timeout_ms:
     txn_commit_rpc_timeout_ms (180000), tbls: []}`.
   - **`sync=true`: the FE replies only after publish finishes.** In the corpus: commit call
     at …326, PUBLISH submit at …331, publish finishTask at …333, commit reply OK at …343.
     A `PUBLISH_TIMEOUT` reply status is "committed but not yet visible"; the response
     status is "Publish Timeout" and there is no rollback.
6. On failure, `handle` (`:110-170`) calls `rollback_txn` (`stream_load_executor.cpp:376-410`)
   with `loadTxnRollback {user, passwd, db, db_id, tbl, txnId, reason:
   status.to_string(), tbls: [], label}`. Decoded:
   `{'db_id': 1793678041777, 'label': 'a71f0f53-…', 'reason': '[DATA_QUALITY_ERROR]too many
   filtered rows', 'txnId': 17510, ...}` with reply `{'status': OK}`. The FE then sends
   CLEAR_TRANSACTION_TASK (section 1.4).
7. The reply is `ctx->to_json() + '\n'`, sent with `HttpChannel::send_reply` (gzip when
   `Accept-Encoding` contains gzip, `http_channel.cpp:58-68,137-162`). Status is **always
   HTTP 200**. In the corpus `Content-Type: text/html; charset=ISO-8859-1` (libevent default),
   `Content-Encoding: gzip` on 907 of 959. The `HTTP/1.1 100 Continue` comes from libevent
   before the body.

### 3.3 Response JSON (`StreamLoadContext::to_json`, `be/src/load/stream_load/stream_load_context.cpp:37-132`)
rapidjson `PrettyWriter`: 4-space indent, `"Key": value`, a newline is appended. Key order:
`TxnId` (int), `Label`, `Comment`, then either `TwoPhaseCommit` (**string** "true"/"false"),
or for group commit `GroupCommit` (bool), `GroupCommitMode`, `LoadId`. Then:
- `Status`: "Success" / "Publish Timeout" / "Label Already Exists" (followed by the
  `ExistingJobStatus` key) / "Fail"
- `Message`: "OK", or `status.to_string_no_stack()`, e.g. `[DATA_QUALITY_ERROR]too many
  filtered rows`
- `NumberTotalRows`, `NumberLoadedRows`, `NumberFilteredRows`, `NumberUnselectedRows`,
  `LoadBytes` (receive_bytes)
- `LoadTimeMs`, then `BeginTxnTimeMs` (non-group-commit only), `StreamLoadPutTimeMs`,
  `ReadDataTimeMs`, `WriteDataTimeMs`, `ReceiveDataTimeMs`, then `CommitAndPublishTimeMs`
  (non-group-commit only)
- then `ErrorURL` and `FirstErrorMsg`, each only if non-empty.

Real response bodies (decoded, gunzipped):
```
{
    "TxnId": 18612,
    "Label": "1fd93631-edf9-40a5-a8bc-fd461f92d00d",
    "Comment": "",
    "TwoPhaseCommit": "false",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 1,
    "NumberLoadedRows": 1,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 24,
    "LoadTimeMs": 31,
    "BeginTxnTimeMs": 0,
    "StreamLoadPutTimeMs": 1,
    "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 12,
    "ReceiveDataTimeMs": 2,
    "CommitAndPublishTimeMs": 16
}
```
```
{   ... "TxnId": 18620, "Status": "Fail", "Message": "[DATA_QUALITY_ERROR]too many filtered rows",
    "NumberTotalRows": 1, "NumberLoadedRows": 0, "NumberFilteredRows": 1, "NumberUnselectedRows": 0,
    "LoadBytes": 117, "LoadTimeMs": 11, "BeginTxnTimeMs": 0, "StreamLoadPutTimeMs": 1, "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 9, "ReceiveDataTimeMs": 2, "CommitAndPublishTimeMs": 0,
    "ErrorURL": "http://127.0.0.1:8040/api/_load_error_log?file=__shard_123/error_log_insert_stmt_fa465a6a78d711ce-b4ab60c3f83d94ac_fa465a6a78d711ce_b4ab60c3f83d94ac",
    "FirstErrorMsg": "column(k0) value is incorrect while strict mode is true, src value is \"3.12\". Src line: ..." }
```
Note that NumberLoadedRows is reset to 0 on failure (`stream_load_executor.cpp:132`). The
framework's check (`StreamLoadAction.groovy:556-600`) requires `Status == Success` and
`NumberTotalRows == NumberLoadedRows`. "Publish Timeout" is re-polled. If the body has an
`ErrorURL`, the framework GETs it (`/api/_load_error_log`, 4 corpus calls).

Key sets seen in the corpus (non-group-commit): 433 bodies with the base set; 30 with
`ErrorURL`+`FirstErrorMsg`; 21 with `ErrorURL` only (filtered rows within max_filter_ratio:
the load succeeds and still has an ErrorURL).

### 3.4 get_stream_load_record
- The record is saved after the reply when `enable_stream_load_record` is set. The default is
  false, but the oracle sets it true: `oracle/compose/reference-single.yml:48-49`,
  `oracle/rust/be.conf:16-17`, `stream_load_record_batch_size=500`.
- `StreamLoadAction::_save_stream_load_record` (`stream_load.cpp:861-877`):
  - key = `"<start_millis+load_cost_millis>_<label>"`
  - value = the to_json string with `cluster`, `Db`, `Table`, `User`, `ClientIp`, `Comment`,
    `StartTime`, `FinishTime` added (`stream_load_context.cpp:134-190`)
  - It goes into `<root0>/stream_load` RocksDB, a DBWithTTL with
    `stream_load_record_expire_time_secs=28800`.
- It's skipped when the `skip_record_to_audit_log_table` header is present.
- `BackendService.get_stream_load_record(last_stream_record_time)` (`backend_service.cpp:1155-1178`):
  `get_batch(start=to_string(last_time), batch_size)` returns
  `TStreamLoadRecordResult{stream_load_record: map<key, TStreamLoadRecord>}`
  (`BackendService.thrift:96-121`). An empty map is valid.
- 19 calls in 17 suites. Decoded:
  ```
  call: {'last_stream_record_time': 1793679029221}
  reply: {'stream_load_record': {'1793679202694_5c891fab-294f-449d-a03a-7b5a41037d07': {
     'label': '5c891fab-…', 'db': 'regression_test_compaction', 'tbl': 'base_compaction_uniq_keys_no_value',
     'user': 'root', 'passwd': '', 'user_ip': '127.0.0.1', 'status': 'Success', 'message': 'OK',
     'url': 'N/A', 'first_error_msg': 'N/A', 'total_rows': 3000607, 'loaded_rows': 3000607,
     'filtered_rows': 0, 'unselected_rows': 0, 'load_bytes': 110852291,
     'start_time': 1793679194560, 'finish_time': 1793679202694, 'comment': ''}, ...}}
  ```

---------------------------------------------------------------------------------------------

## 4. On-disk layout of a loaded rowset (C++ BE)

- **Directory:** `<root>/data/<shard_id>/<tablet_id>/<schema_hash>/`. This is the same tablet
  path as M2 (`crates/doris-be/tests/fixtures/m2/layout.txt`, e.g.
  `$ROOT/data/0/1793832160251/871417280`).
- **Segment file:** `<rowset_id>_<segment_id>.dat` (`be/src/storage/olap_define.h:103-105`).
  Inverted index files are `<rowset_id>_<seg>[_<index_id>].idx`. Segcompaction temp files are
  `<rowset_id>_<b>-<e>.dat`.
- `extract_rowset_id` (`olap_common.h:567-590`) parses the prefix before the first `_`.
- An empty rowset (0 rows) has **no files**: `num_segments=0`, `empty=true`.
- **Rowset id v2** (`olap_common.h:490-560`; `unique_rowset_id_generator.cpp`):
  - The id is `hi = (2<<56) | inc`, `mi = backend_uid.hi`, `lo = backend_uid.lo`, printed as
    48 lowercase hex characters.
  - `inc` is an in-memory counter that starts at 1 on every process start. It must be below
    2^56 or `init` throws.
  - `backend_uid = UniqueId::gen_uid()` is random on every start
    (`runtime/exec_env_init.cpp:406`).
  - Fixture example: `0200000000000017184a47389ed9321542307793d1731d87` (inc 0x17), with
    RowsetMetaPB `rowset_id` (field 1, required) = 0.
  - doris-rs already generates ids this way: `tablet_manager.rs:195-200`.
- **RowsetMetaPB of a published beta rowset** (`gensrc/proto/olap_file.proto` RowsetMetaPB;
  setters in `beta_rowset_writer.cpp:355-381, 975-1098`, `rowset.cpp:87-98`):
  - `rowset_id=0`, `rowset_id_v2`, `partition_id`, `tablet_id`, `tablet_schema_hash`,
    `tablet_uid{hi,lo}`
  - `txn_id`, `load_id{hi,lo}`: kept after publish
  - `rowset_type=BETA_ROWSET`, `rowset_state=VISIBLE` (COMMITTED before publish)
  - `start_version = end_version = v`: unset while pending
  - `num_rows`, `total_disk_size = data + index`, `data_disk_size` = Σ segment file sizes,
    `index_disk_size` = Σ index sizes (0 without indexes)
  - `empty = (num_rows == 0)`, `num_segments`, `num_segment_rows` (repeated, per segment,
    field 56)
  - `segments_overlap_pb`: starts OVERLAPPING. It's set to NONOVERLAPPING when the segments'
    key bounds don't overlap (`:1060-1065`); with one segment it's NONOVERLAPPING.
  - `segments_key_bounds`: one `KeyBoundsPB{min_key, max_key}` per segment, holding the
    **full encoded key** of the segment's first and last row (`segment_writer.cpp:1238-1239`).
    For non-MoW they're aggregated into one bound when `enable_aggregate_non_mow_key_bounds`
    is true (the default), with `segments_key_bounds_aggregated=true`
    (`rowset_meta.cpp:294-318`). Each key is truncated to
    `segments_key_bounds_truncation_threshold` = 36 bytes, which sets
    `segments_key_bounds_truncated` (`:320-339`; `config.cpp:1822,1828`).
  - `creation_time` = time of make_visible, `newest_write_timestamp` = write start (seconds)
  - `tablet_schema`: the full TabletSchemaPB, always serialized (`rowset_meta.cpp:256-265`)
  - `schema_version`, `has_variant_type_in_schema`: as in the initial rowset
  - Not set locally: `segments_file_size` / `enable_segments_file_size` (cloud only),
    `visible_ts_ms`, `resource_id` (an empty resource_id means `is_local()`)
- Initial-rowset fields from a real header (`m2/headers/1793832160308.json`), for
  comparison:
  `{start_version 0, end_version 1, rowset_state VISIBLE, rowset_type BETA_ROWSET,
  num_rows 0, total/data/index_disk_size 0, empty True, num_segments 0,
  segments_overlap_pb OVERLAP_UNKNOWN, newest_write_timestamp = creation_time,
  segments_key_bounds_aggregated False, segments_key_bounds_truncated False,
  has_variant_type_in_schema False, schema_version 0, tablet_schema {...},
  packed_slice_locations {}}`.
- No fixture holds a rowset with data: all of the M2, cpp-wal and cpp-compacted headers are
  data-free (`crates/doris-rocks/tests/fixtures/README.md`). **A data-bearing fixture needs
  capturing.**

---------------------------------------------------------------------------------------------

## 5. What the C++ BE needs to load a root with doris-rs-written rowsets

`DataDir::load` (`be/src/storage/data_dir.cpp:347-640`):
1. `_check_incompatible_old_format_tablet`: no `hdr_` keys.
2. **Traverse `rst_` keys** (`RowsetMetaManager::traverse_rowset_metas`, prefix `rst_`, key
   `rst_<uuid>_<rowset_id>`):
   - Each value must parse as RowsetMetaPB; an unparsable one is skipped.
   - `partition_id == 0` counts toward `ignore_invalid_partition_id_rowset_num`; over the
     limit is FATAL.
3. **Traverse headers** with `TabletManager::load_tablet_from_meta`
   (`tablet_manager.cpp:879-960`):
   - The header must parse.
   - Its `tablet_id`/`schema_hash` must equal the key's, and `tablet_uid != 0`.
   - SHUTDOWN tablets go to the shutdown list.
   - A RUNNING tablet must have at least one rowset (`max_version().first != -1`, `:948`).
   - `Tablet::init` → `_init_once_action` (`tablet.cpp:281-320`) runs `create_rowset` for
     each rs_meta and each stale rs_meta. Only `rowset_type == BETA_ROWSET` is accepted
     (`rowset_factory.cpp:38-49`). `BetaRowset::init` is a no-op: segment files are **not**
     opened at load, only lazily on read.
   - Any failure is FATAL unless `ignore_load_tablet_failure` is set.
4. For each loaded tablet, `set_tablet_schema_into_rowset_meta` fills in missing rowset
   schemas, re-saving the header if it did.
5. For each `rst_` rowset (`data_dir.cpp:536-620`):
   - The tablet must exist and `rowset_meta.tablet_uid == tablet.tablet_uid` (otherwise
     "invalid rowset").
   - **COMMITTED** rowsets are re-registered in the TxnManager (`commit_txn(..., is_recovery=
     true)`, `:552-580`). The FE will (re)send PUBLISH.
   - **VISIBLE** rowsets go through `tablet->add_rowset(rowset)` (`:582-597`), which fails on
     version overlap with a different rowset_id (`TabletMeta::add_rs_meta`,
     `tablet_meta.cpp:995-1011`).
   - So a visible rowset can live **either** in the header's `rs_metas` **or** as a VISIBLE
     `rst_` key (or both, with the same id).
6. Later background work:
   - **Path GC** (`DataDir::_perform_rowset_gc`, `data_dir.cpp:704-810`) deletes any
     `*.dat`/`*.idx` in a tablet directory whose rowset_id is neither in the tablet's version
     map nor in an `rst_` key. Every file doris-rs writes must therefore be referenced.
   - **Auto compaction** (unless `disable_auto_compaction`) and the **tablet-meta checkpoint**
     will rewrite rowsets and headers.
   - Reads open segments through the segment reader, so the segment format must be
     bit-compatible (that's for the segment researcher). The row-count consistency check
     between `num_segment_rows` and segment footers is off by default
     (`enable_segment_rows_consistency_check=false`, `beta_rowset.cpp:132`;
     `config.cpp:407`).
7. Rowset ids generated by the C++ BE after restart use a fresh random backend_uid, so they
   don't collide with doris-rs ids. doris-rs should likewise never reuse its uid across
   starts.

Minimum a swap check needs from a doris-rs-written root:
- For each loaded tablet, the header (or a `rst_` VISIBLE key) holds a RowsetMetaPB per
  published version: BETA, VISIBLE, versions contiguous from `[0,1]`, the correct
  `tablet_uid`, `partition_id > 0`, `num_rows`, sizes, `num_segments`,
  `num_segment_rows`, `segments_overlap_pb`, key bounds and `tablet_schema`.
- The `.dat` files exist at `<tablet_path>/<rowset_id_v2>_<i>.dat` for
  `i in 0..num_segments`.
- No dangling COMMITTED `rst_` keys for transactions the FE has finished: they would be
  re-registered and stay until a CLEAR task arrives.

---------------------------------------------------------------------------------------------

## What doris-rs must build

1. **Partition → tablets index** in `TabletManager`, and a per-tablet in-memory version map
   (rs_metas keyed by version) with checks for version existence and contiguity.
2. **TxnManager**:
   - in-memory `(partition, txn) -> {tablet -> (load_id, pending rowset)}`
   - `prepare` at sink open
   - `commit`: writes `rst_<uid hex>-<hex>_<rowset_id>` = RowsetMetaPB(COMMITTED) to the
     `meta` CF, idempotent on the same rowset id
   - `publish` and `delete` (for clear)
   - on start, reload `rst_` keys: COMMITTED go back into the txn map, VISIBLE are added to
     the tablet
   - **Lift the M3 refusal of `rst_`** (`olap_meta.rs:22`, `data_dir.rs:57,253-255`) for
     doris-rs's own roots.
3. **Rowset writer side (with the sink researcher)**:
   - allocate the rowset id (existing `next_rowset_id`)
   - write `<rowset_id>_<n>.dat` into the tablet path
   - build a RowsetMetaPB with the field set in section 4: `num_segment_rows`, key bounds
     aggregated and truncated to 36 bytes, NONOVERLAPPING for one segment, `empty` and zero
     segments for empty rowsets
   - produce an (empty) rowset for **every** tablet the sink opened
   - return `TTabletCommitInfo` for all of them
4. **PUBLISH_VERSION worker**:
   - For each partition and its txn tablets: `make_visible` (start = end = v, VISIBLE,
     `creation_time = now`), re-save `rst_`, add to the tablet's rs_metas, drop the txn entry.
   - Then `succ_tablets[t] = 0` for every partition tablet that has v. Error tablets go to
     `error_tablet_ids`. `table_id_to_tablet_id_to_delta_num_rows` covers base tablets with
     `num_rows > 0`.
   - finishTask always carries all three fields plus `report_version` (no increment), with
     status OK.
   - Handle empty `partition_version_infos` (empty maps) and repeated publishes of the same
     txn (already visible → still succ).
   - Optional: a header checkpoint (save the TabletMetaPB with the new rs_metas and delete
     the `rst_` keys) after 10 rowsets or 600 s, or simply save the header at publish. Both
     are C++-loadable.
5. **CLEAR_TRANSACTION_TASK**: delete pending `rst_` keys and their files for (txn,
   partitions), or for all of the txn's partitions when the list is empty. finishTask has no
   report_version and status OK.
6. **Reports**:
   - `version` = max continuous version
   - `row_count` = Σ num_rows, `data_size` = Σ total_disk_size
   - `local_segment_size` = Σ data_disk_size (fix: today it's total), `local_index_size` =
     Σ index_disk_size
   - `total_version_count` = number of rs_metas; `visible_version_count` counted within
     `[0, partition visible version]`
   - `transaction_ids` for expired pending txns (optional)
   - the disk report's `data_used_capacity` already sums total_disk_size
   - `get_tablet_stat` follows the report cache
7. **Stream load HTTP handler** `PUT /api/{db}/{table}/_stream_load` on the webserver port:
   - honour `Expect: 100-continue` and both Content-Length and chunked bodies (one required,
     not both)
   - Basic auth passthrough
   - label from the header, or a uuid
   - **loadTxnBegin before the body** (with `use_table_group_commit_mode: true`; if the
     reply sets an async/sync group commit mode, that's a separate path, used by 5 suites)
   - streamLoadPut with the mapped headers (csv first: `column_separator`, `columns`,
     `line_delimiter`, `strict_mode`, `max_filter_ratio`, `timeout`, `enclose`/`escape`,
     `skip_lines`, `trim_double_quotes`, `compress_type`; json: `read_json_by_line`,
     `strip_outer_array`, `jsonpaths`, `json_root`, `fuzzy_parse`)
   - execute the returned `TPipelineFragmentParams` (FILE_SCAN_NODE reading a
     `FILE_STREAM` pipe keyed by load_id → OLAP_TABLE_SINK) while streaming the body in
   - apply the max_filter_ratio check
   - loadTxnCommit (`sync: true`, all commitInfos, `thrift_rpc_timeout_ms 180000`, `tbls
     []`), or loadTxnRollback (`reason`, `label`) on failure
   - reply HTTP 200 with the exact pretty JSON (section 3.3; key order, 4-space indent,
     trailing `\n`, `TwoPhaseCommit` as a string, `ErrorURL`/`FirstErrorMsg` only when
     non-empty), gzip if accepted
8. **Load error log**: write the error file and serve `GET /api/_load_error_log?file=...`.
   `ErrorURL` points at the BE's own `http://<host>:<http_port>/api/_load_error_log?file=__shard_N/error_log_insert_stmt_<id>`.
9. **Stream load recorder**:
   - put `"<finish_ms>_<label>"` → record JSON into `<root0>/stream_load` (already opened in
     `tablet_manager.rs:87-95`)
   - implement `get_stream_load_record` (batch of up to `stream_load_record_batch_size`
     after `last_stream_record_time`; empty map OK)
   - the oracle enables recording
10. **Fixture gap**: capture headers, `rst_` values and a `layout.txt` from the C++ BE
    **after** an INSERT and a stream load into a duplicate-key table (both 1-bucket and
    multi-bucket). That gives a byte-level oracle for RowsetMetaPB and the segment files,
    and a swap-check target.
