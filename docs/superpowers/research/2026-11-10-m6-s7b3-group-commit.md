# Group commit (`sink:GROUP_COMMIT_BLOCK_SINK`) in the C++ BE — research notes

Sources (paths under .cache/doris-src):
- be/src/exec/operator/group_commit_block_sink_operator.cpp: `init` l.274-291 (all params come from
  `t_sink.olap_table_sink`), `_initialize_load_queue` l.87-101, `close` l.103-122 (sync wait),
  `_add_block` l.133-170, `_add_blocks` l.206-261, `sink_impl` l.305-419 (filter-ratio check l.320-331)
- be/src/load/group_commit/group_commit_mgr.cpp: `LoadBlockQueue::add_block` l.55-137 (commit
  conditions l.117-131), `get_block` l.139-200 (interval l.152, eos l.186), `add_load_id` l.222-235,
  `GroupCommitTable::get_first_block_load_queue` l.269-323, `_submit_create_group_commit_load`
  l.352-459, `_create_group_commit_load` l.474-592 (label l.489, put request l.491-512),
  `_finish_group_commit_load` l.594-765 (commit l.609-660, rollback l.662-688, notify sync loads
  l.690-707, WAL delete/recover l.719-731), `_exec_plan_fragment` l.767-783
- be/src/exec/operator/group_commit_scan_operator.cpp l.32-59 (the internal plan's scan; a timer wakes
  it after `group_commit_interval_ms`)
- be/src/exec/pipeline/pipeline_fragment_context.cpp l.2104-2108: report carries `txn_id`/`label`
  when the state has a wal id (= the internal txn id)
- be/src/service/http/action/stream_load.cpp l.223, 359-366, 766, 894-975; stream_load_context.cpp
  l.40-125 (reply JSON); stream_load_executor.cpp l.95-99 (label/txn from the state)
- fe: nereids/.../insert/OlapGroupCommitInsertExecutor.java l.176-198 (status PREPARE, label and txnId
  from the coordinator), AbstractInsertExecutor.java l.207-217 (insert timeout), qe/Coordinator.java
  l.2830-2833 (label/txn taken from the report), FrontendServiceImpl.java l.1897-1919 (auth_code skips
  the token check; groupCommit updates load stats)
- thrift: DataSinks.thrift l.40 (`GROUP_COMMIT_BLOCK_SINK = 12`), l.281-285 (`TGroupCommitMode`
  SYNC=0, ASYNC=1, OFF=2), FrontendService.thrift l.627-639 (`TStreamLoadPutResult` 4-9),
  l.328-329 (report `txn_id`, `label`), l.700-702 (commit `groupCommit`, `receiveBytes`, `backendId`)
- traffic decoded with scratchpad/out_insert_p0.*.txt (the existing `dump` tool) and
  scratchpad/b3/tasks (submit_tasks/finishTask types)

## What the FE sends
One fragment, one instance; plan = `UNION_NODE` (VALUES consts, node type 19) or, for a group-commit
stream load, `FILE_SCAN_NODE` (29). Sink `type_ = 12` but the params are in **`olap_table_sink`**
(no separate struct): `load_id` = query id, `txn_id = -1`, db/table ids, `tuple_id`, schema
(`version` = base schema version, indexes, slot_descs incl. hidden `__DORIS_DELETE_SIGN__` /
`__DORIS_VERSION_COL__` for unique tables), partition, location, `group_commit_mode` (0 sync, 1 async),
`max_filter_ratio` (0.0 for INSERT), `base_schema_version`. Output exprs compute the destination row
(e.g. the generated column `abs(a+b)`). `query_options.query_timeout`/`execution_timeout` = the
session timeouts. The FE picks group commit only for VALUES/one-row/inline-table inserts into OLAP
tables with light schema change (OlapGroupCommitInsertExecutor l.108-157); INSERT…SELECT and CTAS
with `group_commit=async_mode` stay ordinary OLAP_TABLE_SINK writes (large_data's gc_ctas part).

## What the BE does (exact RPC sequence)
1. `exec_plan_fragment` answered OK at once (before anything below).
2. Sink open → `get_first_block_load_queue(table_id, base_schema_version, index_size, load_id)`: an
   existing queue of the table that is not `need_commit` and has the same schema version/index count
   takes the load; else a new internal load is created (background pool):
   `FrontendService.streamLoadPut` with exactly: `load_sql = "insert into doris_internal_table_id(<tbl>)
   WITH LABEL <label> select * from group_commit(\"table_id\"=\"<tbl>\")"`, `loadId` = fresh random,
   `label = "group_commit_" + "<hi:016x>_<lo:016x>"` of that load id, `token "group_commit"`,
   `max_filter_ratio 1.0`, `strictMode false`, `partial_update false`, `user "admin"`, `passwd ""`,
   db/tbl `""`, `txn_id 0`, `backend_id`; nothing else set (traffic #5 of timeout).
   Reply: `pipeline_params` = internal plan `GROUP_COMMIT_SCAN_NODE`(33, `table_id`) →
   `OLAP_TABLE_SINK`(5) with the real `txn_id` (also `txn_conf.txn_id`), output exprs = plain SLOT_REFs
   of the tvf columns `c1..cN`; plus `base_schema_version`, `db_id`, `table_id`,
   `wait_internal_group_commit_finish=false`, `group_commit_interval_ms` (table property; default
   10000), `group_commit_data_bytes` (default 134217728).
   The internal fragment runs locally (`QuerySource::GROUP_COMMIT_LOAD`): **no reportExecStatus is ever
   sent for it** (none in any trace).
3. User sink: rows validated/converted and partition-routed (filtered rows counted, error log, max
   filter ratio check at eos when ≤10000 rows), made nullable, appended to the queue (async also writes
   a WAL). `_data_bytes += block.bytes()`; `need_commit` when `data_bytes >= group_commit_data_bytes` or
   `now - queue_start >= interval_ms` (checked on add, on get, and by a timer).
4. Async: the fragment ends immediately → `reportExecStatus` done=true, status OK,
   `load_counters {dpp.norm.ALL: rows, dpp.abnorm.ALL, unselected.rows}`, top-level `loaded_rows 0`,
   `loaded_bytes 0`, `query_type LOAD(1)`, `fragment_id 0`, `backend_num -1`, `fragment_instance_id 0:0`,
   **`txn_id` and `label` of the internal load**, one `fragment_instance_reports` entry (instance id,
   `num_finished_range 0`, `loaded_rows`, `loaded_bytes` = input block bytes, 25 for `(k,v)` + hidden cols,
   13 for `(1,'a',10)`), no commitInfos. FE then sends `cancel_plan_fragment` (status 76 "Finished").
   (C++ also sends an earlier done=false 161-byte report while the plan is being created; FE ignores it.)
   Sync: the sink waits (finish dependency) until the internal load finishes, then reports with the
   queue's status.
5. Internal load: scan returns blocks; eos when queue empty AND `need_commit` AND no load still writing.
   Then `loadTxnCommit`: `txn_id`, `sync false`, `commit_infos` (every tablet written), `auth_code 0`,
   `thrift_rpc_timeout_ms 180000`, `token` = heartbeat auth token, `db_id`, `table_id`,
   `group_commit true`, `receive_bytes`, `backend_id`; user/passwd/db/tbl `""`. On error
   `loadTxnRollback`. FE then publishes: `submit_tasks PUBLISH_VERSION(16)` → finishTask, then
   `UPDATE_VISIBLE_VERSION(33)` — both already handled by doris-rs. Rows become visible only after this.
6. Client sees `{'label':'group_commit_…', 'status':'PREPARE', 'txnId':'…'}` and the row count from
   `dpp.norm.ALL`.

## Recorded sequences (times relative to the first record)
**test_group_commit_interval_ms_property** (UNIQUE MOW table, 8 buckets, interval 5000):
+6 ins1 exec → streamLoadPut label `…e540…` → reply txn 9036 interval 5000 → report done (txn 9036);
+115 ins2 exec → report (9036, same label); [ALTER interval=1000 — FE only, no BE traffic];
+119 ins3 → report (9036 again: the open queue keeps its 5000 ms); +5036 `loadTxnCommit` 9036 (8 tablets,
receive_bytes 87) → publish → visible-version; +6128 ins4 → new streamLoadPut label `…8c46…` → txn 9037
interval 1000 → report (9037). Suite asserts label1==label2, label3!=label4. No BE read at all.
**test_group_commit_data_bytes_property** (UNIQUE MOW, data_bytes 1024): +7 ins1 → put → txn 9251
(interval 10000, data_bytes 1024) → report; +11 ins2 → report 9251; [ALTER data_bytes=1, FE only;
+765 DROP tasks of an older table]; +10032 commit 9251 (interval elapsed); +10041 ins3 → put → txn 9252
(data_bytes 1) → report; +10057 commit 9252 (1 byte reached at once); +10182 ins4 → put → txn 9253 →
report. msg3 != msg4. The 10 s sleep follows ins1, so ins3 always finds the first queue expired.
**test_group_commit_timeout** (DUP, interval 300000, `sync_mode`, query/insert timeout 5 s): +12 exec
(mode 0) → put → txn 8996 → no done report; +5017 report done status CANCELLED
`"(127.0.0.1)[CANCELLED]PStatus: insert timeout"` with counters, txn_id, label, instance report; FE
`cancel_plan_fragment` "insert timeout" (same ms). The FE produces the client error itself
(`AbstractInsertExecutor` l.207-217: cancel with "insert timeout", ERR_EXECUTE_TIMEOUT), so the BE only
has to *not* finish within 5 s; the queued row would commit ~300 s later.
**insert_group_commit_with_large_data**: 5000-row VALUES (456 KB plan) → put → report → commit at
+303 (interval 40) → publish; `select count(*)` 2 s later sees 5000. Generated-column table: 3 inserts
share one queue (commit +2371), qt_select1 3 rows. Stream load `group_commit: async_mode`, no label:
**no loadTxnBegin**; BE `streamLoadPut` with `group_commit_mode "async_mode"`, `txn_id -1`, the usual
header fields → plan `FILE_SCAN_NODE → GROUP_COMMIT_BLOCK_SINK` (mode 1); rows go to a queue (second,
internal `streamLoadPut`) → reply JSON
`{TxnId 9062, Label "group_commit_…", Comment, GroupCommit true, GroupCommitMode "async_mode",
LoadId "<hex>-<hex>", Status, Message, NumberTotalRows 4, …, LoadBytes 31, LoadTimeMs,
StreamLoadPutTimeMs, ReadDataTimeMs, WriteDataTimeMs, ReceiveDataTimeMs}` (no TwoPhaseCommit,
BeginTxnTimeMs, CommitAndPublishTimeMs; stream_load_context.cpp l.53-120). Then CTAS / INSERT…SELECT
under async_mode run as normal writes. Expected .out: select1 `1 2 3.0 / 3 5 8.0 / 6 7 13.0`,
select2 adds `11 21 32.0 … 14 24 38.0`, cte1-3 (regression-test/data/insert_p0/
insert_group_commit_with_large_data.out). The other three suites have no qt_ steps (their .out files are
stale or absent); they pass iff no assertion throws.

## doris-rs today
- brpc.rs l.86 `is_write` → `fragment::is_load_plan` (fragment.rs l.~150) requires OLAP_TABLE_SINK, so
  a type-12 plan goes to the query service, which refuses it; `fragment::table_sink` (l.159-200) refuses
  `group_commit_mode`. `query.rs` only names the sink and GROUP_COMMIT_SCAN_NODE.
- load.rs: `LoadService::exec_plan_fragment` (l.113-178) = plan_write → values_rows → thread →
  `run_sink` → `exec_status` → one report; `cancel_plan_fragment` (l.180) is a no-op. Stream load
  (`StreamLoad::run`, l.346-515) always does loadTxnBegin/put/run_sink/commit and refuses a
  table group-commit mode (l.402); `reply_json` l.650 has the non-group-commit field set.
- sink.rs: `TableSink`/`run_sink`/`commit` (rowsets + `commit_txn`) and `exec_status` (l.323) — reusable
  for the internal load as is; `exec_status` has no label/txn_id.
- fe_client.rs already has `stream_load_put`, `load_txn_commit`, `load_txn_rollback`; MasterInfo has
  `auth_token`; agent.rs handles PUBLISH_VERSION and UPDATE_VISIBLE_VERSION.

## Why the extractor rejects them (extract-replays.rs)
- l.334-343 `no eos fetch`: interval/data_bytes/timeout have **no BE-served query at all** (only FE-side
  `show create table`), so no fetch_data exists; they would also fail `(1..=40).contains(&results)`
  (l.344) with 0 result queries. Group-commit inserts never need a fetch: the outcome is in the report.
- large_data: l.262 rejects protocol `http` (`stream_load`); `Step` has no HTTP step, and
  extract-stream-loads.rs l.80 skips `group_commit` headers.
- A replay would need harness changes: keep `streamLoadPut`/`loadTxnCommit` (not in `FE_METHODS`) and
  have FakeFe answer puts in order from the recording (its label won't match ours: compare the label's
  shape and same/different pattern, not bytes); accept zero-query suites whose checks are write
  reports; and wall-clock intervals (40 ms … 300 s) make replay timing-sensitive — interval 300000 and
  the 5 s timeout need an injectable clock or a special case. Simpler: unit tests in doris-be
  (FakeFe scripted put/commit) plus the live epic gate, and leave these out of replays.

## What doris-rs needs
1. `fragment`: accept `GROUP_COMMIT_BLOCK_SINK` in `is_load_plan`/`table_sink` (params from
   `olap_table_sink`, mode from `group_commit_mode`); a `WritePlan::GroupCommit` (one
   `GROUP_COMMIT_SCAN_NODE`, SLOT_REF outputs) for the internal plan so `plan_write` builds its
   `WriteFragment` (txn, partitions, tablets) from the put reply.
2. New `doris-be/src/group_commit.rs` (GroupCommitMgr): per table, open queues
   {label, txn_id, schema_version, index_size, interval_ms, data_bytes, start, bytes, rows,
   writers in flight, need_commit, finished+status, Condvar}; `get_queue` = reuse rule above, else
   streamLoadPut (fields above; label from a random u64 pair) — check `need_commit` by elapsed time
   at lookup so reuse is deterministic; one committer thread per queue: wait until need_commit
   (deadline = start+interval, or bytes) and no writer in flight → evaluate internal output exprs →
   `run_sink` → `loadTxnCommit` (fields above) or rollback → finished, wake sync waiters. No WAL.
3. `LoadService::exec_plan_fragment`: for type 12, compute rows (values_rows / stream_rows), route &
   filter via `TableSink` (counts, error log, ratio), append to the queue; async → report now; sync →
   wait for `finished` or cancel. `exec_status` gains `label`/`txn_id`; no commitInfos.
   `cancel_plan_fragment` must wake a sync wait and report CANCELLED with the cancel reason (C++ wording
   `"(<host>)[CANCELLED]PStatus: <reason>"`), plus counters/label/txn.
4. Stream load: `group_commit` header (async/sync/off, else "group_commit can only be [async_mode,
   sync_mode, off_mode]"; label + group_commit → "label and group_commit can't be set at the same
   time"), skip loadTxnBegin, put with `group_commit_mode`, run into the queue, group-commit reply JSON.
   (Table-default mode via loadTxnBegin's `table_group_commit_mode` is not needed by these 4.)
5. `wait_idle`/shutdown must not wait 300 s for an open queue; byte accounting must mirror C++ block
   bytes closely enough that one `(k,v)` row (25 B) stays under 1024 and over 1.

Size: ~3-4 focused days for all four (≈600-900 lines + tests). Cheapest: `test_group_commit_timeout`
(needs only the sink accepted and a sync wait that does not finish in 5 s), then interval/data_bytes
(queue reuse + label + commit; nothing read back). large_data is the dearest (stream-load path, rows
must be visible, 5000-row VALUES, generated column). Risks: timing (commit must fire from the
interval even with no new rows; deterministic reuse rule), MOW unique tables written without delete
bitmaps (unobserved here), WAL/restart durability skipped, background threads vs shutdown, and replay
coverage needing a harness change.
