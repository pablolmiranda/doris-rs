# Doris 4.1.5 BE: REPEAT_NODE, DATA_GEN_SCAN_NODE, GROUP_COMMIT_BLOCK_SINK (M6 story 7, batch 1)

Paths are relative to `be/src/` unless noted. `fe/` means `fe/fe-core/src/main/java/org/apache/doris/`.
RO = `exec/operator/repeat_operator.cpp`, GCS = `exec/operator/group_commit_block_sink_operator.cpp`,
GCM = `load/group_commit/group_commit_mgr.cpp`. Corpus facts come from decoding `oracle/epic1/traffic`
with a scratch decoder built on `doris-wire::suites` and `doris-thrift`; replies come from `oracle/epic1/replies`.

**Bottom line.** REPEAT_NODE and DATA_GEN_SCAN_NODE are small, self-contained operators. GROUP_COMMIT_BLOCK_SINK is
a different kind of work: a BE-driven load subsystem. It needs per-table shared queues, BE-initiated transactions
(`streamLoadPut` → internal plan → `loadTxnCommit`), timers, and a sync-mode wait. Three of its four suites check
that **two inserts share one label**, so "one transaction per insert" cannot pass them. It is roughly 5–8× the size
of the two nodes combined (§5).

## 1. REPEAT_NODE (GROUPING SETS / ROLLUP / CUBE)

**Thrift** (`gensrc/thrift/PlanNodes.thrift:1326-1338`):

| Field | Meaning |
|---|---|
| `output_tuple_id` | Output tuple. Its slots are `exprs.size()` expression columns, then `grouping_list.size()` virtual columns, in that order (RO:113-115, 159). |
| `slot_id_set_list[r]` | Output slot ids **kept** (non-NULL) in repeat `r`. The Thrift comment says "set to null", but BE nulls a repeat slot iff it is **not** in this set (RO:131). |
| `repeat_id_list` | Only its size (= number of repeats R) is used (RO:93). The values (FE bitmaps) are ignored. |
| `grouping_list[g][r]` | The constant of virtual column `g` (GROUPING_ID first, then each GROUPING(x)/GROUPING_ID(x…)) in repeat `r`. The FE precomputes them; BE just copies them (RO:163-176). Init fails if any list is shorter than R: `"grouping_list size {} is less than repeat_id_list size {}"` (RO:63-69). |
| `all_slot_ids` | Output slot ids that take part in grouping (repeat slots) (RO:130). |
| `exprs` | Evaluated on each child block. Each result is made full if const (RO:189-194). |

**Column filling**, for output column `i < exprs.size()` with slot id `s = output_slots[i].id()` (positional mapping,
RO:126-153):
- `s ∉ all_slot_ids`: copied as-is (`insert_range_from`). The FE keeps this slot's type equal to the expr's.
- `s ∈ all_slot_ids` and `s ∉ slot_id_set_list[r]`: all NULL (`insert_many_defaults` on a ColumnNullable).
- `s ∈ all_slot_ids` and `s ∈ slot_id_set_list[r]`: the value, wrapped nullable if the source is not
  (`push_false_to_nullmap`).
- **Nullability:** repeat slots are always nullable (DCHECK RO:135). Virtual grouping columns are **non-nullable
  BIGINT** (`ColumnInt64`, DCHECK RO:170); every row of repeat `r` gets `grouping_list[g][r]`.

**Emission order** (RO:102-105, 178-262; driver `StatefulOperatorX::get_block_impl`, `exec/operator/operator.cpp:719-748`):
- Per **child block**: one `pull` per repeat, `r = 0..R-1`. Each pull emits the **whole** child block for that
  repeat, as one output block of the same row count.
- Only after `r = R-1` are the child block and intermediate block cleared, and the next child block pulled.
- So the order is block-major, then repeat-major, then row order: `B1×r0, B1×r1, …, B1×r(R-1), B2×r0, …`.
  It is **not** "each input row R times".
- Node `conjuncts` filter each output block (RO:253-257). Then `reached_limit` applies, and generic projections run
  after it.
- `eos = child_eos && child_block.rows()==0` (RO:259). An empty child emits no rows. One quirk: an empty child
  with `exprs` empty and R==1 dereferences a null `_intermediate_block` (RO:245-246). That is unreachable in the
  corpus (R≥2 there).

**No-exprs form** (RO:233-250). When `exprs` is empty (e.g. `count(*) … grouping sets ((),(),())`), each pull emits
`child_block.rows()` rows that carry **only** the virtual columns. The child's own columns, e.g. a projection of
literal `1`, are dropped.

**Corpus** (all with `limit -1`, no conjuncts, sometimes projections, `is_serial_operator` sometimes true):
- `test_grouping_sets_empty`: `slot_id_set_list [{},{},{}]`, `grouping_list [[0,1,2]]`, `all_slot_ids {}`.
  This appears both with `exprs: []` and with one slot-ref expr. Duplicate empty sets get distinct GROUPING_IDs
  0,1,2.
- `agg_with_distinct_project`: `[{4},{}]` / `[[0,1]]` / `{4}`, and `[{6,7},{8}]`, `repeat_id_list [1,6]`, `[[1,6]]`,
  `{6,7,8}`.
- `test_grouping_sets1`: up to 6 grouping lists × 3 repeats (e.g. `[[1,2,3],[1,0,1],[0,1,1],…]`).

**Risk.** REPEAT always feeds an aggregation, so its order is visible only through order-sensitive aggregates
without ORDER BY. Block boundaries come from the child, so doris-rs should keep the child's block granularity if it
has one; otherwise row order within a repeat is what matters.

## 2. DATA_GEN_SCAN_NODE (table-valued `numbers(...)`)

**Thrift:**
- `TDataGenScanNode { tuple_id, func_name }` (`PlanNodes.thrift:1703-1706`). `TDataGenFunctionName` has only
  `NUMBERS = 0` (`:790-792`). Any other value fails init with `"Unsupported function type"`
  (`exec/operator/datagen_operator.cpp:42-49`).
- Scan range: `TScanRange.data_gen_scan_range.numbers_params = TTVFNumbersScanRange { totalNumbers, useConst,
  constValue }` (`PlanNodes.thrift:796-804`).

**FE** (`fe/tablefunction/NumbersTableValuedFunction.java`):
- `numbers("number"="N"[, "const_value"="C"])` is the only data-gen TVF. Other TVFs (`s3`, `local`,
  `group_commit`, …) are FILE_SCAN or META_SCAN.
- One column `number BIGINT NOT NULL` (`getTableColumns`).
- **Exactly one scan range**, on one random alive BE (`getTasks`, `:119-145`, `Collections.shuffle`).
- `DataGenScanNode.getNumInstances()` returns 1, or `parallel_exec_instance_num` under
  `ignore_storage_data_distribution` (`fe/planner/DataGenScanNode.java:98-104`). In the latter case the range goes to
  instance 0 only.
- Corpus (`aggregate_groupby_simplify`): 4 `local_params`, only instance 0 has `per_node_scan_ranges {0: [numbers
  totalNumbers=10]}`, `parallel_instances=1`, `is_serial_operator=true`. That is the serial-source/local-shuffle
  shape from `2026-11-09-m6-s6-join-distribution.md` §2, so doris-rs's merged-instance handling applies.

**Rows** (`exec/common/data_gen_functions/vnumbers_tvf.cpp:42-99`):
- Only `scan_range_params[0]` is read (`:92-95`; DCHECK size==1).
- Each `get_next` emits one block of `min(next + batch_size, total) - next` rows (`:62`).
  `batch_size` = `query_options.batch_size` (default 4064).
- Values are `next … end-1` (`insert_range_of_integer`, so **0..N-1 ascending**) or `C` repeated
  (`insert_many_vals`, `:63-67`).
- eos when `end == total`. If `total <= 0`, one empty block with eos.
- Exactly one column (`_slot_num = 1`, `vnumbers_tvf.h:49`), built from tuple slot 0 as `ColumnInt64`.
  doris-rs should refuse a tuple that is not exactly one non-null BIGINT slot (the FE never sends one).
- Then node `conjuncts` filter, then `reached_limit` (`datagen_operator.cpp:73-82`), then generic projections
  (the corpus has `number % 3` projections). Runtime filters are registered but never applied (`:95-101`).

**Risk.** If the merged instance's numbers reached downstream as one stream, unordered results could differ in order
from C++, which local-shuffles blocks to N tasks. That only shows with N > 4064 (several blocks); the suites use
N=10. Ascending order is guaranteed only within one block.

**Suites:**
- `test_assert_num_rows`: `numbers("number"="10")` under a CROSS JOIN with an assert.
- `aggregate_groupby_simplify`: an aggregate over `number % 3`, `number % 2`.
- `test_flexible_partial_update_upgrade_base_data`: `INSERT … SELECT number×6 FROM numbers("number"="10")` into MOW
  tables with and without `store_row_column`.

## 3. GROUP_COMMIT_BLOCK_SINK

### 3.1 What it is

With `set group_commit = async_mode|sync_mode`, an `INSERT … VALUES` qualifies when all of these hold
(`fe/nereids/trees/plans/commands/insert/OlapGroupCommitInsertExecutor.java:70-150`):
- not in a transaction, and not a partial update;
- an OLAP table with light schema change, not in an internal db;
- no explicit partitions, not an overwrite;
- the child is a OneRowRelation, an InlineTable or a constant union.

The FE then plans `UNION_NODE → GROUP_COMMIT_BLOCK_SINK` (sink type 12, carried in `olap_table_sink`, with
`txn_id -1` and `group_commit_mode` 0=SYNC/1=ASYNC, `DataSinks.thrift:281-285`). It begins **no** transaction
(`beginTransaction(){}` `:173`) and sends it by `exec_plan_fragment`.

The BE then does all the transaction work:
- **Queue.** It puts the rows in a per-table **LoadBlockQueue** shared by concurrent and successive inserts.
- **Transaction.** For each queue the BE starts an **internal load**: it asks the FE to plan `insert into
  doris_internal_table_id(T) WITH LABEL group_commit_<hi>_<lo> select * from group_commit("table_id"="T")` via
  `streamLoadPut`. The FE begins the transaction there and returns the plan
  `GROUP_COMMIT_SCAN_NODE → OLAP_TABLE_SINK(txn_id)`.
- **Run and commit.** The BE runs that fragment, which drains the queue, then commits with
  `loadTxnCommit(groupCommit=true)`.
- **Reply.** The insert reports the queue's label and txn id. The FE answers the client
  `{'label':'group_commit_…', 'status':'PREPARE', 'txnId':'N'}` (status is always PREPARE unless it errored:
  `:177-184`, `OlapInsertExecutor.java:358-372`; label and txn from the report, `LoadProcessor.java:211-214`).

### 3.2 Insert side (GCS)

- **open** (GCS:57-85). Get or join a queue via `get_first_block_load_queue` (GCM:269-323). If none is joinable it
  creates one synchronously, waiting on a dependency. It then sets `import_label = queue.label` and
  `wal_id = queue.txn_id` (GCS:95-96).
- **sink_impl** (GCS:305-417), per block:
  - `num_rows_load_total += rows` and `num_bytes_load_total += input_block.bytes()`. This is the
    **pre-conversion** block: in the corpus 25 bytes = 8+8+1+8 for (k BIGINT, v BIGINT, delete-sign TINYINT,
    version BIGINT), all non-null.
  - `OlapTableBlockConvertor::validate_and_convert_block` does the same validation and filter map as OLAP_TABLE_SINK.
  - `find_partition` per row (when not auto-partition). A row with no partition is filtered: `filtered += 1`,
    `total -= 1`, and the error-log line `no partition for this tuple. tuple=<dump_data_json>` (GCS:362-390).
  - Filtered rows are removed, then `_add_block`.
- **_add_block** (GCS:133-170):
  - Every column is made **nullable** (`make_nullable`).
  - While `total + unselected + filtered ≤ group_commit_memory_rows_for_max_filter_ratio` (10000) and nothing was
    appended yet, blocks are buffered locally. Otherwise the buffer is flushed and the block goes straight to the
    queue.
- **eos (wind_up, GCS:315-340):**
  - `total = total + unselected + filtered` and `filtered += convertor_filtered`.
  - If still buffered and `filtered/(total-unselected) > max_filter_ratio` (the sink's, 0.0 in the corpus):
    `DataQualityError("too many filtered rows, {first_error_msg}")`.
  - Otherwise `_add_blocks`, then `remove_load_id`.
  - Past 10000 rows there is no ratio check here (inherited behaviour).
- **_add_blocks** (GCS:206-261). In ASYNC mode it checks WAL disk space; without space the load falls back to SYNC.
  In SYNC mode (or `wait_internal_group_commit_finish`) the sink's finish dependency waits on the queue.
- **close** (GCS:103-122). In SYNC mode it returns `queue.status` once `process_finish`. An unfinished queue gives
  `"_load_block_queue is not finished!"`.
- **Report to FE** (`exec/pipeline/pipeline_fragment_context.cpp:2104-2108`): the normal load report plus
  `txn_id = wal_id` and `label`, and **no `commitInfos`**.
  - Corpus: `done true`, `load_counters {dpp.norm.ALL:"1", dpp.abnorm.ALL:"0", unselected.rows:"0"}`, top
    `loaded_rows 0`, `fragment_instance_reports [{loaded_rows 1, loaded_bytes 25}]`, `txn_id 9036`,
    `label "group_commit_e540a211e0181a83_c07ebc197b1a8989"`.
  - The FE then sends `cancel_plan_fragment(Finished)`.

### 3.3 Queue rules (GCM)

**Joining** (GCM:269-323):
- The first queue of the table that does not `need_commit`.
- `base_schema_version == queue.schema_version && index_size == queue.index_size`. A mismatch is
  `DataQualityError("schema version not match, maybe a schema change is in process. Please retry this load manually.")`.
- Otherwise a new queue is created. On failure: `"can not get a block queue for table_id: T" + reason`.

**Creation** (GCM:474-592):
- Label: `"group_commit_" + UniqueId::gen_uid().to_string()` with `-`→`_`, i.e. `group_commit_%016x_%016x`.
  The same id is the internal load id and query id.
- `streamLoadPut` request (10 s timeout):

  | Field | Value |
  |---|---|
  | `load_sql` | the internal insert SQL above |
  | `loadId` | the label's id |
  | `label` | the label |
  | `token` | `"group_commit"` |
  | `max_filter_ratio` | 1.0 |
  | `strictMode` | false |
  | `partial_update` | false |
  | `user` | `"admin"` |
  | `backend_id` | this BE |

  The corpus request matches exactly (`db`/`tbl` empty, `txn_id 0`, `file_type 0`, `format_type 0`).
- The result carries:
  - `pipeline_params`: GROUP_COMMIT_SCAN_NODE (type 33, `group_commit_scan_node.table_id`, serial, projections
    = slot refs `_tvf_group_commit.c1..cN` onto the sink tuple) → OLAP_TABLE_SINK (`txn_id`, `load_id` = label id).
    Also `txn_conf.txn_id`, `import_label`, `is_mow_table`, one `local_params`.
  - `base_schema_version`, `db_id`, `table_id`, `wait_internal_group_commit_finish` (FE
    `Config.wait_internal_group_commit_finish`, false).
  - **`group_commit_interval_ms`** and **`group_commit_data_bytes`**, the table properties
    (`fe/service/FrontendServiceImpl.java:2536-2548`; defaults 10000 ms and 134217728).
- The c1..cN are all the table's columns in schema order, hidden ones included (`__DORIS_DELETE_SIGN__`,
  `__DORIS_VERSION_COL__` on MOW), **all nullable**. They map positionally to the insert's converted block.
- The BE runs it with `QuerySource::GROUP_COMMIT_LOAD`. The internal fragment sends **no** `reportExecStatus`
  (none in the corpus); its finish callback commits.

**Commit condition**, `need_commit` set once:
- `data_bytes ≥ group_commit_data_bytes` on `add_block` (GCM:117-121). `data_bytes` sums `block.bytes()` of the
  nullable blocks: per row 9 per nullable BIGINT, 2 per nullable TINYINT, strings = chars + 4·rows offsets + null
  map. Corpus: 3 rows → `receive_bytes 87` = 3×29.
- or `now - start ≥ group_commit_interval_ms`, checked on `add_block` and on every `get_block` (GCM:122-129,
  152-154). `start` is the queue's creation time.
- A `RuntimeFilterTimer` wakes the scan at the interval (`exec/operator/group_commit_scan_operator.cpp:51-56`).

**eos of the internal scan:** queue empty && `need_commit` && no registered load ids (GCM:186-190). A slow insert
that is still registered therefore holds the group open past the interval. `add_load_id` refuses once `need_commit`
(GCM:222-235).

**Finish** (GCM:594-765):
- **OK:** `loadTxnCommit` with `token = cluster token`, `db_id`, `table_id`, `txnId`,
  `thrift_rpc_timeout_ms 180000`, `groupCommit true`, `receiveBytes = internal num_bytes_load_total`, `backendId`,
  `commitInfos` (every tablet the sink opened; 8 for 8 buckets with 3 rows), `auth_code 0`. It retries on
  `DELETE_BITMAP_LOCK_ERROR`.
- **Failure:** `loadTxnRollback(reason)`.
- Then cancel the queue on failure, close the WAL, set `process_finish`, and wake the SYNC waiters with the status.
- The WAL is deleted on success (GCM:719-726); on failure it is queued for replay.
- The FE publishes by the usual `PUBLISH_VERSION` agent task.

**WAL** (`exec/sink/writer/vwal_writer.cpp`, `load/group_commit/wal/*`):
- ASYNC writes every block to `<wal_dir>/<db>/<table>/<ver>_<be>_<txn>_<label>` before the insert returns
  (GCM:95-101).
- On BE restart, `WalManager` replays it via an internal load.
- `get_wal_queue_size` (brpc, `service/internal_service.cpp:2425-2441`) is how the FE waits for WALs on every
  schema change (`fe/alter/SchemaChangeJobV2.java:649-651`, `fe/load/GroupCommitManager.java:95-180`) and on
  decommission.

### 3.4 Stream load with `group_commit` header (insert_group_commit_with_large_data)

`service/http/action/stream_load.cpp`:
- Header handling (`:945-985`):
  - A value other than sync/async/off is rejected with `"group_commit can only be [async_mode, sync_mode, off_mode]"`.
  - `label` + group_commit is rejected: `"label and group_commit can't be set at the same time"`.
  - `partial_columns`, partitions, 2pc or `unique_key_update_mode` → no group commit.
  - Content-Length 0 and not chunked → no group commit.
- With group commit:
  - No `loadTxnBegin` (`:359`).
  - The `streamLoadPut` request carries `group_commit_mode` (`:766-768`).
  - The FE returns `FILE_SCAN_NODE → GROUP_COMMIT_BLOCK_SINK`.
  - No commit (`:190-193`).
  - Label and txn come from the queue (`load/stream_load/stream_load_executor.cpp:97-100`).
- Table-property mode: `loadTxnBegin` answers `table_group_commit_mode`, and the begin returns without a txn
  (`stream_load_executor.cpp:227-234`).
- The JSON (`load/stream_load/stream_load_context.cpp:40-130`) drops `TwoPhaseCommit`, `BeginTxnTimeMs` and
  `CommitAndPublishTimeMs`, and adds `GroupCommit:true`, `GroupCommitMode`, `LoadId`. Corpus:
  `{"TxnId":9062,"Label":"group_commit_144d…","Comment":"","GroupCommit":true,"GroupCommitMode":"async_mode","LoadId":"8d4f263e3258b3ae-af6821e197e817b3","Status":"Success",…}`.

### 3.5 What the suites check

| Suite | Checks |
|---|---|
| `insert_group_commit_with_large_data` | A 5000-row VALUES insert in async mode: `affected == 5000`, server info contains `'status':'PREPARE'` and `'label':'group_commit_'`. Then it polls `count(*)` up to 30×2 s for visibility; table interval is 40 ms. Generated-column table: 3 group-commit inserts, poll, `qt_select1`. An **async group-commit stream load** (`json.Status=="success"`, `NumberTotalRows==4`, `strict_mode`, `columns a,dummy,b`), poll, `qt_select2`. CTAS and `INSERT … SELECT … UNION ALL` under async_mode, which are **not** group commit (OLAP_TABLE_SINK, INSERT_SELECT). |
| `test_group_commit_interval_ms_property` | Table interval 5000. msg1 and msg2 (100 ms apart) **must share the label**. After `ALTER … interval=1000`, msg3 still lands in the open 5000-ms queue (corpus: same label/txn 9036 for msgs 1-3). After 6 s, msg4 **must have a different label** (corpus: 9037). |
| `test_group_commit_data_bytes_property` | `data_bytes=1024`: msg1 and msg2 share a label. After `data_bytes=1`: msg3 ≠ msg4, because msg3's 29 bytes ≥ 1 sets need_commit. It retries msg4 while the error says `can not get a block queue`. |
| `test_group_commit_timeout` | `sync_mode`, interval 300000, `query_timeout=insert_timeout=5`: the insert **must fail** with "timeout" within 60 s. The reply (FE-made) is `ERR 1105 "errCode = 2, detailMessage = Execute timeout"`. The FE sends `cancel_plan_fragment(status 1 "insert timeout")`; C++ then reported `CANCELLED "(127.0.0.1)[CANCELLED]PStatus: insert timeout"` with label and txn. |

Labels and txn ids vary between reference runs, so the grader masks them (`oracle/tests/test_grade.py:281-285`).
The **suites themselves** compare label equality: `msg.substring(idx("group_commit")+11, +43)` covers `t_` plus
30 hex characters.

**Simple alternative: each insert as its own transaction.**
- It **fails** interval_ms (msg1 ≠ msg2), data_bytes (msg1 ≠ msg2) and timeout (a sync insert would succeed at
  once).
- It passes only large_data.
- It also still needs the whole BE-driven machinery: no FE transaction exists until the BE calls `streamLoadPut`.

So the faithful queue semantics are the minimum. The honest simplification is **no WAL**:
- WALs are never visible in replies.
- C++ deletes them right after a successful commit.
- They matter only across a BE crash between the reply and the commit, or a failed commit, where C++ would replay.

## 4. Traffic: RPCs used, and what doris-rs lacks

**Needs index.** The full needs of all 10 suites were checked with `oracle/checker/derive.py::unsupported_needs`
(supplement included, directory load suites included) against `oracle/scores/epic1-supported.json`. **Each suite
lacks exactly its one target feature**: REPEAT_NODE ×3, DATA_GEN_SCAN_NODE ×3, GROUP_COMMIT_BLOCK_SINK ×4.

What the index cannot see:
- the internal plan inside `streamLoadPut` replies (GROUP_COMMIT_SCAN_NODE → OLAP_TABLE_SINK with
  `is_mow_table`);
- the group-commit stream load's `FILE_SCAN_NODE → GROUP_COMMIT_BLOCK_SINK`;
- the generated-column insert.

**RPCs in the group-commit suites:**
- FE→BE:
  - `PBackendService.exec_plan_fragment` (single-shot; the plan has GROUP_COMMIT_BLOCK_SINK).
  - `PBackendService.cancel_plan_fragment` (`Finished`, or `insert timeout`).
  - `BackendService.submit_tasks` (CREATE, DROP, PUBLISH_VERSION, UPDATE_VISIBLE_VERSION).
  - `http.stream_load` (large_data).
  - In large_data: `exec_plan_fragment_prepare`, `exec_plan_fragment_start` and `fetch_data`, for its queries.
- BE→FE:
  - `FrontendService.streamLoadPut`, two shapes: the internal one with `load_sql`, and the stream-load one with
    `group_commit_mode`.
  - `FrontendService.loadTxnCommit` (`group_commit=true`).
  - `FrontendService.reportExecStatus` (with `txn_id` and `label`, and workload-statistics reports).
  - `FrontendService.finishTask`.
  - The corpus has no `loadTxnRollback` and no `loadTxnBegin`; the commits succeed and the stream load skips begin.

doris-rs already speaks every one of these **methods**: `crates/doris-be/src/brpc.rs:95-135` and
`fe_client.rs:30-43` cover them. **No new RPC method is needed for these suites.** What is new:
- **New request shapes:** the internal `streamLoadPut` fields, the `loadTxnCommit` group-commit fields and token,
  and report `txn_id`/`label`.
- **Routing:** `brpc.rs::is_write`/`fragment::is_load_plan` route only OLAP_TABLE_SINK to the load service, so a
  GROUP_COMMIT_BLOCK_SINK plan goes to the query service.
- **Existing refusals:** `fragment::table_sink` refuses `group_commit_mode`
  (`crates/doris-exec/src/fragment.rs:169-171`), and the stream load refuses table group-commit mode
  (`crates/doris-be/src/load.rs`).
- **Cancel:** `LoadService::cancel_plan_fragment` is a no-op. A sync-mode wait must instead be released by the
  cancel.

**Methods not in these suites but tied to the feature:**
- `PBackendService.get_wal_queue_size`: every schema change polls it, up to `check_wal_queue_timeout_threshold`.
  Without WALs, answer `size 0`. Cheap, and it removes an ALTER stall risk.
- `PBackendService.group_commit_insert` (`internal_service.cpp:2337-2423`): the legacy prepared-statement path,
  `insert_group_commit_with_prepare_stmt`.
- `http_stream` group commit (`load_p0.http_stream.*`).

## 5. Recommendation

| Feature | Build | Refuse at prepare (naming it) | Size |
|---|---|---|---|
| REPEAT_NODE | The operator exactly as §1: positional exprs, keep-set semantics, nullable repeat slots, BIGINT non-null grouping columns, block×repeat order, conjuncts then limit, then projections. Add to `check_node` and `pipeline.rs`. | `grouping_list` shorter than R (C++ error text); output slot count ≠ exprs + grouping_list (BadRequest). | S: ~150–250 LoC + tests |
| DATA_GEN_SCAN_NODE | A source producing `number` 0..N-1 (or const C) in `batch_size` blocks from the single range, conjuncts, limit. Instances without a range produce nothing; serial/parallel_instances goes through the existing merge. | `func_name ≠ NUMBERS` (`Unsupported function type`); tuple not exactly one non-null BIGINT slot; more than one range. | S: ~100–150 LoC + tests |
| GROUP_COMMIT_BLOCK_SINK | (a) Load-service path for sink 12: VALUES → output exprs → existing convertor and partition filtering → nullable rows → queue; report with label/txn, no commitInfos. (b) `GroupCommitMgr`: per-table queues; join rules (schema version, index size, need_commit); label generation; internal `streamLoadPut`; run the internal plan by adding a GROUP_COMMIT_SCAN_NODE source to `plan_write` and reusing `TableSink` with the returned txn; interval timer and C++ byte accounting; eos rule (no registered loads); `loadTxnCommit(groupCommit)` / `loadTxnRollback`. (c) SYNC: block the insert until the queue finishes, return its status, and release on `cancel_plan_fragment`. (d) Stream load: header rules, skip begin, `group_commit_mode` on put, queue sink, group-commit JSON fields; table-property mode from `loadTxnBegin`. (e) `get_wal_queue_size` → 0. | WAL durability (do not write WALs; document it). Auto-partition sink (already refused). `group_commit_insert` brpc and `http_stream` group commit (not in batch). | **L: ~1200–1800 LoC + tests**, 5–8× REPEAT+DATA_GEN combined. Concurrency (timers, waits, cancel) is the long pole. |

**Risks of wrong answers:**
1. **Group grouping.** Labels must be shared or split exactly as in C++ (§3.3). Get any of these wrong and the
   label assertions fail: start time = queue creation, `need_commit` checked on add *and* by the timer, a slow
   registered insert holding eos, ALTER not affecting an open queue.
2. **Visibility timing.** Rows become visible only after the internal commit and publish. The suites poll, but
   `qt_select1/2` run right after a poll for *count ≥ N*. Committing late (timer drift) only slows things;
   committing the wrong rows, or twice, gives wrong counts.
3. **Byte accounting.** It only matters near the `data_bytes` threshold (suites use 1 and 1024 against 29-byte
   rows). Use the nullable-block formula above.
4. **Counters.** `dpp.norm.ALL` becomes the affected rows. Filtered and no-partition rows, the 10000-row
   max-filter-ratio window, and the `"too many filtered rows, …"` text must follow GCS:315-340.
5. **Sync timeout.** The client text is FE-made ("Execute timeout"). Still, release the waiting insert on cancel and
   report CANCELLED. Otherwise the thread blocks for 300 s, and `wait_idle` and shutdown stall.
6. **No WAL.** Data acknowledged in ASYNC mode is lost if doris-rs dies or the commit fails before the interval
   ends. That is invisible to the graded replies, but it is a real divergence and must be documented.
7. **Order.** REPEAT and DATA_GEN order shows only through unordered outputs; DATA_GEN only when N exceeds one
   batch.
