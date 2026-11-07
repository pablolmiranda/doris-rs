# Epic 1 · M4: the write loop — design

Status: written from the needs-index numbers at the user's request ("just go ahead and write the
plan based on the numbers"), 2026-11-05. Every scope call below is the designer's, listed in §9.

Research behind this design (decoded corpus examples, C++ citations):

- `research/2026-11-05-m4-insert-on-the-wire.md`: how `INSERT … VALUES` reaches the BE, and
  what the BE reports back.
- `research/2026-11-05-m4-txn-publish-stream-load.md`: transactions, publish, stream load, and
  rowset metadata.
- `research/2026-11-05-m4-segment-writer.md`: writing segment files the C++ BE reads.

## 1. Intent

doris-rs holds empty tablets today (M2) in the C++ BE's data directory (M3). M4 makes it store
rows. `INSERT … VALUES` and stream load into duplicate-key tables land as rowsets of segment
files, under transactions the FE commits and publishes, in exactly the form the C++ BE writes. A
C++ BE started on doris-rs's root must query that data and get the reference's answers.

The epic spec's M4 was "the minimum write and read loop". The needs index splits it in two:

| doris-rs supports | gate suites whose BE needs are all covered |
|---|---|
| today (M2 + M3) | 59 |
| + the write loop (this milestone) | 96 |
| + scans, filters, projections, `LIMIT`, sort, materialization, result sink (M5) | 559 |

Reads alone (M5 without M4) would cover 417 suites by features, but most of those read tables
another suite loaded earlier in the same run. So they pass only once doris-rs writes. ADR 0002
already put writes first. M4 is therefore the write loop, and M5 is the read loop. M5 adds sort
and the 4.x materialization node, which the spec's M4 lacked: without sort, M4+M5 cover 156
suites, not 306, and the materialization node adds 253 more.

## 2. Goals

1. **INSERT … VALUES.** `PBackendService.exec_plan_fragment` with a UNION_NODE of constant rows →
   OLAP_TABLE_SINK runs to completion. The BE reports it with `reportExecStatus`, the FE commits,
   and PUBLISH_VERSION makes it visible. This is 6,267 statements in 652 suites of the corpus.
2. **Stream load** of CSV and JSON over `PUT /api/{db}/{table}/_stream_load`, with
   `loadTxnBegin` → `streamLoadPut` → run the returned fragment → `loadTxnCommit` or
   `loadTxnRollback`, and the C++ BE's JSON reply. This covers 149 suites; CSV is about 94% of
   the loads.
3. **Transactions and versions.**
   - Pending rowsets persist as `rst_` keys (COMMITTED).
   - PUBLISH_VERSION makes them VISIBLE at `[v,v]`.
   - CLEAR_TRANSACTION_TASK discards them.
   - Tablet reports carry the version, row count and sizes the C++ BE reports.
4. **Segment files the C++ BE reads.** V2 segments for every M4 column type: PLAIN pages, RLE
   null maps, an ordinal index, zone maps, the short key index, and the footer. Rows are sorted
   as the memtable sorts them.
5. **Roots with data open.** M3's refusal of roots holding rowsets is lifted. A root the C++ BE
   loaded, or one doris-rs loaded, opens with all its rowsets, visible and pending.
6. **The readback check** (epic spec §5.5, first form). doris-rs loads a fixed dataset on the
   swap cluster; then the C++ BE starts on that root and runs a fixed query set. Every reply
   equals the reply from the same loads made by the C++ BE itself.
7. **The gate.** Every epic suite whose BE needs are within the M4 feature set (§7) passes on
   both gates, and the M2 suites still pass.

## 3. Non-goals and anti-goals

- **No queries.** doris-rs runs no scan, sort or result sink until M5. `exec_plan_fragment`
  accepts only the write shapes (§4.1). Any other plan is refused with a status that names it,
  never run half-way.
- **No unique-key, aggregate-key or merge-on-write tables;** CREATE for them stays unsupported
  as in M2. No partial update, delete, schema change, compaction, or `INSERT … SELECT` (that
  needs a scan).
- **No group commit.** Stream loads the FE answers with a group-commit mode, and
  GROUP_COMMIT_BLOCK_SINK, get NOT_IMPLEMENTED: 5 suites, 468 loads.
- **No parquet, orc or arrow stream loads** (6 loads).
- **Not byte-identical segments.** BIT_SHUFFLE, DICT, ZSTD and real zone-map bounds are later
  work. The C++ BE must read doris-rs's segments correctly; it need not produce the same bytes.
- **Anti-goal: a load that reports success but did not persist.** The FE must never hear a
  commit before the rowset and its `rst_` key are durable to the same degree as on the C++ BE.
- **Anti-goal: a root the C++ BE can no longer open.** The swap check and the readback check
  hold after every load shape.

## 4. What the BE must do

### 4.1 Fragments

- **The request.** `exec_plan_fragment`: `compact=true`, `version=3`, a compact-Thrift
  `TPipelineFragmentParamsList` holding one fragment. Reply at once with
  `PExecPlanFragmentResult{status OK, received/execution/execution_done_time}`, then run in
  the background.
- **The write shapes.**
  - INSERT: UNION_NODE (`const_expr_lists`) → projections → `output_exprs` → OLAP_TABLE_SINK.
  - Stream load: FILE_SCAN_NODE over a FILE_STREAM pipe keyed by the load id → OLAP_TABLE_SINK.
- **Expressions.** Typed literals (INT, LARGE_INT, STRING, BOOL, FLOAT, DECIMAL, DATE*, NULL),
  SLOT_REF, the casts in a stream load's `expr_of_dest_slot`, and `substring(s, 1, n)`, which
  the FE adds when `enable_insert_strict=false`. Anything else refuses the fragment.
- **`cancel_plan_fragment`** answers OK. Status FINISHED is a no-op; otherwise a running
  fragment is cancelled.
- **The final report.** The fragment ends with one `reportExecStatus` to `fragment.coord`:
  - the fixed fields: `done=true`, `backend_num=-1`, instance `0/0`, `fragment_id`,
    `query_type=LOAD`, `backend_id`;
  - the three `load_counters`;
  - `fragment_instance_reports` with the row and byte counts;
  - `commit_infos` for every tablet of every index of each touched partition;
  - when rows were filtered, `tracking_url` and `first_error_msg`.

  Field values are in the INSERT research note, §2.

### 4.2 The table sink

- **Partitions.** Find each row's partition: unpartitioned, RANGE or LIST, from the sink's
  partition params. A row matching no partition is filtered with the C++ message.
- **Buckets.** Route each row to a bucket by the zlib crc32 chain over the distribution
  columns, mod the bucket count. This was checked against the corpus's publish delta rows.
- **Validation.** NULL into NOT NULL and a string longer than its column are filtered and
  counted, with the C++ first-error text. Filtered rows go to an error log served at
  `/api/_load_error_log?file=…`.
- **Per tablet.**
  - Sort rows as `MemTable::_sort` does: key columns, NULLs first, equal keys in descending
    input order.
  - Write one segment.
  - Build the RowsetMetaPB; its fields are listed in the txn research note, §4.
  - Commit it. Every opened tablet without rows gets an empty rowset.

### 4.3 Transactions, publish, clear

- **TxnManager.** An in-memory `(partition, txn) → tablet → (load id, rowset)` map.
  - **Commit** writes `rst_<tablet_uid>_<rowset_id>` = RowsetMetaPB(COMMITTED) to the `meta`
    CF. A second commit with the same rowset id changes nothing.
  - **At start**, the BE reloads `rst_` keys: COMMITTED ones into the map, VISIBLE ones onto
    their tablets.
- **PUBLISH_VERSION.** For each partition: make the rowsets visible at `[v,v]` with
  `creation_time` now, save the `rst_` key again, and add them to the tablet. Then send
  `finishTask` with:
  - `succ_tablets{t: 0}` for every partition tablet that has `v`;
  - `error_tablet_ids`;
  - `table_id_to_tablet_id_to_delta_num_rows`, covering tablets with rows only;
  - `report_version`.

  Publishing an already-visible txn succeeds again. A task with no partitions sends empty maps.
- **Header checkpoint.** The C++ BE folds visible rowsets into the tablet header every 10 new
  rowsets or 600 s. doris-rs saves the header at publish instead, and deletes the `rst_` key in
  the same write batch; both layouts are C++-loadable. See §9.
- **CLEAR_TRANSACTION_TASK** deletes the txn's pending `rst_` keys and files, for its
  partitions or for all of them. Its `finishTask` has no `report_version`.

### 4.4 Stream load

- **The request.** `PUT /api/{db}/{table}/_stream_load` on the webserver port.
  - Honour `Expect: 100-continue`, and a Content-Length or chunked body.
  - The label comes from the header, or is a UUID.
- **The FE calls.**
  - Call `loadTxnBegin` before reading the body, with `use_table_group_commit_mode: true`; if
    the reply sets a group-commit mode, refuse it (§3).
  - Call `streamLoadPut` with the mapped headers. CSV: `column_separator`, `line_delimiter`,
    `columns`, `enclose`/`escape`, `skip_lines`, `trim_double_quotes`, `strict_mode`,
    `max_filter_ratio`, `timeout`. JSON: `read_json_by_line`, `strip_outer_array`,
    `jsonpaths`, `json_root`, `fuzzy_parse`.
- **Running it.** Run the returned fragment, feeding the body into its pipe, and apply
  `max_filter_ratio`. Then call `loadTxnCommit` (`sync: true`, all commit infos) or
  `loadTxnRollback`.
- **The reply.** HTTP 200 with the C++ JSON: key order, 4-space indent, a trailing newline,
  `ErrorURL`/`FirstErrorMsg` only when non-empty.
- **Recording.** Each load is recorded in `<root0>/stream_load` as `"<finish_ms>_<label>"`, and
  `BackendService.get_stream_load_record` serves the records.

### 4.5 Segments

- **Encoding.**
  - Every column uses PLAIN pages: fixed-width values, or binary with offsets.
  - A page that holds a NULL gets an RLE null map.
  - Pages are uncompressed: `NO_COMPRESSION`, with the crc32c page framing.
- **Indexes per column.**
  - An ordinal index: the root data page when there is one page, otherwise one leaf index page.
  - A zone map per page and per segment, written as `pass_all=true` with truthful
    `has_null`/`has_not_null`. Omitting zone maps makes the C++ BE fail `MIN`/`MAX`.
- **Short key page.** An entry every 1024 rows, KeyCoder-encoded. `num_short_key_columns` and
  `index_length` are taken from the tablet schema the FE sent.
- **Footer.** SegmentFooterPB, then `len | crc32c | "D0R1"`.
- **Values.** DATEV2 as a packed u32, DATETIMEV2 as a packed u64, DECIMALV3 as the unscaled
  integer, LARGEINT as an i128, CHAR zero-padded to N.
- **Reader.** `doris-segment` learns PLAIN columns, the short key page and zone maps, so every
  file the writer produces is read back in tests.

### 4.6 Reports

- `version` is the max continuous version, `row_count` = Σ num_rows, `data_size` = Σ total disk
  size, `local_segment_size` = Σ data disk size (M2 reports the total; that is fixed here),
  and `local_index_size` = Σ index size.
- `total_version_count` counts the rowsets; `visible_version_count` counts those up to the
  partition's visible version.

## 5. Fixtures

`make meta-fixtures` grows a third capture, `cpp-loaded`: the C++ BE after INSERTs and CSV and
JSON stream loads into duplicate-key tables. It covers 1 and several buckets, a RANGE-partitioned
table, nullable and NOT NULL columns, and every M4 type. Kept: headers, `rst_` values, the
segment files, `layout.txt`, and the queries' replies. It is the byte-level oracle for
RowsetMetaPB fields, key bounds, the short key page, and the footer apart from encodings.

## 6. The readback check

`make -C oracle readback-check` runs on the swap cluster (M3).

1. A fixed script loads the dataset through the FE: the `cpp-loaded` statements, plus an
   INSERT and a stream load of more than 1,024 rows, so there are several short-key entries and
   pages.
2. The script runs twice: once with the C++ BE as the only backend, then on a fresh root with
   doris-rs as the backend. In the second run doris-rs stops and the C++ BE starts on its root.
3. Both times the C++ BE runs the fixed query set, and the replies are compared. The set covers:
   - `SELECT *` with and without ORDER BY;
   - point and range predicates on the key prefix;
   - `IS NULL` and value predicates;
   - `COUNT(*)`, `MIN`, `MAX`;
   - a join of two loaded tables.
4. The FE must see no difference: the M3 snapshot compare, the tablet report diff, and no DROP
   or CLONE.

The swap check (M3) keeps its six steps, now with rows in its tables. The `no_data` guard is
removed.

## 7. The M4 feature set and the gate

`scores/epic1-supported.json` gains:
- the fragment RPCs `exec_plan_fragment`, `cancel_plan_fragment` and `reportExecStatus`.
  `_prepare`/`_start` (used only by `INSERT … SELECT`) and `update_cache` wait for M5; leaving
  them out changes no count;
- the write sinks and nodes (`OLAP_TABLE_SINK`, `UNION_NODE`);
- the tasks `PUBLISH_VERSION` and `CLEAR_TRANSACTION_TASK`;
- the stream load RPCs and route;
- the literal and cast expressions;
- the remaining scalar types: BOOLEAN, SMALLINT, FLOAT, DOUBLE, DATE, DATETIME, NULL_TYPE and
  INVALID_TYPE.

The gate is every suite whose needs fall within that set (96 by today's index). The ratchets
record what passes.

## 8. Left to the builder

- **Code layout:** crate and module names. The designer's guess: a `doris-exec` crate for
  fragments, expressions and the sink; the writer in `doris-segment`; transactions in
  `doris-storage`.
- **Concurrency:** how fragments and loads run concurrently.
- **Error logs:** their format beyond the C++ fields.
- **Test data:** the readback dataset's contents, within §6.

## 9. Calls made by the designer (not by the user)

1. The spec's M4 is split: the write loop (M4) first, the read loop (M5) next. M5 gains sort and
   the materialization node. These are needs-index numbers: 96, then 559.
2. Group commit and the parquet, orc and arrow loads stay out of M4 (5 suites, 6 loads).
3. Segments are PLAIN and uncompressed, with pass_all zone maps. Byte parity comes later.
4. Publish saves the tablet header and drops the `rst_` key at once, instead of the C++ BE's
   periodic checkpoint. Both layouts load on the C++ BE.
5. The readback check uses a small fixed dataset now, not TPC-H SF 0.01. TPC-H and the
   unique-key update load come with M5's queries and the unique-key milestone.
6. `exec_plan_fragment` refuses every non-write plan until M5.
