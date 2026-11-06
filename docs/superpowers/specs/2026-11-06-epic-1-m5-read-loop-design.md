# Epic 1 · M5: the read loop — design

Status: written from the needs-index numbers at the user's request ("let's write the plan based on
data"), 2026-11-06. Every scope call below is the designer's, listed in §10.

Research behind this design (decoded corpus examples, C++ citations):

- `research/2026-11-06-m5-queries-on-the-wire.md`: how a SELECT reaches the BE, the result path,
  the row text encoding, `update_cache`, sort and materialization.
- `research/2026-11-06-m5-reading-segments.md`: which rowsets a scan reads, the encodings the C++
  BE writes, what the scan may skip, row ids.
- `research/2026-11-06-m5-aggregation-and-schema-scan.md`: aggregation shapes and semantics, and
  the information_schema scans `test_query_sys_tables` needs.

## 1. Intent

doris-rs stores rows since M4, but the C++ BE is still the only thing that can read them. M5 makes
doris-rs answer SELECTs over its tablets: scan, filter, project, sort, limit, aggregate, and send
the rows to the FE as the C++ BE does.

The epic spec's M5 row lists scans, filters, projections, `LIMIT`, sort, the materialization node
and the result sink. The needs index (gate of 1,622 suites, 170 covered after M4) says that list
alone is worth little, and two additions are worth a lot:

| doris-rs supports | gate suites whose needs are all covered |
|---|---|
| today (M4) | 170 |
| + the spec's read loop (scan, filters, sort, materialization, result sink, empty set) | 209 |
| + `state:global wait_timeout=31000` | 386 |
| + aggregation (`count`, `sum`, `min`, `max`, `avg`, `count(distinct)`) | 467 |
| + the information_schema scans that make the line above true | **476** (this milestone) |
| + hash and cross joins (M6 candidate) | 562 |

**Why `wait_timeout` matters.** 809 gate suites read `@@wait_timeout` in their JDBC connect query
and expect 31000. The value comes from `nereids_p0.system.test_query_sys_tables`, which runs
`SET GLOBAL wait_timeout = 31000` at its line 175 and is the suite just before all 809 in the run.
The reference ran it to the end (aggregation research, B.3). On doris-rs it stops at line 33, its
first BE query. So every statement before line 175 must succeed on doris-rs: two `count(*)`
queries over static schema tables, then the columns, schemata, tables and session_variables
scans. This one suite is the single largest unlock in the index.

## 2. Goals

1. **Query fragments.** `exec_plan_fragment` (one fragment) and `exec_plan_fragment_prepare` +
   `exec_plan_fragment_start` (two or more fragments), sharing the first fragment's descriptor
   table, globals and coordinator with `is_simplified_param` fragments. The result buffer exists
   once `prepare` returns; execution waits for `start` when `need_wait_execution_trigger` is set.
2. **The result path.** RESULT_SINK feeds a buffer keyed by query id. `fetch_data` long-polls it:
   data packets `{packet_seq n, eos false, row_batch = binary-thrift TResultBatch}`, then
   `{packet_seq N, eos true, query_statistics{returned_rows}}`. Rows are MySQL text rows with the
   C++ BE's exact per-type text (wire research §3.2).
3. **The OLAP scan** of `[tablet_id, version]`: the rowsets on the version path to `v`, their
   segments in order, the columns the tuple needs, every conjunct evaluated after reading, and the
   scan `limit`. Duplicate-key tablets, plus unique-key merge-on-write tablets that hold no rows
   (§4.3).
4. **Operators:** projections, SELECT_NODE, EMPTY_SET_NODE, constant UNION_NODE → RESULT_SINK,
   SORT_NODE (full and top-n, offset, per-key `is_asc`/`nulls_first`), in-process EXCHANGE_NODE +
   DATA_STREAM_SINK (UNPARTITIONED, merging, HASH_PARTITIONED, RANDOM), MATERIALIZATION_NODE, and
   AGGREGATION_NODE in one or two phases with `count`, `sum`, `min`, `max`, `avg` and
   `multi_distinct_count`.
5. **Expressions:** BINARY_PRED (`eq ne lt gt le ge`), COMPOUND_PRED (and/or/not), IN_PRED,
   `is_null_pred`, `is_not_null_pred`, `substring`, over every M4 type, with SQL three-valued
   logic.
6. **Schema scans** for CHARSETS, COLLATIONS, COLUMNS, SCHEMATA, TABLES and SESSION/GLOBAL
   VARIABLES, through the FE calls the C++ scanners make (`getDbNames`, `getTableNames`,
   `describeTables`, `listTableStatus`, `showVariables`).
7. **Cache RPCs.** `update_cache` replies CACHE_OK; `fetch_cache` replies NO_SQL_KEY, so the FE
   runs the query itself.
8. **Errors.** A failing query sends one `reportExecStatus` per fragment and fails the parked
   `fetch_data` with the raw status code. A successful query sends no report.
9. **The readback check, second form.** doris-rs answers the readback queries too (§7).
10. **The gate.** Every epic suite whose needs fall within the M5 feature set (§8) passes on both
    gates, and the M4 suites still pass.

## 3. Non-goals and anti-goals

- **No joins, window functions, set operations, multi-cast sinks or table functions.** A plan
  holding any of them is refused at `prepare` with a status that names the node, never run
  half-way. Joins are the strongest M6 candidate (+86 suites).
- **No other functions.** Arithmetic, `if`, `like`, `concat`, casts beyond M4's, `ndv` and the rest
  wait for a later milestone. An unknown function refuses the fragment.
- **No reads of unique-key or aggregate-key tablets that hold rows,** nor of rowsets with a
  `delete_predicate`. The scan refuses them with a clear error rather than return wrong rows.
- **No old-style DATE/DATETIME (v1), no DECIMALV2, no complex types.** Same as M4.
- **No SQL result cache.** `fetch_cache` always misses.
- **No point-query short circuit** (`tablet_fetch_data`), no `fold_constant_expr`.
- **No changes to FE settings** to avoid plan shapes (e.g. turning lazy materialization off). The
  grading cluster must plan exactly as the reference does.
- **Anti-goal: a wrong answer.** Anything doris-rs cannot read or evaluate exactly is an error,
  never a silent approximation.
- **Anti-goal: a query that hangs.** Every refusal or failure reaches the FE (a failed fetch or
  report) at once, never through the FE's query timeout.

## 4. What the BE must do

### 4.1 Fragments and the pipeline

- **Which RPC.** One fragment uses `exec_plan_fragment`; more than one uses `prepare` + `start`
  (the FE rule is `fragments > 1`; the proto comment saying "< 3" is stale). Both carry a
  compact-Thrift `TPipelineFragmentParamsList`, as in M4.
- **Plan check at prepare.** Every node, sink, expression and function in every fragment must be
  in the M5 set; otherwise refuse the whole query at prepare with a status naming the first
  unsupported item.
- **Instances.** A fragment may have several instances (`local_params`), each with its own scan
  ranges. Exchanges honour `per_exch_num_senders`. doris-rs may run instances on threads or in
  sequence, as long as results match.
- **Exchanges are in-process.** The C++ BE never calls `transmit_block` to its own address, and
  the corpus has none. doris-rs connects DATA_STREAM_SINK to EXCHANGE_NODE by
  `(query id, dest fragment instance id, exchange node id)` in memory. A merging exchange
  (`sort_info` set) merges its sorted inputs and applies offset/limit.
- **`cancel_plan_fragment`.** FINISHED is a no-op; LIMIT_REACH and errors stop the query and fail
  any parked fetch.
- **Lifetime.** A query's state is dropped when its result is fully fetched, it is cancelled, or
  it fails; nothing outlives the query.

### 4.2 The result sink and `fetch_data`

- **Buffer.** Created at prepare, keyed by query id (`fetch_data.finst_id == query_id` in every
  corpus query). The FE may fetch before `start`; the fetch waits.
- **Packets.** One data packet per batch (`batch_size`, default 4064 rows), `packet_seq`
  contiguous from 0, no `query_statistics` on data packets. The eos packet has
  `packet_seq = data packets sent` and `returned_rows`. An empty result is just the eos packet.
- **Row text.** Length-encoded cells, `0xFB` for NULL. Per type (wire research §3.2): integers in
  decimal; BOOLEAN `0`/`1`; FLOAT/DOUBLE in fmt 7.1.3's shortest round-trip form (exponent form
  below 1e-4 and from 1e16), `inf`/`-inf`/`nan` as C++ prints them; DECIMALV3 with exactly
  `scale` digits; DATEV2 `YYYY-MM-DD`; DATETIMEV2 with the column's scale of fraction digits;
  CHAR/VARCHAR/STRING as stored (CHAR without its zero padding).
- **Projections.** The result sink's `output_exprs` run on the final batch, as M4's sink does.

### 4.3 The OLAP scan

- **Rowsets.** Walk the version graph from 0 to `v`, taking at each step the widest rowset whose
  end does not pass `v`, visible rowsets before stale ones (`version_graph.cpp:417-480`). No path:
  fail with the C++ missed-version error.
- **Order.** Rowsets in version order, segments in id order, rows in segment order; no merge for
  duplicate keys. A query without ORDER BY has no defined order, and the C++ BE's parallel scan
  makes its own order vary.
- **Columns.** The tuple's slots by column unique id; columns absent from a segment (added by a
  later schema change) are refused in M5.
- **Conjuncts.** Evaluate every conjunct after reading. Key ranges, zone maps, the short key
  index, push-down aggregates, and runtime and top-n filters are optimisations; ignore them.
  `sort_info` / `read_orderby_key` can be ignored because the FE keeps the sort node.
- **Unique-key tablets.** The FE's statistics cache reads `__internal_schema.column_statistics`
  (a merge-on-write unique-key table) in 707 suites: 10,837 of the 12,928 single-fragment scans in
  the corpus, all of which return nothing. M5 reads unique-key tablets whose visible rowsets hold
  no rows (empty result); a unique-key tablet with rows is refused.
- **Encodings.** Today's reader decodes all 17 C++-written segments in the `cpp-loaded` fixture.
  M5 adds ZSTD and LZ4F page decompression. The C++ BE writes ZSTD pages once a flush passes
  256 KB, and after any compaction, so a root it has worked on will have them. LZ4 (raw), SNAPPY,
  ZLIB, PLAIN_V2 and V3 footers stay refused (reading-segments research §2.5).

### 4.4 Sort, limit, materialization

- **SORT_NODE.** Full sort and top-n (`use_topn`, `limit`, `offset`), per-key `is_asc` and
  `nulls_first`, with SQL ordering per type (strings byte-wise, NaN as the C++ BE orders it).
- **MATERIALIZATION_NODE** (258 suites' `SELECT * … ORDER BY … LIMIT`). The scan emits a row id
  column (`__DORIS_GLOBAL_ROWID_COL__…`); after the top-n, the node fetches the remaining columns
  for those row ids. The row id never leaves the BE, so doris-rs uses its own encoding and
  fetches in-process. The C++ BE's `multiget_data_v2` to itself is not needed.

### 4.5 Aggregation

- **Shapes.** One phase, or two phases over an exchange (update/serialize, then merge/finalize),
  with or without grouping keys; streaming pre-aggregation behaves as plain pre-aggregation.
- **State.** No intermediate state crosses a wire, so doris-rs picks its own. The merge phase
  rebuilds each function from `agg_expr.param_types`: the FE types partial-state slots as
  `VARCHAR(65533)`, which says nothing about the state.
- **Semantics.** The FE fixes `ret_type` and nullability. `count` skips NULLs; `count(*)` counts
  rows; `sum` widens as the FE says; `avg` of a decimal is DECIMAL128(38, max(s, 4)), truncated;
  `avg` of an integer or float is DOUBLE; a scalar aggregate over no rows is NULL when its output
  slot is nullable, and `count` is 0.

### 4.6 Schema scans

- **The node.** `TSchemaScanNode`: table name, db, FE `ip:port` (or `fe_addr_list`),
  `current_user_ident`, `thread_id`, catalog. The tuple holds all of the table's columns; slots
  map to scanner columns by case-insensitive name.
- **Scanners** (aggregation research, B.1):
  - CHARSETS and COLLATIONS: one static row each.
  - COLUMNS: `getDbNames`, `getTableNames` per db, `describeTables` per batch of ≤ 100 tables;
    DATA_TYPE, COLUMN_TYPE and COLUMN_SIZE as the C++ scanner computes them.
  - SCHEMATA: `getDbNames`.
  - TABLES: `getDbNames`, then `listTableStatus` per db with `required_columns`.
  - SESSION and GLOBAL VARIABLES: `showVariables{thread_id, var_type}`.
- **Calls.** Plain Thrift to the FE, with `current_user_ident` set. A failed call fails the
  scan, as in C++.

## 5. Reports

Tablet reports already carry versions and sizes (M4). The tables scan reads `index_length` from
the replicas' reported inverted-index sizes, so doris-rs keeps reporting 0 for it.

## 6. Fixtures

- The `cpp-loaded` capture (M4) gains the C++ BE's replies to `swap/readback-queries.sql`, if they
  are not already kept, and one table loaded with more than 256 KB per flush, so ZSTD pages are
  covered by a C++-written file.
- Row text tests take their expected strings from corpus `fetch_data` replies, not from
  hand-written values.

## 7. The readback check, second form

`make -C oracle readback-check` keeps its M4 form (the C++ BE reads doris-rs's loads) and adds:

1. The C++ BE loads the dataset; doris-rs starts on its root.
2. doris-rs answers `swap/readback-queries.sql`, minus the join (M6), and every reply equals the
   C++ BE's own replies to the same queries on the same data.
3. doris-rs also answers them on its own loads (the M4 run's root).

Replies without ORDER BY are compared as sorted rows, as in M4.

## 8. The M5 feature set and the gate

`scores/epic1-supported.json` gains 41 features:

- **RPCs:** `exec_plan_fragment_prepare`, `exec_plan_fragment_start`, `fetch_data`,
  `update_cache`, `fetch_cache`; FrontendService `getDbNames`, `getTableNames`, `describeTables`,
  `listTableStatus`, `showVariables`.
- **Nodes:** OLAP_SCAN, EXCHANGE, SORT, MATERIALIZATION, EMPTY_SET, SELECT, AGGREGATION,
  SCHEMA_SCAN.
- **Sinks:** RESULT_SINK, DATA_STREAM_SINK.
- **Expressions:** BINARY_PRED, COMPOUND_PRED, IN_PRED, AGG_EXPR, FUNCTION_CALL.
- **Functions:** `eq ne lt gt le ge is_null_pred is_not_null_pred substring count sum min max avg
  multi_distinct_count`.
- **State:** `state:global wait_timeout=31000`, which holds once `test_query_sys_tables` passes
  its line 175. When that suite passes, remove the supplement group from
  `needs_supplement.json`, as its note says.

The gate is every suite whose needs fall within that set (476 by today's index). These are upper
bounds, as in M4: a suite can still fail on something the index does not see. The ratchets record
what passes.

## 9. Left to the builder

- **Code layout.** The designer's guess:
  - a pipeline and operators in `doris-exec`;
  - the result buffer and the brpc handlers in `doris-be`;
  - the scan's rowset walk in `doris-storage`;
  - decompression in `doris-segment`;
  - schema scanners in `doris-exec`, with a FrontendService client in `doris-be`.
- **Execution model:** batches, threads, and how instances are scheduled.
- **Representations:** the row id encoding and the aggregation state format.
- **Order.** The order of the readback dataset's queries, within §7.

## 10. Calls made by the designer (not by the user)

1. M5 adds aggregation and the six schema scans to the spec's read loop, because together they
   take the gate from 209 to 476. Aggregation alone is +81; the `wait_timeout` suite is +177.
2. Joins are out of scope (+86) and are the first candidate for M6.
3. Unique-key tablets are read only when they hold no rows, to answer the FE's statistics probes.
   Full unique-key reads come with merge-on-write.
4. MATERIALIZATION fetches in-process with doris-rs's own row ids. No `multiget_data_v2`, and FE
   settings stay at the reference's values.
5. `fetch_cache` always misses and `update_cache` stores nothing. That is correct, only slower.
6. ZSTD and LZ4F are decoded now. LZ4, SNAPPY, ZLIB, PLAIN_V2 and V3 footers are refused until a
   suite needs them.
7. The second readback form leaves out the join query until M6.
