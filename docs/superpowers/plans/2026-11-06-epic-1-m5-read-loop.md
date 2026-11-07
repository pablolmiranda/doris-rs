# Epic 1 M5: The Read Loop — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** doris-rs answers the FE's SELECTs over its tablets: scans, filters, projections, sort, limit, top-n materialization, one- and two-phase aggregation, and the information_schema scans `test_query_sys_tables` needs, returning rows exactly as the C++ BE does.

**Architecture:**
- **Storage.** `doris-storage` gains a tablet reader: the version path to `v`, then segments in order, projected to the columns a scan needs.
- **Execution.** `doris-exec` gains row blocks, the MySQL row-text writer, read-side expressions, and query operators. Operators are connected per fragment instance by in-process exchanges.
- **BE.** `doris-be` gains a `QueryService` (prepare / start / exec, result buffers, `fetch_data`, cancel, failure reports, cache RPCs) wired to brpc, plus FrontendService schema calls in the FE client.
- **Proof.** Corpus suites are replayed in-process against doris-rs, and each query's `fetch_data` rows are compared with the reference's. The readback check gains its second form, in which doris-rs answers the queries.

**Tech Stack:** Rust workspace crates as in M4 (`doris-thrift` compact/binary protocols, `doris-proto` prost, `hyper`/`tokio` brpc), Python 3 and bash for the oracle, Docker (apache/doris:all-in-one-4.1.5).

**Spec:** `docs/superpowers/specs/2026-11-06-epic-1-m5-read-loop-design.md`. Research notes, cited below as **[wire]**, **[seg]** and **[agg]**, live in `docs/superpowers/specs/research/2026-11-06-m5-{queries-on-the-wire,reading-segments,aggregation-and-schema-scan}.md`.

## As built (differences from the tasks below)

The tasks below are the plan as written. Implementation changed these:

- **The replays.**
  - 60 suites are replayed in-process against `QueryService` and `LoadService` with `FakeFe`.
  - Each query's whole result is compared (rows of every packet, eos, error), not packet by
    packet: C++ packet boundaries follow scanner timing.
  - Captured order within a millisecond is unreliable, so replies are re-sorted by
    `packet_seq` and a query's exec/start runs before its first fetch.
  - A query the reference ended early (LIMIT_REACH) must have its rows contained in doris-rs's.
  - Suites are left out when they read unique/aggregate tables (other than the always-empty
    stats table), ask for binary rows, use `INSERT … SELECT` or the v1 two-phase read, or scan
    a schema table beyond the six.
  - Shape J (sort over aggregation over scan) is in no replayable suite. Unit tests and the
    full run cover it.
- **Interfaces.**
  - `version_path` returns `Option`; the caller names the tablet and version in `MissedVersion`.
  - `run_instance` takes a fragment and an instance index.
  - Row ids are `tablet | version | segment position in the scan | ordinal`.
    `fetch_by_row_ids` re-walks the same version path.
- **What the C++ BE does, learned from the corpus:**
  - year 0 is not a leap year;
  - DATETIMEV2 rounds half up to a lower scale, in casts and in text (M4's parser truncated);
  - a sort without slot exprs relabels its child's columns by position;
  - a scalar aggregation over no rows still gives one row;
  - `describeTables` sends `show_hidden_columns=false`;
  - dry-run queries count rows and send none;
  - a limit with `agg_sort_info_by_group_key` keeps the top groups;
  - an empty `jsonpaths` means none.
- **Added beyond the plan.** UNION over children (three replays use UNION ALL). The
  `information_schema.tables` times are refused when the FE sends one.
- **Errors.** Errors reach the FE as `INTERNAL_ERROR`, `NOT_IMPLEMENTED_ERROR` or `CANCELLED`
  with the code name, not the C++ BE's E-codes. A query's state is forgotten once its eos or
  failure is served. Exchange sender counts are checked against the plan.
- **The gate.** The needs index records what plans need that the types do not show
  (`read:UNIQUE_KEYS`, `read:AGG_KEYS`, `write:INSERT_SELECT`, `result:binary_rows`,
  `sort:two_phase_read`, `schema_table:<name>`). That takes the estimate from 476 to 370
  eligible suites, and the supplement to 348:
  - `state:fe enable_date_conversion=true` (18 suites) was added: `test_coalesce_new` leaks
    v1 date columns;
  - four single-suite needs were added;
  - the `wait_timeout` group was removed.
- **Results.**
  - Readback check: PASS, with rounds a/b/c of 26/25/25 replies equal.
  - Swap check: PASS.
  - `m5-full-1` found 23 out and 21 strict eligible failures. They were a top-n-by-group-key
    bug, empty jsonpaths, the date-conversion leak and four unsupported features.
  - `m5-full-2`, the final run: 348 eligible, failing 0 on both gates. Out passes 378 (211
    reaching the BE), strict 276 (196). `test_move_column_with_cast` left the out ratchet: it
    needs `task:ALTER` and had passed in only one run of six.
  - `m5-full-2` ran on 8bef912. The review fixes after it (query and exchange state forgotten
    once served, sender counts checked, the cancel/finish race) change no reply.

## Global Constraints

- **Plans.** A query runs only if every node, sink, expression, function and type in every fragment is in the M5 set (spec §8, plus M4's). Otherwise it is refused at prepare (or at `exec_plan_fragment`), with a status naming the first unsupported item, and nothing runs.
- **Exec RPC.** One fragment uses `exec_plan_fragment`; more than one uses `prepare` + `start`. `is_simplified_param` fragments take the first fragment's desc table, globals and coordinator ([wire] §1.3).
- **Never a wrong answer.** The scan refuses, with an error, rowsets with `delete_predicate`, unique- or aggregate-key tablets holding rows, columns missing from a segment, and the encodings and codecs [seg] §2.5 lists as unsupported.
- **Never a hang.** Every refusal or failure reaches the FE at once: the parked `fetch_data` fails with the raw status code, and one `reportExecStatus` is sent per fragment ([wire] §1.6). A successful query sends no report.
- **Row text.** Exactly the C++ BE's bytes per type ([wire] §3.2). Expected strings in tests come from corpus replies, never from hand-written guesses.
- **FE settings unchanged.** No session or FE config changes to avoid plan shapes.
- **No C or C++ dependencies** in `doris-segment`, `doris-storage` or `doris-exec`.
- **Process.** The repo is public: no credentials, and ask before posting to GitHub. `.cache/doris-src` and `.cache/rocksdb-5.14.2` are read-only. zsh doesn't word-split, and macOS has no `timeout`.
- **Commits** end with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XsjaVQNaumxtUfsAZPdeNs
  ```

## Review Focus

1. **A query that fails after the FE has parked a `fetch_data`.** Examples: a refused node in the second fragment, a scan error, a tablet that is missing. The fetch must fail at once with the status, not after the FE's query timeout. Pinned by Task 5's `a_failing_scan_fails_the_parked_fetch`.
2. **An exchange with several senders** (`per_exch_num_senders` > 1, several instances). It must wait for every sender's end before it ends, and must neither drop nor duplicate rows. Pinned by Task 5's `exchange_waits_for_every_sender`.
3. **NULL through predicates, IN lists, sort keys and aggregates.**
   - `x IN (1, NULL)` is NULL when x ≠ 1.
   - NOT of NULL is NULL.
   - Sorting puts NULL first or last as `nulls_first` says.
   - `count(x)` skips NULLs, and `sum` over only NULLs is NULL.
   
   Pinned by Task 3's `in_list_with_null_is_null_on_no_match`, Task 6's `nulls_first_per_key` and Task 7's `aggregates_skip_nulls`.
4. **A scan at a version the tablet cannot reach.** This happens when a publish is missing, or `v` is past the tablet's versions. The scan fails with the missed-version error, never returns fewer rows. Pinned by Task 4's `a_version_past_the_tablet_fails`.
5. **A result larger than one batch, and an empty result.** Packets are numbered from 0 without gaps, eos carries `returned_rows`, and an empty result is the eos packet alone. Pinned by Task 5's `results_span_packets` and `an_empty_result_is_one_eos_packet`.

---

### Task 1: Replay vectors from the corpus

**Files:**
- Create: `crates/doris-wire/src/bin/extract-replays.rs`, `crates/doris-fixtures/src/replays.rs`, `crates/doris-fixtures/replays/` (one directory per suite, plus `index.json`)
- Modify: `crates/doris-fixtures/src/lib.rs`
- Test: `crates/doris-fixtures/tests/replays.rs`

**Interfaces:**
- Produces:
  - **`Replay { suite: String, steps: Vec<Step> }`**, loaded by `doris_fixtures::replays::all() -> Vec<Replay>` and `replays::load(suite: &str) -> Replay`.
  - **`enum Step`**, one variant per message, in capture order:
    - `SubmitTasks(Vec<TAgentTaskRequest>)`;
    - `ExecPlanFragment { rpc: Rpc, request: PExecPlanFragmentRequest }`, where `enum Rpc { Exec, Prepare }`;
    - `Start(PExecPlanFragmentStartRequest)`;
    - `Fetch { request: PFetchDataRequest, reply: PFetchDataResult }`;
    - `Cancel(PCancelPlanFragmentRequest)`;
    - `UpdateCache`;
    - `FetchCache`.
  - **`Replay::fe_replies() -> FeReplies`**: what the reference's BE called on the FE, and what the FE answered. It is a map `(method, request bytes) → reply bytes`, falling back to `method → replies in order`. It covers `finishTask`, `reportExecStatus`, `getDbNames`, `getTableNames`, `describeTables`, `listTableStatus` and `showVariables`.
  - **`Replay::synthetic_tablets() -> Vec<TCreateTabletReq>`**: one for each tablet a scan reads that the suite never created (the FE's `__internal_schema.column_statistics`). It is built from the scan node and the desc table (keys type, columns, `enable_unique_key_merge_on_write`), with `version` set to the highest version scanned.

- [ ] **Step 1: Write the failing test** `tests/replays.rs`:
  - `every_replay_loads`: `all()` holds at least 40 replays, and each has at least one `Fetch` whose reply has `eos == Some(true)`.
  - `shapes_are_covered`: across all replays, each of the [wire] §6 shapes A, B, C, D, E, F, H, I, J and K appears at least once, as does each function in spec §8.
  - `sys_tables_stops_before_the_set`: `load("nereids_p0.system.test_query_sys_tables.test_query_sys_tables")` has 6 result queries (charsets, collations, columns, schemata, tables, session_variables), and no step after the session_variables fetch.
- [ ] **Step 2:** Run `cargo test -p doris-fixtures --test replays`. Expected: FAIL, unresolved `replays`.
- [ ] **Step 3: Write `extract-replays`.** It reuses `extract-values`' corpus reading. It picks gate suites (`oracle/epic1/suites.txt`) that meet three conditions:
  - every BE-bound message is one of the `Step` kinds above (so no stream loads, joins or other functions);
  - every plan is within the M5 set;
  - the replay holds between 1 and 40 result queries.
  
  Selection order: first the sys_tables suite, cut after its session_variables fetch; then the smallest suite for each uncovered shape or function; then the smallest remaining suites, up to 60. It writes each message's raw bytes and an `index.json` per suite.
- [ ] **Step 4:** Run `cargo run -p doris-wire --bin extract-replays -- crates/doris-fixtures/replays`, then the test. Expected: PASS. Keep the replays directory under 20 MB (`du -sh`). If it is larger, lower the cap of 60 and ledger it.
- [ ] **Step 5: Commit** (`doris-fixtures: query replays from the corpus`).

### Task 2: Blocks, row text and result batches

**Files:**
- Create: `crates/doris-exec/src/block.rs`, `crates/doris-exec/src/row_text.rs`
- Modify: `crates/doris-exec/src/lib.rs`
- Test: `crates/doris-exec/tests/row_text.rs`

**Interfaces:**
- Consumes: `doris_segment::writer::Datum`, `doris_exec::value::ColType` (M4), and Task 1's replays.
- Produces:
  - `pub struct Block { pub types: Vec<ColType>, pub rows: Vec<Vec<Datum>> }`. A value's meaning comes from its column's `ColType`:
    - decimals are unscaled `Int`;
    - DATEV2 is a packed u32 and DATETIMEV2 a packed u64, both as `Int`;
    - strings are `Bytes` without CHAR padding;
    - FLOAT is `Float` and DOUBLE is `Double`.
  - `pub fn cell_text(d: &Datum, t: &ColType) -> Option<Vec<u8>>`, where `None` means NULL.
  - `pub fn result_batch(block: &Block) -> Vec<u8>`: a binary-protocol `TResultBatch` whose rows are length-encoded cells, with `0xFB` for NULL ([wire] §3.1–3.2).

- [ ] **Step 1: Write the failing tests:**
  - `samples_render_as_the_reference`: one case per [wire] §3.2 sample. For example, `Double(0.1)` renders `0.1`, `Double(1e16)` renders `1e+16`, `Float(1.5)` renders `1.5`, decimal(9,3) `1500` renders `1.500`, and DATETIMEV2(3) prints three fraction digits. Copy every sample in that table verbatim.
  - `every_corpus_cell_round_trips`: for every replay `Fetch` reply, decode its `TResultBatch` and get each column's `ColType` from the query's result-sink `output_exprs`. For each cell, `parse_text` (M4) followed by `to_datum` gives a `Datum`, and `cell_text` of that `Datum` must equal the recorded bytes. Skip only the types outside M4 and say so in the assertion message.
  - `result_batch_round_trips`: `result_batch` of the decoded rows of every corpus reply equals the reply's `row_batch` bytes.
- [ ] **Step 2:** Run `cargo test -p doris-exec --test row_text`. Expected: FAIL, unresolved `row_text`.
- [ ] **Step 3: Implement** `block.rs` and `row_text.rs`. Floats need fmt 7.1.3's shortest round-trip form: Rust's shortest `{}` digits, re-laid-out with fmt's exponent thresholds (below 1e-4, or 1e16 and above, use `e+NN`/`e-NN` with at least two exponent digits).
- [ ] **Step 4:** Run the tests. Expected: PASS. A mismatch is fixed in `cell_text`, never by editing the expected bytes.
- [ ] **Step 5: Commit** (`doris-exec: blocks, MySQL row text, result batches`).

### Task 3: Read-side expressions

**Files:**
- Create: `crates/doris-exec/src/eval.rs`
- Modify: `crates/doris-exec/src/lib.rs`
- Test: `crates/doris-exec/tests/eval.rs`

**Interfaces:**
- Consumes: `Block` (Task 2), `ColType::from_type_desc` (M4).
- Produces:
  - `pub struct SlotMap(HashMap<i32, usize>)`, mapping a slot id to a column index in the input block.
  - `pub fn check_read(e: &TExpr) -> Result<(), ExecError>`. It accepts:
    - M4's literals, SLOT_REF and CAST_EXPR (between M4 types);
    - BINARY_PRED (`eq ne lt gt le ge`), COMPOUND_PRED (`and or not`) and IN_PRED (`in`, `not in`);
    - `is_null_pred` and `is_not_null_pred`;
    - FUNCTION_CALL `substring`.
    
    Anything else is `Unsupported` naming the node or function.
  - `pub fn eval_column(e: &TExpr, block: &Block, slots: &SlotMap) -> Result<(ColType, Vec<Datum>), ExecError>`.
  - `pub fn filter(conjuncts: &[TExpr], block: Block, slots: &SlotMap) -> Result<Block, ExecError>`. It keeps a row only when every conjunct is true; false and NULL drop it.

- [ ] **Step 1: Write the failing tests:**
  - `comparisons_per_type`: on a block holding every M4 type, `eq`, `lt` and `ge` against literals give the expected rows. Decimal literals are scaled to the node's type, and dates are packed from their literal text.
  - `three_valued_logic`: `NULL and false` is false, `NULL or true` is true, and `not NULL` is NULL.
  - `in_list_with_null_is_null_on_no_match`: `x IN (1, NULL)` is true for 1, NULL for 2 and NULL for NULL. `filter` keeps only the row holding 1.
  - `substring_counts_characters`: `substring('héllo', 2, 3)` is `éll`.
  - `unknown_function_is_refused`: `check_read` of a FUNCTION_CALL `like` is `Unsupported` with `function like` in the message.
- [ ] **Step 2:** Run `cargo test -p doris-exec --test eval`. Expected: FAIL, unresolved `eval`.
- [ ] **Step 3: Implement `eval.rs`.**
  - Compare values by type: integers and dates numerically, decimals at the common scale the FE's casts give, strings byte-wise, and floats with IEEE `<`. For NaN, follow the C++ BE: NaN = NaN, and NaN sorts greater than any number.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5: Commit** (`doris-exec: read-side predicates and expressions`).

### Task 4: The tablet reader

**Files:**
- Create: `crates/doris-storage/src/reader.rs`
- Modify:
  - `crates/doris-storage/src/lib.rs`, `crates/doris-storage/src/error.rs`;
  - `oracle/swap/load-dataset.sql`: add `m5.zstd`, a table of 60,000 rows of four columns by one stream load, so a flush passes 256 KB;
  - `oracle/swap/capture-loaded.sh`, if it needs changing.
- Test: `crates/doris-storage/tests/reader.rs`

**Interfaces:**
- Consumes: `TabletManager::tablet` and `tablet_path` (M4), `doris_segment::{SegmentFile, column::read_column}`.
- Produces:
  - `pub fn version_path(rowsets: &[RowsetMetaPb], v: i64) -> Result<Vec<&RowsetMetaPb>, StorageError>`. It walks from 0 to `v`, taking the widest rowset that does not pass `v` at each step, visible rowsets before stale ones ([seg] §1.2).
  - `pub struct ScanColumn { pub unique_id: u32, pub ty: ColTypeRef }`. `ColTypeRef` is whatever `doris-storage` needs to build `Datum`s without depending on `doris-exec`: field type, precision and scale.
  - `pub fn scan_tablet(mgr: &TabletManager, tablet_id: i64, version: i64, columns: &[ScanColumn]) -> Result<Vec<Vec<Vec<Datum>>>, StorageError>`. It returns one row vector per segment, in rowset-version order, then segment order.
  - New `StorageError` variants:
    - `MissedVersion { tablet_id, version }`, whose message is the C++ text from [seg] §1.2;
    - `UnsupportedRead { tablet_id, what }`.

- [ ] **Step 1: Write the failing tests:**
  - `cpp_loaded_tables_read_back`: open the `cpp-loaded` fixture root and scan every tablet of each `m4.*` table at its visible version. The sorted rows, rendered as TSV, equal `replies/` for that table's `SELECT *` query.
  - `zstd_pages_read_back`: the same check for `m5.zstd`. Also assert that at least one page in its segments is ZSTD-compressed.
  - `version_path_picks_the_widest`: rowsets `[0,1] [2,2] [3,3] [2,3]` at `v = 3` give `[0,1] [2,3]`.
  - `a_version_past_the_tablet_fails`: scanning at `v = max + 1` is `MissedVersion`.
  - `unique_key_with_rows_is_refused` and `unique_key_without_rows_reads_empty`.
  - `delete_predicate_is_refused`.
- [ ] **Step 2:** Run `make -C oracle loaded-fixture`. Expected: `cpp-loaded/` gains `m5.zstd`'s tablets and replies, and the M4 storage tests still pass.
- [ ] **Step 3:** Run `cargo test -p doris-storage --test reader`. Expected: FAIL, unresolved `reader`.
- [ ] **Step 4: Implement `reader.rs`.** Convert `ColumnValues` into `Datum`s per Task 2's conventions. CHAR loses its trailing zero padding.
- [ ] **Step 5:** Run the tests. Expected: PASS.
- [ ] **Step 6: Commit** (`doris-storage: tablet reader; cpp-loaded gains a ZSTD table`).

### Task 5: Query plans, the pipeline core, and the QueryService

**Files:**
- Create:
  - `crates/doris-exec/src/query.rs`: plan decoding and the plan check;
  - `crates/doris-exec/src/pipeline.rs`: instances and operators;
  - `crates/doris-exec/src/exchange.rs`;
  - `crates/doris-be/src/query.rs`.
- Modify: `crates/doris-be/src/brpc.rs`, `crates/doris-be/src/be.rs`, `crates/doris-be/src/lib.rs`, `crates/doris-be/src/load.rs` (route non-write single fragments to the QueryService)
- Test: `crates/doris-be/tests/query.rs`, and `crates/doris-be/tests/replay.rs`, the replay driver.

**Interfaces:**
- Consumes: Tasks 1–4. M4's `decode_fragments`, `AgentService` and `LoadService`, and `FakeFe`.
- Produces:
  - `pub struct QueryPlan` from `pub fn plan_query(list: &TPipelineFragmentParamsList) -> Result<QueryPlan, ExecError>`. It covers fragments, instances, scan ranges `(tablet_id, version)`, the desc table and query options.
  - `pub fn run_instance(plan: &QueryPlan, instance: usize, ctx: &ExecCtx) -> Result<(), ExecError>`. `ExecCtx` holds:
    - `scan: &dyn Fn(i64, i64, &[ScanColumn]) -> Result<…, StorageError>`;
    - the exchange registry;
    - the result sink, `&dyn Fn(Block)`.
  - `pub struct Exchanges`: an in-process registry keyed by `(query_id, dest_instance_id, node_id)`. Each key has its own channel. A receiver ends once `per_exch_num_senders` ends have arrived.
  - Operators in this task:
    - OLAP_SCAN, with conjuncts (Task 3), projections and limit;
    - SELECT_NODE and EMPTY_SET_NODE;
    - UNION_NODE with constants only (reusing M4's literal evaluation);
    - EXCHANGE_NODE and DATA_STREAM_SINK, UNPARTITIONED only;
    - RESULT_SINK, which evaluates `output_exprs`.
  - `pub struct QueryService` with:
    - `exec_plan_fragment(&PExecPlanFragmentRequest) -> PExecPlanFragmentResult` and `prepare(…)`;
    - `start(&PExecPlanFragmentStartRequest) -> PExecPlanFragmentResult`;
    - `fetch_data(&PFetchDataRequest) -> PFetchDataResult`, which blocks until a packet or the end;
    - `cancel(&PCancelPlanFragmentRequest)`;
    - `update_cache() -> PCacheResponse{CACHE_OK}` and `fetch_cache() -> PFetchCacheResult{NO_SQL_KEY}`.
    
    Failures go through `FeClient::report_exec_status`, with `query_type SELECT`.
  - In `tests/replay.rs`: `fn replay(r: &Replay) -> Vec<Mismatch>`. It runs the replay's steps against an in-process BE (AgentService + LoadService + QueryService with a `FakeFe` serving `fe_replies()`), after creating `synthetic_tablets()`.
    - Each `Fetch` reply is compared on eos, `packet_seq` and rows.
    - Rows are compared exactly when the plan has a sort or a merging exchange, and as multisets otherwise.
    - It also exposes `replays_with_shapes(&[char])`.

- [ ] **Step 1: Write the failing tests** in `tests/query.rs`:
  - `select_star_returns_inserted_rows`: CREATE, INSERT (M4) and publish, then a single-fragment scan returns the rows.
  - `results_span_packets`: 10,000 rows with `batch_size` 4064 give packets 0, 1 and 2, then eos with `packet_seq 3` and `returned_rows 10000`.
  - `an_empty_result_is_one_eos_packet`.
  - `fetch_before_start_waits`: a fetch issued after prepare but before start returns rows once start arrives.
  - `a_failing_scan_fails_the_parked_fetch`: a scan at a missing version fails the waiting fetch within 1 s with the status code. FakeFe records one `reportExecStatus` per fragment, with `done` set.
  - `exchange_waits_for_every_sender`: 3 sender instances into one exchange deliver all their rows, once each.
  - `unsupported_plans_are_refused_at_prepare`: a HASH_JOIN_NODE plan is refused, naming `HASH_JOIN_NODE`, and nothing runs.
  - `a_fetch_for_an_unknown_query_fails`.
  
  In `tests/replay.rs`, `shapes_a_e_h_match_the_reference`: every replay whose shapes are all in {A, E, H} has no mismatches.
- [ ] **Step 2:** Run `cargo test -p doris-be --test query --test replay`. Expected: FAIL, unresolved `QueryService`.
- [ ] **Step 3: Implement** the plan decoding, the operators listed above, `Exchanges`, `QueryService`, and the brpc routes: `exec_plan_fragment_prepare`, `exec_plan_fragment_start`, `fetch_data`, `update_cache` and `fetch_cache`. `exec_plan_fragment` dispatches on the sink: OLAP_TABLE_SINK goes to LoadService, anything else to QueryService.
- [ ] **Step 4:** Run the tests. Expected: PASS, and all M4 tests still pass (`cargo test`).
- [ ] **Step 5: Commit** (`doris-be: query fragments, result buffers, fetch_data; scans, exchanges, result sink`).

### Task 6: Sort, merging and partitioned exchanges, materialization

**Files:**
- Create: `crates/doris-exec/src/sort.rs`, `crates/doris-exec/src/materialize.rs`
- Modify: `crates/doris-exec/src/pipeline.rs`, `crates/doris-exec/src/exchange.rs`, `crates/doris-exec/src/query.rs`, `crates/doris-storage/src/reader.rs` (fetch rows by row id)
- Test: `crates/doris-exec/tests/sort.rs`, plus the replay test

**Interfaces:**
- Consumes: Task 5's pipeline and Task 4's reader.
- Produces:
  - **SORT_NODE:** a full sort, or a top-n (`use_topn`, `limit`, `offset`), with per-key `is_asc` and `nulls_first`.
  - **Merging EXCHANGE_NODE:** used when `sort_info` is set; it merges the sorted inputs, then applies offset and limit.
  - **DATA_STREAM_SINK partitioning:** HASH_PARTITIONED, by any stable hash of the partition exprs mod the number of destinations, and RANDOM, round robin.
  - **Row ids.** `pub fn row_id(tablet_id: i64, rowset: usize, segment: u32, ordinal: u32) -> Vec<u8>` in doris-rs's own format (spec §4.4), with `pub fn fetch_by_row_ids(mgr, ids: &[Vec<u8>], columns: &[ScanColumn]) -> Result<Vec<Vec<Datum>>, StorageError>` in `reader.rs`.
  - **MATERIALIZATION_NODE:** the scan fills the row-id slot; the node fetches the other slots for the rows that survive the top-n.

- [ ] **Step 1: Write the failing tests:**
  - `nulls_first_per_key`: keys `(a asc nulls first, b desc nulls last)` order a fixed block as expected.
  - `topn_with_offset`: `limit 3 offset 2` over 10 rows gives rows 3–5 of the sorted order.
  - `merge_of_sorted_inputs`: 3 sorted senders merge into one sorted stream.
  - `materialization_fetches_the_rest`: a scan emitting `(k, row_id)`, then a top-n on `k`, then materialization of `v`, gives the same rows as sorting the full rows.
  
  In the replay test, `shapes_b_c_match_the_reference`.
- [ ] **Step 2:** Run `cargo test -p doris-exec --test sort` and `cargo test -p doris-be --test replay`. Expected: FAIL.
- [ ] **Step 3: Implement**, ordering values per type as in Task 3's comparison.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5: Commit** (`doris-exec: sort, top-n, merging and partitioned exchanges, materialization`).

### Task 7: Aggregation

**Files:**
- Create: `crates/doris-exec/src/aggregate.rs`
- Modify: `crates/doris-exec/src/pipeline.rs`, `crates/doris-exec/src/query.rs`, `crates/doris-exec/src/eval.rs` (`AGG_EXPR` passes `check_read` only inside an aggregation node)
- Test: `crates/doris-exec/tests/aggregate.rs`, plus the replay test

**Interfaces:**
- Consumes: Tasks 3, 5 and 6.
- Produces:
  - AGGREGATION_NODE in all three modes:
    - update + finalize, one phase;
    - update + serialize, the first phase (`need_finalize false`);
    - merge + finalize, the second phase (`is_merge` / not `is_first_phase`).
  - Grouping exprs. Streaming pre-aggregation behaves as plain pre-aggregation.
  - Functions: `count` (with and without args), `sum`, `min`, `max`, `avg`, `multi_distinct_count`.
  - State is a Rust enum per function, carried between phases as a `Datum::Bytes` holding doris-rs's own encoding. The merge phase picks each function from `agg_expr.param_types`, never from the `VARCHAR(65533)` slot type ([agg] A.3).
  - Result types and nullability come from the FE ([agg] A.2).

- [ ] **Step 1: Write the failing tests:**
  - `count_star_over_two_phases`: 3 instances of the first phase, then a merge, give the row count.
  - `aggregates_skip_nulls`: over `[1, NULL, 3]`:
    - `count(x)` is 2 and `count(*)` is 3;
    - `sum` is 4, `min` 1 and `max` 3;
    - `avg` is `2` as DOUBLE.
    
    Over `[NULL]`, `sum` is NULL.
  - `empty_input`: scalar `count(*)` is 0 and `sum` is NULL. With grouping keys, there are no rows.
  - `avg_of_decimal_truncates`: avg of decimal(9,2) `[1.00, 2.00, 2.00]` is `1.6666` at DECIMAL128(38, 4).
  - `count_distinct`: `multi_distinct_count` over `[1, 1, NULL, 2]` is 2.
  
  In the replay test, `shapes_d_i_j_k_match_the_reference`.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3: Implement** `aggregate.rs`, with `sum` widening to the FE's `ret_type` and checked overflow (an error, never a wrap).
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5: Commit** (`doris-exec: one- and two-phase aggregation`).

### Task 8: Schema scans

**Files:**
- Create: `crates/doris-exec/src/schema_scan.rs`
- Modify:
  - `crates/doris-be/src/fe_client.rs`: `get_db_names`, `get_table_names`, `describe_tables`, `list_table_status` and `show_variables`, on `FeClient` and `ThriftFeClient`, to the node's FE address, not to the master;
  - `crates/doris-be/src/testing.rs`: FakeFe serves recorded replies;
  - `crates/doris-exec/src/pipeline.rs`, `crates/doris-exec/src/query.rs`.
- Test: `crates/doris-exec/tests/schema_scan.rs`, plus the replay test

**Interfaces:**
- Consumes: Task 5's `ExecCtx` (it gains `fe: &dyn SchemaFe`, a doris-exec trait with the five calls, which doris-be implements over `FeClient`).
- Produces: SCHEMA_SCAN_NODE for CHARSETS, COLLATIONS, COLUMNS, SCHEMATA, TABLES and SESSION/GLOBAL VARIABLES, as described in [agg] B.1:
  - the static rows;
  - the RPC order, with `describeTables` batches of ≤ 100 tables;
  - DATA_TYPE, COLUMN_TYPE and COLUMN_SIZE computed as `schema_columns_scanner.cpp` computes them;
  - tuple slots mapped by case-insensitive name.
  
  Any other schema table is `Unsupported` naming it.

- [ ] **Step 1: Write the failing tests:**
  - `columns_rows_from_describe_tables`: the describeTables reply of [agg] B.1 gives `internal aaa 1 varchar varchar(170) 170`, then `bbb 2 varchar varchar(20) 20`, then `ccc 3 int int(11) 10`, then `ddd 4 smallint smallint(6) 5`.
  - `describe_tables_batches_of_100`: 250 tables give 3 calls, of 100, 100 and 50 tables.
  - `variables_need_four_fields`: a `showVariables` row with 3 fields is an error.
  - `unknown_schema_table_is_refused`.
  
  In the replay test, `sys_tables_matches_the_reference_up_to_the_set`: the sys_tables replay has no mismatches.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run the tests, then the whole replay set. Expected: `every_replay_matches_the_reference` (all replays, no mismatches) passes.
- [ ] **Step 5: Commit** (`doris-exec: information_schema scans; FE schema calls`).

### Task 9: The readback check, second form

**Files:**
- Modify: `oracle/swap/readback-check.sh`, `oracle/swap/lib.sh` if needed, `oracle/README.md` (the readback paragraph)
- Test: `make -C oracle readback-check`

**Interfaces:**
- Consumes: the M4 readback check, and `readback-queries.sql` (the join query is the one named `*_join*`, or whichever line joins two tables).

- [ ] **Step 1:** Extend `readback-check.sh` with two rounds after the M4 round:
  - **(b)** The C++ BE loads the dataset and stops. doris-rs starts on its root and runs the queries minus the join. Every reply equals the C++ BE's reply on the same root.
  - **(c)** doris-rs answers the same queries on the root it loaded itself in the M4 round. The replies equal the C++ BE's.
  
  Replies without ORDER BY are compared as sorted rows.
- [ ] **Step 2:** Run `make -C oracle readback-check`. Expected: last line `readback check: PASS`, with three rounds reported. A mismatch is fixed in doris-rs, never by editing the queries.
- [ ] **Step 3:** Run `make -C oracle swap-check`. Expected: `swap check: PASS`.
- [ ] **Step 4: Commit** (`oracle: readback check, second form (doris-rs answers)`).

### Task 10: The gate, docs, As built

**Files:**
- Modify:
  - `oracle/scores/epic1-supported.json`: the 41 features of spec §8;
  - `oracle/needs_supplement.json`: drop the `wait_timeout` group once `test_query_sys_tables` passes;
  - `oracle/scores/epic1-out.txt` and `oracle/scores/epic1-strict.txt`;
  - `oracle/README.md` and this plan (an "As built" section).

- [ ] **Step 1:** Add the features. Run `python3 oracle/checker/rank.py oracle/epic1` and check that the eligible count printed by grade's dry run is 476. If it differs, explain the difference in the ledger before going on.
- [ ] **Step 2:** Run the full epic run on doris-rs and grade it, as in M4 (`m5-full-1`). Expected: every eligible suite passes both gates. Triage each failure: either fix it (test first), or show that its needs are wrong (supplement entry with evidence), and ledger it.
- [ ] **Step 3:** Once `nereids_p0.system.test_query_sys_tables` passes, remove the `wait_timeout` supplement group, re-rank and re-grade. Expected: still 0 eligible failures.
- [ ] **Step 4:** Regenerate the ratchets from the final grade. Write the M5 paragraph in `oracle/README.md` and "As built" in this plan, with the run's numbers.
- [ ] **Step 5:** Run `cargo test`, `cargo clippy --all-targets`, `make -C oracle test`, the readback check and the swap check. Expected: all green.
- [ ] **Step 6: Commit** (`M5: gate, docs, As built`).
