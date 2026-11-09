# Epic 1 M6 Story 4: INSERT … SELECT — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** doris-rs writes a query's rows into a table (`write:INSERT_SELECT`) and answers as the C++ BE
does. It also fixes what keeps the newly eligible suites from passing: v1 DATE/DATETIME writes (the
`enable_date_conversion` leak), Parquet and ORC stream loads, and two stream-load cast gaps. Eligible
goes 378 → 414, all passing both gates.

**Architecture:**
- The table sink becomes an accumulator, `sink::TableSink`, that any number of instances feed. When
  the last instance has finished, it writes and commits once, as `VTabletWriterV2` does.
- `INSERT … VALUES` and stream load keep their path through `LoadService`. Every other plan with an
  `OLAP_TABLE_SINK` runs in the query pipeline: the sink fragment's instances project their output
  exprs and push the rows into the fragment's `TableSink`.
- The query service then sends one `reportExecStatus` per fragment, of type LOAD, shaped as
  `PipelineFragmentContext::_coordinator_callback` builds it.
- v1 dates stay v2-packed in memory and become v1 only at the storage boundary (page bytes, key
  coder) and in the bucket hash (their text). v1 reads stay refused.
- Parquet and ORC bodies are read with the `parquet` and `orc-rust` crates into the text C++ casts
  each value to. The existing CSV/JSON cast path does the rest.

**Tech Stack:** Rust (`doris-segment`, `doris-exec`, `doris-be`, `doris-fixtures`, `doris-wire`),
new crates `parquet` (53, default features off) and `orc-rust` (0.6, which brings arrow 56); Python
oracle.

**Spec:** `docs/superpowers/specs/2026-11-08-epic-1-m6-query-breadth-design.md` §2, §3, §5. The
user widened this story on 2026-11-08: "Fix them in story 4 too" (the 10 suites that fail for
other reasons), and "Use crates" for the Parquet and ORC readers.

## Research

**Recount** (`checker/derive.py`, supported = `scores/epic1-supported.json` + `write:INSERT_SELECT`,
suites = `epic1/suites.txt`):
- 18 suites need only `write:INSERT_SELECT`.
- With the supplement group `state:fe enable_date_conversion=true` removed (Task 1), 18 more have no
  remaining needs.
- 378 + 18 + 18 = **414**.

**The INSERT … SELECT plans in those suites** (56 queries, decoded from `oracle/epic1/traffic`):

| Shape | Count | RPCs | Fragments × instances | Plan | Where |
|---|---|---|---|---|---|
| S0 | 47 | prepare + start | F1 ×1 ← F0 ×1 | F1: EXCHANGE (OLAP_TABLE_SINK_HASH_PARTITIONED) → OLAP_TABLE_SINK; F0: OLAP_SCAN of `column_statistics` → DATA_STREAM_SINK | the FE's own `DELETE FROM __internal_schema.column_statistics …` after TRUNCATE, a column rename or a light schema change: a partial update with `__DORIS_DELETE_SIGN__` into a merge-on-write table. Run on an FE thread; its errors are only logged and never reach a client. |
| S1 | 1 | `exec_plan_fragment` | F0 ×4 | OLAP_SCAN (DUP, conjunct `lt`) → OLAP_TABLE_SINK | docs_demo `INSERT INTO dst SELECT * FROM src WHERE age < 30` |
| S2 | 5 | prepare + start | F1 ×1 ← F0 ×4 | F1: EXCHANGE (UNPARTITIONED) → OLAP_TABLE_SINK; F0: OLAP_SCAN → DATA_STREAM_SINK (UNPARTITIONED) | fail_url `INSERT INTO src SELECT * FROM src`; role_mapping `insert into tbl(...) select 2, …` |
| S3 | 3 | `exec_plan_fragment` | F0 ×4 | OLAP_SCAN (DUP) → OLAP_TABLE_SINK | fail_url, clean_label `insert … with LABEL … select * from …` |

- All of them run with `enable_insert_strict=true`, `query_type=LOAD`, `load_zero_tolerance=true`
  and `time_zone="UTC"`. None of them uses group commit, automatic partitions or
  `load_to_single_tablet`.
- Target tables are DUP_KEYS, either unpartitioned or RANGE on `id` (fail_url's `dst`, 2 partitions).
- Today, doris-rs refuses S1 and S3 in `plan_write` ("OLAP_SCAN_NODE"), and S2 in `check_plan`
  ("OLAP_TABLE_SINK"). It refuses S0 at its exchange's partition type.

**The C++ reports** (`pipeline_fragment_context.cpp` `send_report`, `_coordinator_callback` :1992;
`vtablet_writer_v2.cpp` `close` :718–781):
- **One report per fragment, not per instance.**
  - Every report carries `fragment_instance_id` (0,0), `backend_num` −1, `done` true,
    `query_type` LOAD, `loaded_rows` 0, `loaded_bytes` 0, `backend_id`, `fragment_id`, and the
    status.
  - Scan-only fragments report too: the FE waits on every (fragment, backend) pair
    (LoadProcessor.java:84–90).
- **The sink fragment's report adds:**
  - `load_counters` `dpp.norm.ALL` / `dpp.abnorm.ALL` / `unselected.rows`, summed over the instances;
  - one `TFragmentInstanceReport{fragment_instance_id, num_finished_range 0, loaded_rows,
    loaded_bytes}` per instance that had rows. `loaded_rows` counts rows that reached the sink,
    filtered ones included; `loaded_bytes` is `Block::bytes` at the sink.
  - Both are sent only when some instance had rows.
- `commit_infos` are sent only when not empty. They list every tablet of every index of each
  touched partition.
- `tracking_url` and `first_error_msg` are sent only when set.
- Example, docs_demo: 4 instances, 3 rows on 2 of them. One report lists 10 tablets, with instance
  reports (2 rows, 44 bytes) and (1 row, 24 bytes), and `dpp.norm.ALL=3`.
- The FE uses the first done report per (backend, fragment) and sums them. Its SHOW LOAD JobDetails
  `{"ScannedRows":5,"LoadBytes":121,…}` (clean_label exchange 20, compared in the strict gate) is
  the sum of the instance reports.
- **Today**, doris-rs's VALUES/stream path always sends counters, one instance report and
  `commit_infos: Some(..)`, even when empty. Its query service reports nothing on success, and
  SELECT on failure.

**Too many errors** (`RuntimeState::append_error_msg_to_file`, runtime_state.cpp:385–421):
- With `load_zero_tolerance`, once more than 50 error rows have been logged, the 52nd error row
  cancels the load. fail_url query 6: 96 rows, 64 errors.
- The report's status is CANCELLED, with the text `(<localhost>)[CANCELLED]load_id=<hi hex>-<lo hex>,
  reason: [DATA_QUALITY_ERROR]Encountered unqualified data, stop processing. Please check if the
  source data matches the schema, and consider disabling strict mode or increasing max_filter_ratio.`
  The `load_id` is the query id, printed as `print_id`.
- The same report carries `dpp.norm.ALL` = the rows that reached the sink (96), `dpp.abnorm.ALL`=0,
  the instance report (96 rows), `tracking_url` and `first_error_msg`, and no commit infos.
- The error log is named with db `insert_stmt` and label `print_id(fragment_instance_id)`
  (runtime_state.cpp:289–290).

**v1 dates** (`storage/types.h:157,182`, `key_coder.h:132`, `olap_data_convertor.cpp:716,764`,
`util/raw_value.h:83-89`). The 25 suites that the date leak fails were all planned on DATEV2 by the
reference.
- **How it leaks.** `test_coalesce_new` sets `enable_date_conversion=false` at line 21. It then fails
  at line 43, `insert into test_cls values …`, with a DATETIME v1 `dt` in a UNIQUE MoW table:
  "doris-rs does not support column dt of type TPrimitiveType(10)". So it never reaches line 49,
  which sets the config back.
- **What line 43 needs.** Only the write: the CREATE already succeeds. Its later reads are in
  `try_sql`, which tolerates a refusal.
- **Formats:**
  - DATE v1: 3 bytes LE of `year<<9 | month<<5 | day`. Its key is 3 bytes big-endian, with no sign
    flip.
  - DATETIME v1: an i64 LE `YYYYMMDDhhmmss`. Its key uses the sign-flipped big-endian integer coder.
  - Both use PLAIN pages.
  - The bucket hash of a v1 value is the crc of its text (`YYYY-MM-DD[ HH:MM:SS]`).
- `doris-segment` already reads v1 (`column.rs:343-354`, `values.rs`). `datums()` gives v2-packed
  `Datum::Int`.

**Stream-load gaps** (from the recorded HTTP exchanges):
- `test_load_with_decimal`:
  - Loads a Parquet file: parquet-mr 1.10, 1 row group, UNCOMPRESSED, PLAIN pages, RLE definition
    levels, `id` INT32, and FLBA decimals (8,4) (18,6) (38,12) (9,0) (27,9) (9,0), one NULL per
    column.
  - Loads an ORC file: ZLIB, 1 stripe, DIRECT_V2. Its decimals have per-value scales and must be
    rescaled to the column's scale.
  - All four loads are Success 5/5/0.
  - For Parquet and ORC, the C++ BE sends the streamLoadPut with `file_type=FILE_LOCAL`, a
    `mini_download` `path` and `file_size` (stream_load.cpp:443-466).
  - The FE plans every source slot as VARCHAR and each destination as `CAST(slot AS decimal)`. So
    each file value must be the text C++ prints for it, e.g. `1234.0000` at the file's scale.
    Columns match by lower-cased name.
- `test_stream_load_cast` (once types are DATEV2):
  - A string to int cast must accept a fractional suffix: `"3.12"` → 3, as in
    `StringParser::string_to_int_internal` (string_parser.hpp:462, 477–549).
  - Overflow must still fail.
  - Float overflow must give ±Infinity.
  - `2024-4-2` must parse as `2024-04-02`; `2024-14-2`, `2024-4-222` and `27:00:00` must fail.
- `test_stream_load_strict_mode_and_filter_ratio` load 21:
  - Its `columns` is `k00=cast(concat('invalid', k01) as bigint)`, which needs `concat` in load
    expressions and a decimal → VARCHAR cast at full scale.
  - The strict filter is C++'s rule (`file_scanner.cpp` `_convert_to_output_block` ~904–930): only a
    destination in `dest_sid_to_src_sid_without_trans`, whose source is not NULL and whose result is
    NULL, filters its row.
  - Expected result: 10/7/3, Success.

## Global Constraints

- Byte-exact replies (spec §3): a feature that can't match the C++ BE is refused with a status naming
  it. A wrong answer is never acceptable.
- A plan that holds anything doris-rs does not run is refused at prepare, naming it (spec §3).
- No regression: the ratchets hold, and every eligible suite passes both gates in the full run. The
  swap and readback checks stay green.
- `type:DATE` and `type:DATETIME` stay out of `scores/epic1-supported.json`: v1 reads stay refused.
  `function:concat` stays out too: query-side `concat` is story 5.
- Never `cargo fmt` whole packages. New files only: `rustfmt --edition 2021 --config max_width=150`.
  Never rustfmt test files that declare `mod common`.
- `.cache/doris-src` and `.cache/rocksdb-5.14.2` are read-only. FE settings stay unchanged.
- Each task ends with `cargo test -q --workspace --no-fail-fast` green and clippy clean
  (`cargo clippy --workspace --all-targets -- -D warnings`).

## Review Focus

1. An INSERT … SELECT whose SELECT yields no rows. Expected: one done report per fragment, no
   counters, no instance reports, no commit infos, status OK, so the FE commits an empty txn
   (Task 4, `an_insert_select_of_no_rows_reports_no_counters`).
2. An INSERT … SELECT into the table it reads (S2). Expected: the scan reads the version in its
   scan ranges and never the rows this load writes (Task 4,
   `an_insert_select_into_its_own_table_reads_the_old_version`).
3. A multi-instance sink where one instance fails (a tablet error, or another instance's error).
   Expected: nothing is committed, the reports carry the error, and no commit infos are sent
   (Task 4, `a_failed_instance_commits_nothing`).
4. A v1 DATE or DATETIME as a key or distribution column. Expected: C++'s key bytes and bucket
   (Task 1, `v1_date_keys_encode_as_cpp` and `v1_dates_hash_their_text`).
5. A Parquet or ORC file with anything the reader can't turn into C++'s text (a nested column, an
   unsupported physical type, an unmatched column). Expected: the load fails with a status naming
   it, never wrong rows (Task 3, `a_parquet_file_with_a_nested_column_is_refused`).

---

### Task 1: v1 DATE / DATETIME writes

**Files:**
- Modify: `crates/doris-segment/src/writer.rs` (`field_type_of`, `value_bytes`),
  `crates/doris-segment/src/key_coder.rs` (`encode_value`)
- Modify: `crates/doris-exec/src/value.rs` (`ColType::writable`, `type_name`, `to_datum`,
  `parse_text`), `crates/doris-exec/src/fragment.rs` (`plan_write` uses `writable`),
  `crates/doris-exec/src/expr.rs` (`check` uses `writable`), `crates/doris-exec/src/file_scan.rs`
  (its type gates use `writable`), `crates/doris-exec/src/route.rs` (`hash_bytes`)
- Test: `crates/doris-segment/tests/writer.rs`, `crates/doris-exec/tests/value.rs`,
  `crates/doris-exec/tests/fragment.rs`, `crates/doris-exec/tests/sink.rs`

**Interfaces:**
- Produces:
  - `ColType::writable(&self) -> bool`, which is `supported()` plus DATE and DATETIME. Write paths
    use it; `supported()` is unchanged and still gates reads.
  - The v1 type names are `"Date"` and `"DateTime"`.

- [ ] **Step 1: Write the failing tests.**
  - `doris-segment` `round_trips_every_type` gains a DATE and a DATETIME column, written from
    v2-packed `Datum::Int` and read back as the same values.
  - `v1_date_keys_encode_as_cpp`: the key bytes are the big-endian 3 bytes of `y<<9|m<<5|d` for
    DATE `2023-06-01`, and the sign-flipped big-endian i64 `20230601120000` for DATETIME
    `2023-06-01 12:00:00`.
  - `v1_dates_hash_their_text`: a v1 DATE distribution column buckets by the crc of `"2023-06-01"`,
    with DATETIME likewise.
  - `a_v1_datetime_insert_is_written`: a `plan_write` + `run_sink` of a VALUES fragment with a
    DATETIME `dt` NOT NULL column (the shape of test_coalesce_new's line 43) writes and commits.
  - `v1_reads_stay_refused`: `ColType::supported()` is false for DATE/DATETIME.
  - Also: `corpus_rows_rewrite_equal` must cover the v1 columns of the corpus table `scalar_more`
    (`c_datev1`, `c_datetimev1`) once `field_type_of` knows their names. Check that it does.
- [ ] **Step 2: Run them.** `cargo test -q -p doris-segment -p doris-exec --no-fail-fast`.
  Expected: they FAIL on "DATE"/"DATETIME" unsupported.
- [ ] **Step 3: Implement** as in Research.
  - Convert from v2-packed to v1 only in `value_bytes` and `encode_value`.
  - `to_datum` takes DATE through `parse_datev2` and DATETIME through `parse_datetimev2(s, 0)`.
  - `parse_text` gets arms for both.
- [ ] **Step 4: Run** the workspace tests and clippy. Expected: all PASS.
- [ ] **Step 5: Commit** ("M6 s4: v1 DATE/DATETIME writes").

### Task 2: Stream-load casts and `concat`

**Files:**
- Modify: `crates/doris-exec/src/value.rs` (`parse_text`: integer fractional suffix; float overflow),
  `crates/doris-exec/src/expr.rs` (`concat`; decimal → VARCHAR cast text),
  `crates/doris-exec/src/file_scan.rs` (the strict rule)
- Test: `crates/doris-exec/tests/value.rs`, `crates/doris-exec/tests/fragment.rs` (or a stream-load
  test beside the existing ones in `crates/doris-be/tests/stream_load.rs`)

**Interfaces:**
- Produces:
  - `concat(...)` in `expr::check` and `eval`. It takes any number of arguments, gives NULL if any
    argument is NULL, and casts each argument to a string first.
  - A decimal → CHAR/VARCHAR/STRING cast gives the value at full scale (`1234.0000`, `-0.5000`).

- [ ] **Step 1: Write the failing tests.**
  - `a_string_with_a_fraction_casts_to_its_integer_part`: `"3.12"` → 3 for TINYINT through
    LARGEINT. `"3."` → 3. `"3.1x"`, `"x3"`, `"2e+128"` and `"2147483648"` (INT) → None.
    Cite `is_float_suffix` for the exact accepted suffix, and follow it.
  - `a_float_that_overflows_is_infinity`: `"1e400"` → +Infinity as DOUBLE and as FLOAT;
    `"-1e400"` → −Infinity.
  - `dates_normalise_and_bad_dates_fail`: `2024-4-2` → `2024-04-02`. `2024-14-2`, `2024-4-222` and
    `2024-4-2 27:00:00` → None.
  - `concat_joins_its_arguments_and_null_wins`: `concat('invalid', CAST(dec AS VARCHAR))` →
    `"invalid1234567890"`; a NULL argument → NULL.
  - `only_a_direct_column_with_a_value_filters_in_strict_mode`: test_stream_load_strict_mode load 21
    (columns `k01, k00=cast(concat('invalid', k01) as bigint)`, strict, ratio 0.3, its 10 rows).
    Expected: 7 loaded, 3 filtered, and k00 NULL on every row.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement.** `parse_text` is shared with the insert path. C++'s non-strict cast uses
  the same `string_to_int` rule, so apply it once.
- [ ] **Step 4: Run** the workspace tests and clippy. Expected: all PASS, including the existing
  stream-load and insert tests.
- [ ] **Step 5: Commit** ("M6 s4: stream-load casts and concat").

### Task 3: Parquet and ORC stream loads

**Files:**
- Modify: `Cargo.toml` (workspace deps), `crates/doris-exec/Cargo.toml`: `parquet = { version =
  "53", default-features = false, features = ["snap", "flate2", "zstd", "lz4"] }` and `orc-rust =
  { version = "0.6", default-features = false }`. Use the arrow version orc-rust brings.
- Create: `crates/doris-exec/src/columnar.rs`
- Modify: `crates/doris-exec/src/file_scan.rs` (`check_stream`, `stream_rows`),
  `crates/doris-be/src/load.rs` (`parse_format`, `put_request`)
- Test: `crates/doris-exec/tests/columnar.rs`, `crates/doris-be/tests/stream_load.rs`
- Fixtures: copy `test_decimal.parquet` and `test_decimal.orc` from
  `.cache/doris-src/regression-test/data/load_p0/stream_load/` into
  `crates/doris-exec/tests/fixtures/`.

**Interfaces:**
- Produces:
  - `columnar::parquet_rows(body: &[u8], names: &[String]) -> Result<Vec<Vec<Option<String>>>,
    ExecError>` and `columnar::orc_rows(...)`, with the same signature. Each returns, per row and
    per wanted name (matched lower-cased), the text C++ casts the file value to: integers in
    decimal, decimals at the file column's scale, strings as they are, `None` for NULL.
  - A wanted name the file lacks gives `None` (C++ fills missing columns with NULL).
  - A column the reader can't turn into text, such as a nested type or an unsupported physical or
    logical type, gives `ExecError::Unsupported` naming it.

- [ ] **Step 1: Write the failing tests.**
  - `the_decimal_parquet_file_reads_as_cpp_text` and `the_decimal_orc_file_reads_as_cpp_text`: the
    5 rows of each fixture, as the text of the recorded rows (`replies` for test_load_with_decimal:
    sql1 and sql2 give the same rows). The ORC values stored at scale 0 come out at the column's
    scale.
  - `a_parquet_file_with_a_nested_column_is_refused`: build a small file with a LIST column in the
    test using the `parquet` writer. Expected: Unsupported naming the column.
  - `parquet_and_orc_stream_loads_answer_as_cpp`: in `doris-be/tests/stream_load.rs`, a load with
    `format: parquet` and one with `format: orc`. Expected:
    - the streamLoadPut the FakeFe gets has `file_type` FILE_LOCAL, `format_type` FORMAT_PARQUET (6)
      or FORMAT_ORC (8), compress PLAIN, `file_size` equal to the body length, and a `path`;
    - the reply is Success 5/5/0.
- [ ] **Step 2: Run them.** Expected: FAIL (no module; format refused).
- [ ] **Step 3: Implement.**
  - `columnar.rs` over the `parquet` row API without arrow, and over orc-rust's arrow batches.
  - Decimal text: the unscaled i128 printed at the scale.
  - `stream_rows` takes the Parquet/ORC rows into the same source-row path as CSV/JSON.
  - `put_request` mirrors C++ for these formats. The `path` follows the recorded PUT's form
    (`<root>/mini_download/…`); the FE only echoes it, and doris-rs reads the body from memory.
- [ ] **Step 4: Run** the workspace tests and clippy. Expected: all PASS.
- [ ] **Step 5: Commit** ("M6 s4: Parquet and ORC stream loads").

### Task 4: INSERT … SELECT

**Files:**
- Modify:
  - `crates/doris-exec/src/sink.rs`: `TableSink`; `run_sink` over it; `exec_status`.
  - `crates/doris-exec/src/fragment.rs`: `table_sink`, `WritePlan::Select`, `is_load_plan`.
  - `crates/doris-exec/src/value.rs`: `datum_value`.
  - `crates/doris-exec/src/query.rs`: `FragmentPlan.write`, `check_plan`.
  - `crates/doris-exec/src/pipeline.rs`: `ExecCtx.table`, the `OLAP_TABLE_SINK` arm.
  - `crates/doris-be/src/brpc.rs`: routing.
  - `crates/doris-be/src/query.rs`: the sinks, finish, reports.
  - `crates/doris-exec/src/error_log.rs` callers: name.
- Test: `crates/doris-exec/tests/sink.rs`, `crates/doris-be/tests/query.rs`,
  `crates/doris-be/tests/insert.rs`

**Interfaces:**
- Consumes: `ColType::writable` (Task 1).
- Produces:
  - `fragment::table_sink(params: &TPipelineFragmentParams, plan: WritePlan) -> Result<WriteFragment,
    ExecError>`. It holds `plan_write`'s sink checks (sink type, partial update, group commit,
    automatic partitions, column types). `plan_write` calls it for Values/Stream; the query planner
    calls it with `WritePlan::Select`.
  - `fragment::is_load_plan(list: &TPipelineFragmentParamsList) -> bool`: one fragment, one node,
    UNION_NODE or FILE_SCAN_NODE, into an OLAP_TABLE_SINK. `brpc::is_write` becomes this, so
    every other table-sink plan goes to the query service.
  - `value::datum_value(d: &Datum, c: &ColType) -> Value`: a block value in the form `Router::route`
    takes. Decimals become `Value::Decimal` text at the column's scale, dates `Value::Date` text,
    strings `Str`, and so on.
  - `sink::TableSink`:
    - `new(f: &WriteFragment, instance_ids: Vec<TUniqueId>) -> Result<TableSink, ExecError>`;
    - `push(&self, instance: usize, rows: Vec<Result<Vec<Value>, RowError>>, nullable: &[bool]) ->
      Result<(), ExecError>`, which returns `ExecError::Cancelled(<the DATA_QUALITY_ERROR text>)`
      when the load is cancelled for too many errors;
    - `finish(self, tablets: &Mutex<TabletManager>, error_dir: &Path) -> SinkOutcome`.
    - It is `Sync`. Instances push concurrently, and `finish` runs once, after every instance has
      ended.
  - `SinkOutcome` gains `instances: Vec<(TUniqueId, i64 /*rows*/, i64 /*bytes*/)>`.
  - `FragmentPlan.write: Option<WriteFragment>`, set by `plan_query` for an OLAP_TABLE_SINK fragment.
  - `ExecCtx.table: &TableFn`, where `type TableFn<'a> = dyn Fn(usize /*fragment*/, usize
    /*instance*/, Block) -> Result<(), ExecError> + Sync + 'a`.

- [ ] **Step 1: Write the failing tests.**
  - `sink.rs`:
    - `a_table_sink_fed_by_two_instances_commits_once`: 2 instances push 2 rows and 1 row.
      Expected: one commit over every tablet of the touched partition, `dpp.norm.ALL`=3, and two
      instance reports with each instance's rows and bytes.
    - `an_insert_select_of_no_rows_reports_no_counters`: no rows. Expected: status OK, no
      `load_counters` entries, no instance reports, and `commit_infos` None.
    - `the_fifty_second_error_row_cancels_a_zero_tolerance_load`: 96 rows, 64 of them with no
      partition, `load_zero_tolerance` true. Expected:
      - CANCELLED with the exact DATA_QUALITY_ERROR text from Research;
      - `dpp.norm.ALL`=96, `dpp.abnorm.ALL`=0;
      - `tracking_url` and `first_error_msg` set, no commit infos.
    - `the_error_log_is_named_as_cpp`: db `insert_stmt`, label = the instance id as `print_id`.
  - `doris-be/tests/query.rs`:
    - `an_insert_select_runs_in_the_query_pipeline`: the S2 shape, 4 scan instances into 1 sink
      instance. Expected:
      - the FE gets 2 reports, both LOAD;
      - the sink fragment's report carries `commit_infos` and the counters;
      - the scan fragment's report carries only status OK and done;
      - a following SELECT, after publish, reads the rows.
    - `a_four_instance_table_sink_fragment_writes_once` (the S1/S3 shape over `exec_plan_fragment`).
    - `an_insert_select_into_its_own_table_reads_the_old_version`.
    - `a_failed_instance_commits_nothing`.
    - `the_statistics_delete_is_refused_naming_partial_update`: the S0 shape is refused at prepare
      with "partial update".
  - `doris-be/tests/insert.rs`: `a_values_insert_still_goes_to_the_load_service`.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement.**
  - `run_sink` becomes `TableSink::new` + one `push` + `finish`, so VALUES and stream loads keep
    their behaviour, plus the C++ report shape:
    - counters and instance reports only when there were rows;
    - `commit_infos` None when empty;
    - the 52nd-error cancel;
    - the error log's name.
  - `check_plan` accepts OLAP_TABLE_SINK through `table_sink`'s checks. OLAP_TABLE_SINK_HASH_PARTITIONED
    stays refused.
  - The query service keeps one `TableSink` per sink fragment. When every instance has ended, it
    calls `finish`, then sends one report per fragment: LOAD, status, and the sink fragment's
    fields.
  - On failure, every fragment's report is LOAD when the plan writes, and the sink commits nothing.
  - SELECT queries keep reporting nothing on success.
- [ ] **Step 4: Run** the workspace tests and clippy. Expected: all PASS.
- [ ] **Step 5: Commit** ("M6 s4: INSERT … SELECT").

### Task 5: Replays and features

**Files:**
- Modify: `crates/doris-wire/src/bin/extract-replays.rs` (allow INSERT … SELECT writes),
  `oracle/scores/epic1-supported.json` (`write:INSERT_SELECT`), `oracle/needs_supplement.json`
  (remove the `state:fe enable_date_conversion=true` group)
- Add: replays under `crates/doris-fixtures/replays` for the user INSERT … SELECT suites that
  extract: `test_insert_docs_demo`, `test_role_mapping_keyword`,
  `test_insert_partition_fail_url` and `test_clean_label_nereids`. Use
  `cargo run -q --release -p doris-wire --bin extract-replays -- crates/doris-fixtures/replays --only …`.
- Test: `crates/doris-fixtures/tests/replays.rs` (`INSERT_SELECTS`), `crates/doris-be/tests/replay.rs`
  (`insert_selects_match_the_reference`)

- [ ] **Step 1:** Allow `write:INSERT_SELECT` suites in extract-replays, then extract the four.
  Ledger each that won't extract, with its reason.
- [ ] **Step 2: Write the failing test** `insert_selects_match_the_reference`. Expected: every
  replayed query's rows equal the reference's, and every write fragment's report commits the same
  tablets as the reference's. Run it. Expected: PASS on Task 4's code. A replay that fails is a
  finding to fix, not to drop.
- [ ] **Step 3:** Update supported.json and the supplement, then recount with `derive`. Expected:
  eligible 414.
- [ ] **Step 4: Run** the workspace tests, clippy and `make -C oracle test`. Expected: all PASS.
- [ ] **Step 5: Commit** ("M6 s4: INSERT … SELECT replays and features").

### Task 6: Gate, docs, As built

**Files:**
- Modify: `oracle/scores/epic1-out.txt`, `oracle/scores/epic1-strict.txt` (from the run)
- Modify: `oracle/README.md` (an M6 story 4 paragraph; the supplement paragraph loses the
  `enable_date_conversion` group), this plan (As built)

- [ ] **Step 1:** Make sure no BE holds the ports. Reset the FE (`make fe-down`, remove
  `/tmp/doris-rs-oracle/storage`, `make fe-up`), start `make rust-be` with nohup, and run
  `RUN_ID=m6-s4-full-1 make epic-check TARGET=rust`. Expected: `eligible: 414; failing out-gate: 0;
  failing strict: 0`. Also check that `test_coalesce_new` now fails at line 54, not 43. Triage any
  failure with superpowers:systematic-debugging.
- [ ] **Step 2:** Stop the BE. Regenerate the ratchets from the grade, leaving out
  `test_move_column_with_cast`. Run `make -C oracle test`, then `readback-check`, then
  `swap-check`. Run them one at a time: they share the FE container. Expected: all PASS.
- [ ] **Step 3:** Write the README paragraph and the As built section, with the run's numbers.
- [ ] **Step 4: Commit** ("M6 s4: gate, docs, As built").
