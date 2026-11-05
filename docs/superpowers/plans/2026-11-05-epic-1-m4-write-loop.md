# Epic 1 M4: The Write Loop — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** doris-rs stores the rows of `INSERT … VALUES` and CSV/JSON stream loads into duplicate-key tables, as transactions the FE commits and publishes, in segment files and rowset metadata the C++ BE reads back.

**Architecture:**
- **Storage.** `doris-segment` gains a writer and fuller reader. `doris-storage` gains rowsets in tablets, a TxnManager over the M3 `meta/` store, and the end of M3's refusal of roots with data.
- **Execution.** A new `doris-exec` crate decodes write fragments, evaluates constant and cast expressions, and runs the table sink.
- **BE.** `doris-be` wires the sink to brpc `exec_plan_fragment`, `reportExecStatus`, the stream-load HTTP route, and the PUBLISH_VERSION and CLEAR_TRANSACTION_TASK agent tasks.
- **Proof.** The oracle gains a `cpp-loaded` fixture and a readback check, in which the C++ BE queries what doris-rs wrote.

**Tech Stack:** Rust (workspace crates, `thrift` compact protocol via `doris-thrift`, `prost` via `doris-proto`, `hyper`/`tokio` as in M2), `crc32fast` (zlib crc32 for bucket routing), `crc32c` (as in `doris-segment`), Python 3 and bash for the oracle, Docker (apache/doris:all-in-one-4.1.5).

**Spec:** `docs/superpowers/specs/2026-11-05-epic-1-m4-write-loop-design.md`. Research notes, cited below as **[wire]**, **[txn]** and **[seg]**, live in `docs/superpowers/specs/research/2026-11-05-m4-{insert-on-the-wire,txn-publish-stream-load,segment-writer}.md`.

## Global Constraints

- **Table types.** Duplicate-key tables only. Unique, aggregate and merge-on-write stay unsupported, as in M2.
- **Plans.** `exec_plan_fragment` runs only two shapes: UNION_NODE → OLAP_TABLE_SINK, and FILE_SCAN_NODE (FILE_STREAM) → OLAP_TABLE_SINK. Any other plan is refused with a status naming the first unsupported node, sink or expression, and nothing runs.
- **Not in M4.** Group commit is refused, as are parquet, orc and arrow loads, and `INSERT … SELECT`.
- **Segments.** V2 segments with PLAIN pages, RLE null maps and `NO_COMPRESSION`; zone maps with `pass_all=true` per page and per segment; an ordinal index for every column; the short key page; and the footer ending `len | crc32c | "D0R1"`.
- **Durability.** No success reaches the FE before the rowset files and their `rst_` key are written. They are synced as the C++ BE syncs them: segment files on close, and `meta/` per `sync_tablet_meta`.
- **Row order.** Rows within a segment are sorted by key columns: NULLs first, strings bytewise, numbers by value, equal keys in descending input position (`MemTable::_sort`).
- **Bucket routing.** A zlib crc32 chain over the distribution columns' values, mod the bucket count ([wire] §1.5).
- **No C or C++ dependencies** in `doris-segment`, `doris-storage` or `doris-exec`, as in M3.
- **Process.** The repo is public: no credentials, and ask before posting to GitHub. `.cache/doris-src` is read-only. zsh doesn't word-split, and macOS has no `timeout`.
- **Commits** end with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XsjaVQNaumxtUfsAZPdeNs
  ```

## Review Focus

1. **A load into a multi-partition, multi-bucket table where some tablets get no rows.** Every tablet of every touched partition must get a commit info and an empty rowset, or the FE's commit fails. Pinned by Task 7's `untouched_tablets_get_empty_rowsets`.
2. **A crash or kill between commit and publish.** On restart, COMMITTED `rst_` keys are back in the txn map, and the next PUBLISH_VERSION of that txn succeeds. Pinned by Task 5's `committed_txns_survive_restart`.
3. **The same PUBLISH_VERSION delivered twice, or after the rowset is already visible.** Both report success and nothing is written twice. Pinned by Task 5's `publish_is_idempotent`.
4. **NULL into a NOT NULL column, an over-long string, or a row with no partition.**
   - The row is filtered with the C++ message, the counters match, and the FE gets OK with `first_error_msg`.
   - Strict mode and `max_filter_ratio` are applied by the FE for INSERT and by the BE for stream load.
   - Pinned by Task 7's `filtered_rows_are_counted_and_logged` and Task 8's `max_filter_ratio_fails_the_load`.
5. **More than 1024 rows, with equal keys, NULL keys and CHAR keys.** The short key page has several entries, ties keep descending input order, and CHAR is padded. The C++ BE's key-range query returns the right rows. Pinned by Task 3's `short_keys_match_the_reference` and the readback check's range queries (Task 9).

---

### Task 1: The `cpp-loaded` fixture and corpus vectors

**Files:**
- Create: `oracle/swap/load-dataset.sql`, `oracle/swap/load-dataset/` (CSV and JSON files), `oracle/swap/readback-queries.sql`
- Modify: `oracle/swap/capture-meta.sh` (a third capture, `cpp-loaded`), `oracle/Makefile` (`meta-fixtures` writes it)
- Create: `crates/doris-storage/tests/fixtures/cpp-loaded/` (`meta/`, `stream_load/`, `headers/`, `rowsets.txt`, `layout.txt`, the `.dat` files, `replies/`, `README.md`)
- Create: `crates/doris-exec/tests/fixtures/values/` and `oracle/bin/extract_values.py`. The extractor writes one `.bin` per sampled INSERT `exec_plan_fragment` request, plus `expect.json` with that statement's `reportExecStatus` fields and its publish `finishTask` delta rows. Sample 60 requests across 20 suites, covering every literal kind in [wire] §4.
- Test: `oracle/tests/test_swap_be_keeps_every_be_setting.py` (unchanged; must still pass)

**Interfaces:**
- Produces:
  - `load-dataset.sql`. Statements in order, run through the FE: CREATE of four duplicate-key tables, then INSERTs and stream loads.
    - `m4.all_types`: every M4 type, nullable, 1 bucket.
    - `m4.buckets`: INT key, 8 buckets.
    - `m4.ranged`: DATE key, RANGE with 3 partitions, 2 buckets.
    - `m4.big`: 3000 rows by stream load, CHAR(8) and INT keys, with duplicates and NULLs.
    - Stream loads are lines `-- stream_load <table> <file> <header>=<value>…`, which the runner turns into `curl` calls.
  - `readback-queries.sql`: one query per line, each with an ORDER BY over all columns unless its name ends `_unordered`.
  - Fixture layout as in `cpp-wal`, plus:
    - `rowsets.txt`: `<tablet_id> <rowset_id> <start>-<end> <num_rows> <num_segments>` per visible rowset;
    - `replies/<n>.tsv`: one reply per readback query.

- [ ] **Step 1:** Write `load-dataset.sql`, its data files and `readback-queries.sql`. Each table in the readback set gets a point query, a key-range query, `IS NULL`, `COUNT(*)`, and `MIN`/`MAX`. Also add one join of `m4.buckets` and `m4.ranged`.
- [ ] **Step 2:** Extend `capture-meta.sh` with a `cpp-loaded` round:
  1. Run the dataset on the C++ BE.
  2. Wait for the publishes: `SHOW TABLETS` versions stop moving.
  3. Run the queries into `replies/`.
  4. Stop the BE gracefully.
  5. Copy `meta/`, `stream_load/` and the tablet directories.
  6. Write `rowsets.txt` from the headers and the `rst_` keys.
- [ ] **Step 3:** Run `make -C oracle meta-fixtures`. Expected: `crates/doris-storage/tests/fixtures/cpp-loaded/` exists; `rowsets.txt` lists at least one rowset with `num_rows > 0` for each table; `replies/` holds one file per query.
- [ ] **Step 4:** Write `oracle/bin/extract_values.py`; it decodes the corpus with `crates/doris-wire`'s dump tool. Run it, then check that `crates/doris-exec/tests/fixtures/values/expect.json` has 60 entries, each with non-empty `commit_infos` and `delta_rows`.
- [ ] **Step 5:** Run `make -C oracle test`. Expected: OK.
- [ ] **Step 6: Commit** (`oracle: cpp-loaded fixture, load dataset and readback queries; INSERT vectors from the corpus`).

### Task 2: `doris-segment` reads PLAIN columns, short keys and zone maps

**Files:**
- Modify: `crates/doris-segment/src/column.rs` (decode `PLAIN_ENCODING` for fixed-width and binary pages)
- Create: `crates/doris-segment/src/short_key.rs`, `crates/doris-segment/src/zone_map.rs`
- Modify: `crates/doris-segment/src/lib.rs`
- Test: `crates/doris-segment/tests/index_corpus.rs`

**Interfaces:**
- Produces:
  - `pub fn read_short_keys(file: &SegmentFile) -> Result<ShortKeys, Error>`, where `ShortKeys { num_items: u32, key_bytes: Vec<Vec<u8>>, num_rows_per_block: u32, num_segment_rows: u32 }`.
  - `pub fn read_zone_maps(file: &SegmentFile, unique_id: u32) -> Result<ZoneMaps, Error>`, where `ZoneMaps { segment: ZoneMapPb, pages: Vec<ZoneMapPb> }`.
  - `read_column` decodes PLAIN pages.

- [ ] **Step 1: Write the failing tests** in `tests/index_corpus.rs`:
  - `corpus_short_keys_decode`: for every corpus table with rows, `num_items == ceil(num_rows / 1024)` and the first key's first byte is `0x01` or `0x02`.
  - `corpus_zone_maps_decode`: every column of every corpus segment has `pages.len()` equal to its data page count, and `segment.has_not_null` is true wherever the column has non-null values in `expected/`.
- [ ] **Step 2:** Run `cargo test -p doris-segment --test index_corpus`. Expected: FAIL, unresolved `read_short_keys`.
- [ ] **Step 3:** Implement both parsers, following [seg] §2.5–2.6:
  - the short key page body is the keys, then varint offsets, then `ShortKeyFooterPB` in the page footer;
  - the zone map's page-level index is an IndexedColumn of BinaryPlain pages holding serialized `ZoneMapPB`.
- [ ] **Step 4:** Run the test. Expected: PASS.
- [ ] **Step 5:** The PLAIN decode is tested in Task 3's round trip, where the writer first produces PLAIN. Commit (`doris-segment: short key and zone map readers, PLAIN pages`).

### Task 3: The segment writer

**Files:**
- Create: `crates/doris-segment/src/writer.rs`, `crates/doris-segment/src/key_coder.rs`, `crates/doris-segment/src/sort.rs`
- Modify: `crates/doris-segment/src/lib.rs`, `Cargo.toml` of doris-segment (prost encode is already present)
- Test: `crates/doris-segment/tests/writer.rs`

**Interfaces:**
- Consumes: Task 2's readers.
- Produces:
  - `pub enum Datum { Null, Bool(bool), Int(i128), Float(f32), Double(f64), Bytes(Vec<u8>) }`. Ints of every width, decimals (unscaled), DATEV2 (packed u32) and DATETIMEV2 (packed u64) are all `Int`. CHAR, VARCHAR and STRING are `Bytes`, before padding.
  - `pub struct ColumnSpec { unique_id: u32, field_type: i32 /* FieldType */, length: u32, precision: u32, frac: u32, nullable: bool, is_key: bool, index_length: u32 }`, built from `TabletColumnPb` by `ColumnSpec::from_pb(&TabletColumnPb)`.
  - `pub struct SegmentSchema { columns: Vec<ColumnSpec>, num_short_key_columns: u32, num_rows_per_block: u32 /* 1024 */ }`.
  - `pub fn sort_rows(schema: &SegmentSchema, rows: &mut Vec<Vec<Datum>>)`, implementing the memtable order from the Global Constraints.
  - `pub fn write_segment(schema: &SegmentSchema, rows: &[Vec<Datum>]) -> Result<WrittenSegment, Error>`, where `WrittenSegment { bytes: Vec<u8>, num_rows: u32, min_key: Vec<u8>, max_key: Vec<u8> }`. The keys are full KeyCoder encodings of the first and last row, as in `segments_key_bounds`.
  - `pub fn encode_key(schema: &SegmentSchema, row: &[Datum], short: bool) -> Vec<u8>`, the KeyCoder of [seg] "What doris-rs must build" item 2.

- [ ] **Step 1: Write the failing tests** in `tests/writer.rs`:
  - `round_trips_every_type`: 2500 rows over every M4 type, nullable and not, empty strings, an all-NULL page, and strings that make more than one page. After `write_segment`, `read_column` returns the values (CHAR padded to N) and `read_short_keys` has 3 entries.
  - `corpus_rows_rewrite_equal`: every corpus table with M4 types only goes through `read_column` → `write_segment` → `read_column`, then the existing `expected/<table>.tsv` comparison.
  - `short_keys_match_the_reference`: for the same tables, the short key page's `key_bytes` and the segment's min/max keys equal the reference segment's, byte for byte (for min/max, `segments_key_bounds` in the corpus's `tablet_meta.json`).
  - `ties_keep_descending_input_order`: rows `(1,'a')`, `(1,'b')`, `(0,'c')` with key column 0 sort to `(0,'c')`, `(1,'b')`, `(1,'a')`.
- [ ] **Step 2:** Run `cargo test -p doris-segment --test writer`. Expected: FAIL, unresolved `write_segment`.
- [ ] **Step 3:** Implement `key_coder.rs` and `sort.rs` per [seg] §4 and §6.1. Then implement `writer.rs` per [seg] §6.1 steps 2–8:
  - pages of at most 64 KiB of values;
  - the RLE null map only on pages with a NULL;
  - ColumnMetaPB fields as in the corpus footer dump;
  - `compression = NO_COMPRESSION`.
- [ ] **Step 4:** Run the tests. Expected: PASS. A mismatch in `short_keys_match_the_reference` is fixed in the KeyCoder, never in the test.
- [ ] **Step 5: Commit** (`doris-segment: segment writer (PLAIN, pass_all zone maps, short keys)`).

### Task 4: Rowsets in tablets, and roots with data open

**Files:**
- Create: `crates/doris-storage/src/rowset.rs`
- Modify:
  - `crates/doris-storage/src/tablet_manager.rs`: rowsets per tablet, the partition → tablets index, reports.
  - `crates/doris-storage/src/data_dir.rs`, `crates/doris-storage/src/olap_meta.rs`: remove the refusal of data keys and of rowsets with segments; keep the legacy `hdr_` refusal.
  - `crates/doris-storage/src/error.rs`.
- Test: `crates/doris-storage/tests/rowsets.rs`. Update `crates/doris-storage/tests/olap_meta.rs` (the data-root refusal tests become load tests) and the process test `a_root_with_data_stops_the_start` in doris-be, which now expects a start.

**Interfaces:**
- Consumes:
  - Task 3's `write_segment`, `WrittenSegment` and `SegmentSchema`;
  - from M3: `OlapMeta` and `TabletManager::open`.
- Produces:
  - `pub fn rowset_meta_key(tablet_uid: &TabletUidPb, rowset_id: &str) -> String`, giving `rst_<hi hex>-<lo hex>_<rowset_id>` as in [txn] §1.1.
  - `OlapMeta`:
    - `save_rowset_meta(&mut self, uid, &RowsetMetaPb, sync)`
    - `remove_rowset_meta(&mut self, uid, rowset_id, sync)`
    - `rowset_metas(&self) -> Result<Vec<RowsetMetaPb>, StorageError>`
  - `pub struct WrittenRowset { meta: RowsetMetaPb }`, from `pub fn write_rowset(dir: &Path, tablet: &TabletMetaPb, rowset_id: &str, txn_id: i64, load_id: &PUniqueIdPb, rows: Vec<Vec<Datum>>) -> Result<WrittenRowset, StorageError>`.
    - It sorts the rows, writes `<rowset_id>_0.dat` when there are rows and nothing otherwise, and syncs the file.
    - It fills RowsetMetaPB per [txn] §4: COMMITTED, BETA, NONOVERLAPPING, `num_segment_rows`, key bounds truncated to 36 bytes, `empty` for no rows.
  - `TabletManager`:
    - `tablet(&self, tablet_id) -> Option<&TabletMetaPb>`
    - `tablets_of_partition(&self, partition_id) -> Vec<i64>`
    - `next_rowset_id(&mut self) -> String`, if it isn't public yet
    - `add_visible_rowset(&mut self, tablet_id, RowsetMetaPb) -> Result<(), StorageError>`, which saves the header and deletes the `rst_` key in one batch (design §9.4).
  - Reports per design §4.6.

- [ ] **Step 1: Write the failing tests** in `tests/rowsets.rs`:
  - `cpp_loaded_root_opens_with_its_rowsets`: open a copy of `fixtures/cpp-loaded`. `tablet_infos()` gives, per tablet, the `version`, `row_count` and `data_size` from `rowsets.txt`, and `local_segment_size` = Σ data disk size.
  - `written_rowset_matches_the_cpp_fields`: write the rows of the fixture's `m4.all_types` tablet with `write_rowset`. The RowsetMetaPB matches the fixture's in every field except rowset id, txn id, load id, creation time and the disk sizes, and its `segments_key_bounds` are equal.
  - `an_empty_rowset_has_no_files`: no rows → `num_segments 0`, `empty true`, and no `.dat` file.
  - `visible_rowsets_survive_restart`: `add_visible_rowset`, reopen; the version and row count are reported, and there is no `rst_` key for it.
- [ ] **Step 2:** Run `cargo test -p doris-storage --test rowsets`. Expected: FAIL.
- [ ] **Step 3:** Implement, and remove the refusal. Expected test changes:
  - `olap_meta.rs` refusal tests are renamed `a_root_with_data_opens` and assert the open succeeds;
  - the doris-be process test is renamed `a_root_with_data_starts`.
- [ ] **Step 4:** Run `cargo test --workspace`. Expected: PASS.
- [ ] **Step 5: Commit** (`doris-storage: rowsets, rst_ keys, roots with data open`).

### Task 5: The TxnManager

**Files:**
- Create: `crates/doris-storage/src/txn.rs`
- Modify: `crates/doris-storage/src/tablet_manager.rs`, `crates/doris-storage/src/lib.rs`
- Test: `crates/doris-storage/tests/txn.rs`

**Interfaces:**
- Consumes: Task 4.
- Produces, on `TabletManager`:
  - `commit_txn(&mut self, partition_id: i64, txn_id: i64, tablet_id: i64, load_id: PUniqueIdPb, rowset: RowsetMetaPb) -> Result<(), StorageError>`. It writes the COMMITTED `rst_` key. The same rowset id again is Ok; another rowset for the same (txn, tablet) is `StorageError::TxnConflict`.
  - `publish_txn(&mut self, partition_id: i64, txn_id: i64, version: i64) -> PublishResult`, where `PublishResult { succ: Vec<i64>, errors: Vec<i64>, delta_rows: BTreeMap<i64, i64> }`. It makes the rowsets visible at `[version, version]` with `creation_time` now and calls `add_visible_rowset`. `succ` lists every tablet of the partition whose max continuous version ≥ `version`.
  - `clear_txn(&mut self, txn_id: i64, partition_ids: &[i64]) -> Result<(), StorageError>`, where an empty slice means every partition. It deletes the pending keys and files.
  - Reload at `open`: COMMITTED `rst_` keys go into the txn map; VISIBLE ones are added to their tablets.

- [ ] **Step 1: Write the failing tests** in `tests/txn.rs`:
  - `commit_then_publish_makes_rows_visible`: one tablet, one rowset of 3 rows, publish v2. The tablet reports version 2 and row count 3; `delta_rows == {tablet: 3}`.
  - `committed_txns_survive_restart`: commit, drop the manager, reopen, publish. It succeeds with the same delta.
  - `publish_is_idempotent`: publish twice. Both have the same `succ`, and the header is saved once (`meta` key count unchanged).
  - `clear_removes_pending_rowsets`: commit, clear. The `rst_` key and `.dat` are gone, and a later publish of that txn returns the tablet in `errors`.
  - `publish_with_no_rowsets_still_succeeds`: a partition whose tablets have no txn rowsets.
- [ ] **Step 2:** Run `cargo test -p doris-storage --test txn`. Expected: FAIL.
- [ ] **Step 3:** Implement per [txn] §1.
- [ ] **Step 4:** Run `cargo test --workspace`. Expected: PASS.
- [ ] **Step 5: Commit** (`doris-storage: transactions, publish and clear`).

### Task 6: PUBLISH_VERSION and CLEAR_TRANSACTION_TASK

**Files:**
- Modify: `crates/doris-be/src/agent.rs`
- Test: `crates/doris-be/tests/publish.rs`

**Interfaces:**
- Consumes: Task 5 (`publish_txn`, `clear_txn`).
- Produces, as `finishTask` per [txn] §1.3–1.4:
  - PUBLISH_VERSION sends `succ_tablets{t: 0}`, `error_tablet_ids`, `table_id_to_tablet_id_to_delta_num_rows` (rows > 0 only) and `report_version` (not increased), with status OK.
  - CLEAR_TRANSACTION_TASK sends status OK with no `report_version`.

- [ ] **Step 1: Write the failing tests:**
  - `publish_finish_task_matches_the_corpus`: replay a decoded PUBLISH_VERSION from a Task 1 vector against a manager with that txn committed. The `finishTask` equals the captured one, apart from `report_version` and backend.
  - `clear_finish_task_has_no_report_version`.
  - `publish_without_partitions_sends_empty_maps`.
- [ ] **Step 2:** Run `cargo test -p doris-be --test publish`. Expected: FAIL (`NOT_IMPLEMENTED`).
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5: Commit** (`doris-be: PUBLISH_VERSION and CLEAR_TRANSACTION_TASK`).

### Task 7: `doris-exec`, write fragments, expressions and the table sink

**Files:**
- Create: `crates/doris-exec/` (`Cargo.toml`, `src/lib.rs`, `src/fragment.rs`, `src/expr.rs`, `src/value.rs`, `src/sink.rs`, `src/route.rs`, `src/error_log.rs`)
- Modify: workspace `Cargo.toml` (add the member and `crc32fast = "1"`)
- Test: `crates/doris-exec/tests/{fragment,sink}.rs`

**Interfaces:**
- Consumes:
  - Task 1's `values/` vectors;
  - Task 3's `Datum`;
  - Tasks 4–5: `write_rowset`, `commit_txn`, `tablets_of_partition`, and `TabletManager` behind `Arc<Mutex<_>>` as in M2.
- Produces:
  - `pub fn decode_fragments(req: &PExecPlanFragmentRequest) -> Result<TPipelineFragmentParamsList, ExecError>`. It requires `compact=true` and `version=3`.
  - `pub enum WritePlan { Values { rows: Vec<Vec<TExpr>> }, Stream { scan: TFileScanRange /* params of the FILE_SCAN_NODE */ } }` plus `pub struct WriteFragment { plan: WritePlan, projections: Vec<TExpr>, sink: TOlapTableSink, desc: TDescriptorTable, query_id: TUniqueId, fragment_id: i32, coord: TNetworkAddress, backend_id: i64, txn_id: i64, load_id: PUniqueId }`, built by `pub fn plan_write(params: &TPipelineFragmentParams) -> Result<WriteFragment, ExecError>`. `ExecError::Unsupported { what: String }` names the first node, sink or expression outside the write shapes.
  - `pub fn eval_const(e: &TExpr, dest: &TSlotDescriptor) -> Result<Datum, RowError>`, and `pub fn cast_text(text: Option<&str>, dest: &TSlotDescriptor) -> Result<Datum, RowError>` (used by stream load). `RowError` carries the C++ first-error message ([wire] §5).
  - `pub fn route(sink: &TOlapTableSink, row: &[Datum]) -> Result<(i64 /* partition */, i64 /* tablet */), RowError>`.
  - `pub fn run_sink(fragment: &WriteFragment, rows: impl Iterator<Item = Result<Vec<Datum>, RowError>>, tablets: &Mutex<TabletManager>) -> SinkOutcome`, where `SinkOutcome { status: TStatus, commit_infos: Vec<TTabletCommitInfo>, loaded: i64, filtered: i64, unselected: i64, bytes: i64, first_error: Option<String>, error_log: Option<String /* file name */> }`. It commits every tablet of every touched partition, writing an empty rowset where a tablet got no rows.
  - `pub fn exec_status(fragment: &WriteFragment, outcome: &SinkOutcome, tracking_url: Option<String>) -> TReportExecStatusParams`, with the fields per [wire] §2.1.

- [ ] **Step 1: Write the failing tests:**
  - `corpus_values_fragments_plan`: every Task 1 vector decodes and `plan_write` gives `Values`.
  - `a_select_fragment_is_refused`: a corpus fragment with OLAP_SCAN_NODE gives `Unsupported { what: "OLAP_SCAN_NODE" }`.
  - `routing_reproduces_the_corpus_delta_rows`: for each vector, `route` over its evaluated rows counts rows per tablet equal to `expect.json`'s `delta_rows`.
  - `exec_status_matches_the_corpus`: run each vector into a temp `TabletManager` with the vector's tablets created. `exec_status` equals the captured report, apart from ids, timestamps and `backend_id`; `commit_infos` sorted.
  - `untouched_tablets_get_empty_rowsets`: one row into an 8-bucket table gives 8 commit infos and 7 empty rowsets.
  - `filtered_rows_are_counted_and_logged`: NULL into NOT NULL, a 5-char string into VARCHAR(3), and a date outside every partition. The outcome has `filtered 3`, OK status, and `first_error` equal to the C++ texts in [wire] §5; the error log has 3 lines.
- [ ] **Step 2:** Run `cargo test -p doris-exec`. Expected: FAIL (crate missing).
- [ ] **Step 3:** Implement. `value.rs` converts literals to `Datum` per [seg] §4: DATEV2/DATETIMEV2 bit packing, decimal unscaling with the column scale, LARGEINT from its string form.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5: Commit** (`doris-exec: write fragments, constant expressions, table sink`).

### Task 8: INSERT and stream load on the BE

**Files:**
- Modify:
  - `crates/doris-be/src/brpc.rs`: route `exec_plan_fragment` and `_prepare`/`_start` (refused, with "INSERT … SELECT is not supported"), and `cancel_plan_fragment`.
  - `crates/doris-be/src/fe_client.rs`: new `FeClient` methods.
  - `crates/doris-be/src/http.rs`: `/api/{db}/{table}/_stream_load` and `/api/_load_error_log`.
  - `crates/doris-be/src/backend_service.rs`: `get_stream_load_record`.
  - `crates/doris-be/src/agent.rs`, `crates/doris-be/src/be.rs`.
- Create: `crates/doris-be/src/load.rs` (fragment runner, registry and cancel), `crates/doris-be/src/stream_load.rs` (HTTP flow, CSV/JSON readers, response JSON, recorder)
- Test: `crates/doris-be/tests/insert.rs`, `crates/doris-be/tests/stream_load.rs`

**Interfaces:**
- Consumes: Task 7.
- Produces:
  - `FeClient` methods, all over the M2 Thrift client:
    - `report_exec_status(&self, coord: &TNetworkAddress, req: TReportExecStatusParams) -> Result<TReportExecStatusResult, FeError>`
    - `load_txn_begin(&self, TLoadTxnBeginRequest) -> Result<TLoadTxnBeginResult, FeError>`
    - `stream_load_put(&self, TStreamLoadPutRequest) -> Result<TStreamLoadPutResult, FeError>`
    - `load_txn_commit(&self, TLoadTxnCommitRequest) -> Result<TLoadTxnCommitResult, FeError>`
    - `load_txn_rollback(&self, TLoadTxnRollbackRequest) -> Result<TLoadTxnRollbackResult, FeError>`
  - The brpc reply to `exec_plan_fragment` is sent before the fragment runs. On completion one report is sent to `fragment.coord`.
  - The stream load reply is the JSON of [txn] §3.3: exact key order, 4-space indent, trailing `\n`.

- [ ] **Step 1: Write the failing tests:**
  - `insert_values_reports_and_commits`: a fake FE (the M2 `testing.rs` pattern) gets a corpus vector over gRPC. The reply arrives first, then exactly one `reportExecStatus` equal to the vector's.
  - `cancel_finished_is_ok`.
  - `stream_load_csv_end_to_end`: the fake FE answers `loadTxnBegin`/`streamLoadPut` with the decoded corpus replies from [txn] §3; a CSV of 3 rows is sent with `Expect: 100-continue`. The calls are begin → put → commit(`sync: true`, commit infos), and the reply JSON equals the corpus body apart from TxnId, Label and times.
  - `stream_load_json_lines`: the same with `format: json`, `read_json_by_line: true`.
  - `max_filter_ratio_fails_the_load`: 1 bad row of 2 with `max_filter_ratio 0` gives `loadTxnRollback`, and the JSON has `"Status": "Fail"` and an `ErrorURL` that `GET` serves.
  - `group_commit_is_refused`: a begin reply that sets group commit gives a JSON `Fail` naming group commit, and a rollback.
  - `stream_load_records_are_served`: after a load, `get_stream_load_record(0)` returns it.
- [ ] **Step 2:** Run `cargo test -p doris-be --test insert --test stream_load`. Expected: FAIL.
- [ ] **Step 3:** Implement. CSV follows the stream load's `column_separator`, `line_delimiter`, `enclose`/`escape`, `skip_lines` and `trim_double_quotes`. JSON supports `read_json_by_line`, `strip_outer_array`, `jsonpaths` and `json_root`.
- [ ] **Step 4:** Run `cargo test --workspace` and `cargo clippy --all-targets -- -D warnings`. Expected: PASS, clean.
- [ ] **Step 5: Commit** (`doris-be: INSERT VALUES and stream load`).

### Task 9: The readback check, and swaps with data

**Files:**
- Create: `oracle/swap/readback-check.sh`, `oracle/swap/run_dataset.py` (runs `load-dataset.sql`, its stream-load lines with curl through the FE, then `readback-queries.sql`)
- Modify: `oracle/swap/swap-check.sh` (remove `no_data`; put rows in its tables with one INSERT per table), `oracle/Makefile` (`readback-check`), `oracle/README.md`
- Test: `oracle/tests/test_run_dataset.py`

**Interfaces:**
- Consumes: Task 1's dataset and queries; M3's `lib.sh`; Task 8's binary.
- Produces: `make -C oracle readback-check`, whose last line is `readback check: PASS` or `readback check: FAIL (<first reason>)`.

- [ ] **Step 1: Write the failing test** `test_run_dataset.py::test_stream_load_lines_become_curl_calls`: the parser turns `-- stream_load m4.big big.csv column_separator=,` into a curl argv with `-H column_separator:,`, `-T <dir>/big.csv` and `--location-trusted`.
- [ ] **Step 2:** Run `make -C oracle test`. Expected: FAIL.
- [ ] **Step 3:** Implement the two scripts. `readback-check.sh`:
  1. Bring up the swap cluster. Run the dataset with the C++ BE only, saving replies R1.
  2. Bring up a fresh root. Start doris-rs and run the dataset's loads; doris-rs runs no query, so replies come later.
  3. Stop doris-rs. Start the C++ BE on its root, check `reported`, and run the queries into R2.
  4. R1 == R2, compared exactly.
  5. The C++ BE's log has no DROP or CLONE, and the tablet report diff has `metaDel: 0`.
- [ ] **Step 4:** Run `make -C oracle test`, then `make -C oracle readback-check` and `make -C oracle swap-check`. Expected: OK, `readback check: PASS`, `swap check: PASS`.
- [ ] **Step 5: Commit** (`oracle: the readback check; swaps carry data`).

### Task 10: The gate, docs, As built

**Files:**
- Modify:
  - `oracle/scores/epic1-supported.json`: design §7's features.
  - `oracle/scores/epic1-out.txt`, `oracle/scores/epic1-strict.txt`: the ratchets.
  - `docs/adr/0002-*.md`: consequences — M4 is the write loop, and the readback check's first form lands.
  - `oracle/README.md`: numbers, readback.
  - This plan's "As built".

- [ ] **Step 1:** Run `make -C oracle fe-up` and `make rust-be`, then `RUN_ID=m4-full-1 make epic-check TARGET=rust`, then `python3 checker/grade.py results/rust/m4-full-1 epic1 scores`. Expected: `failing out-gate: 0; failing strict: 0`, with eligible ≥ 155 and up to the 96 BE-reaching suites of the M4 set plus the M2 ones.
- [ ] **Step 2:** Triage each failing eligible suite as M2's Task 11 did. Fix it in the owning crate with a test that fails first, then rerun as `m4-full-2`.
- [ ] **Step 3:** Write the docs. Run `cargo test --workspace`, `cargo clippy --all-targets -- -D warnings`, `make -C oracle test`, `make -C oracle swap-check` and `make -C oracle readback-check`. Expected: all pass.
- [ ] **Step 4: Commit** (`M4: gate, docs, As built`).
