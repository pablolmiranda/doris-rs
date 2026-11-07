# Epic 1 · M3: C++-compatible Data Directory — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** doris-rs keeps tablet headers in `<root>/meta`, a RocksDB 5.14.2 database it reads and writes in pure Rust, and a live swap check shows a C++ BE and doris-rs taking turns on one root with the FE noticing nothing.

**Architecture:** A new crate `doris-rocks` implements the subset of RocksDB 5.14.2's on-disk format the BE uses (log files, write batches, manifest, block-based tables, the small files), with the whole key set held in memory (the metadata is small) and a flush that rewrites all live keys into one table file. `doris-storage` replaces M2's temporary header store with an `OlapMeta` over `doris-rocks`, adds the C++ BE's startup checks, the data-root refusal and SHUTDOWN-then-sweep drops. The oracle gains a swap cluster (the FE-only cluster plus the reference C++ BE) that captures `meta/` fixtures and runs the swap check.

**Tech Stack:** Rust 1.93; `crc32c` 0.6 and `lz4_flex` 0.14 (already workspace dependencies), `snap` 1 (pure-Rust Snappy, new); oracle in bash and standard-library Python; reference image `apache/doris:all-in-one-4.1.5`.

**Spec:** `docs/superpowers/specs/2026-11-05-epic-1-m3-cpp-data-directory-design.md` (parent: `docs/superpowers/specs/2026-11-02-epic-1-reporting-design.md` §6 row 3; ADR 0003).

## As built (differences from the tasks below)

The tasks below are the plan as written. Implementation changed these:

- **One address without a container.** Host networking holds on OrbStack: the C++ BE takes the
  FE's `backend_ip` without a locality check, and its path hash uses the VM address
  `priority_networks` picks, which doris-rs gets as `host`. No containerized doris-rs. The C++ BE
  runs with `docker run` from `oracle/swap/lib.sh`, not a compose file (the image's entrypoint
  registers its own IP; `start_be.sh` does not exec `doris_be`, so it stops through
  `stop_be.sh --grace`).
- **What the C++ BE actually does, found in the source and the fixtures:**
  - A dropped tablet's SHUTDOWN header is deleted soon after the BE's next start: the trash
    sweeper sweeps before its first wait, then every `min_garbage_sweep_interval`. It never uses
    the interval it computes from disk usage. doris-rs's sweeper does the same, and the plan's
    usage-based interval is dropped.
  - A sweep first expires `snapshot/` and `trash/` entries, then saves each shut-down tablet's
    `<tablet_id>.hdr` (`FileHeader<TabletMetaPB>`) before moving it to trash. With
    `trash_file_expire_time_sec` <= 0 (the default) the tablet's directory is deleted at once
    instead (`DataDir::move_to_trash`). doris-rs does all three; the direct delete was found by
    the swap check's sixth step after the final review.
  - Headers that fail to load (wrong key, zero `tablet_uid`, a running tablet without versions)
    are skipped, not fatal (`ignore_load_tablet_failure`, default true). doris-rs lists them in
    `DataDir::skipped_headers`.
  - Booleans in `be.conf` are exactly `true` or `false`.
- **Interfaces:**
  - `read_table` returns `Table { entries, compressed_blocks }`, and `read_properties` is added.
  - `Db::read_only` lets the refusal checks leave a root untouched.
  - A doc-hidden `inject_write_failure(on)` stands in for a full disk in tests.
  - `DataDir::meta()` and `meta_mut()`.
  - `TabletManager::start_trash_sweep(SweepOptions)` runs the whole sweep; `sweep_shutdown` is the
    tablet part.
  - The compare script is `oracle/swap/swap_compare.py`.
- **The swap check is stronger than planned.** It also requires the FE's tablet report diff after
  each swap to count every tablet on the backend. It lists tables from FE metadata only
  (`information_schema` needs a BE that runs queries). It polls for DROP tasks, which the FE sends
  from report handling, up to 26 s after the DDL in one run. After the final review it reads only
  FE log lines from after each swap, and a sixth step starts the C++ BE on the SHUTDOWN headers
  doris-rs left and checks that its log shows each of those tablets swept and their directories gone.
- **The M2 gates hold.** `m3-full-1` grades the same as `m2-full-2`: 155 eligible suites, none
  failing either gate (out: 250 of 1622 epic suites pass; strict: 81).

## Global Constraints

- No RocksDB or other C or C++ dependency in `doris-rocks`, `doris-storage` or their tests (`snap`, `crc32c`, `lz4_flex` are pure Rust).
- Every file doris-rs writes under `meta/` or `stream_load/` must be one RocksDB 5.14.2 opens; doris-rs must read what 5.14.2 writes, including Snappy-compressed, compacted table files.
- No FE change. The swap check fails on any DROP or CLONE task, a replica marked bad, or a changed backend id, tablet id, replica id, schema hash, `tablet_uid`, version or path hash.
- Crash safety as the C++ BE: each change is in the log before it is acknowledged; synced only when `sync_tablet_meta` is true (default false) and on close.
- Roots with data are refused at startup, naming the root and the first such tablet, touching nothing.
- doris-rs runs natively; only if the C++ BE cannot take doris-rs's address does the swap check (and only it) run doris-rs in a container.
- `.cache/doris-src` and `.cache/rocksdb-5.14.2` are read-only references. The repo is public: no secrets.
- Each task leaves `cargo test`, `make -C oracle test` and `cargo clippy --all-targets` green.
- Commits end with the session's `Co-Authored-By` / `Claude-Session` lines.

## Review Focus

1. **A log whose last record is torn** (process killed mid-append): open recovers every whole record before it and ignores the tail, as 5.14.2's default recovery does (Task 2 and Task 4 tests).
2. **A crash in the middle of a flush** (new table file and manifest written, `CURRENT` not yet switched): reopen sees the complete old state; leftover files are removed (Task 4 test).
3. **A second process on the same root**: it fails at open with an error naming the `LOCK` file; the first keeps running (Task 4 test).
4. **Many tablets** (20,000 header writes): the log stays bounded by the flush threshold and reopening returns every key (Task 4 test).
5. **A header without its tablet directory, or a directory without its header, after a crash**: M2's cleanup still applies with headers in `meta/` (Task 5 test).

---

### Task 1: The swap cluster's C++ side, one address for both binaries, and the `meta/` fixtures

**Files:**
- Create: `oracle/compose/swap.yml`, `oracle/swap/be.conf`, `oracle/swap/lib.sh`, `oracle/swap/capture-meta.sh`, `crates/doris-rocks/tests/fixtures/README.md`, `crates/doris-rocks/tests/fixtures/{cpp-wal,cpp-compacted}/` (`meta/`, `stream_load/`, `headers/<tablet>.json`, `keys.txt`)
- Modify: `oracle/Makefile` (`swap-up`, `swap-down`, `meta-fixtures`; help lines), `oracle/tests/test_compose_settings.py`
- Reference only: `.cache/rocksdb-5.14.2/` (fetched in Step 1, git-ignored)

**Interfaces:**
- Produces: `make swap-up` (FE-only cluster with internal loads off, plus the C++ BE stopped and ready), `oracle/swap/lib.sh` functions `start_cpp`, `stop_cpp`, `start_rs`, `stop_rs`, `wait_alive`, `sql` used by Task 7; fixture directories read by Tasks 2–5; `SWAP_ROOT` (absolute path, same string for both binaries).

- [ ] **Step 1: Fetch the reference.** Download the RocksDB v5.14.2 source tarball (github.com/facebook/rocksdb, tag `v5.14.2`) into `.cache/rocksdb-5.14.2/`; record its sha256 in the fixture README. Every format constant in Tasks 2–4 is checked against it (`db/log_format.h`, `db/write_batch.cc`, `db/dbformat.h`, `db/version_edit.cc`, `table/format.{h,cc}`, `table/block_builder.cc`, `table/meta_blocks.cc`, `util/crc32c.h`).
- [ ] **Step 2: Write the failing settings test** `test_swap_be_keeps_every_be_setting` in `oracle/tests/test_compose_settings.py`: every `BE_CONFIG_EXTRA` line of `reference-single.yml` except `mem_limit` is in `oracle/swap/be.conf`. Run `make -C oracle test` → FAIL (file missing).
- [ ] **Step 3: Compose and config.** `swap.yml` adds service `be` (reference image, `DORIS_ROLE=be`, `network_mode: host`, bind mount `${SWAP_ROOT}:${SWAP_ROOT}`, `oracle/swap/be.conf` as its `be.conf`, no restart policy) to `fe-only.yml`. `be.conf`: the reference BE settings, `storage_root_path = ${SWAP_ROOT}`, the C++ default ports, `report_tablet_interval_seconds = 5`, `report_disk_state_interval_seconds = 5`, `report_task_interval_seconds = 5`. `make swap-up` starts the FE, runs `SET GLOBAL enable_audit_plugin = false` and `SET GLOBAL enable_auto_analyze = false` (so no internal table receives data; confirm both names in the FE source), and adds the backend at the address of Step 4. Run `make -C oracle test` → PASS.
- [ ] **Step 4: One address.** Start the C++ BE through `start_cpp` with host networking and `priority_networks` set to the address `host.docker.internal` resolves to in the FE container. Pass if `SHOW BACKENDS` shows it `Alive: true` and its disk's `PathHash` equals `doris_storage::hash_of_path(<that address>, SWAP_ROOT)`. If it does not pass, apply the user's fallback: `oracle/swap/doris-rs.Dockerfile` builds doris-rs for Linux, both binaries run as containers with one fixed `ipv4_address` on the `doris-oracle-rust` network, and `start_rs`/`stop_rs` run that container instead of the native binary. Ledger which one holds as a ruling.
- [ ] **Step 5: Capture script** `capture-meta.sh` (make target `meta-fixtures`): on an empty `SWAP_ROOT`, start the C++ BE, create the M2 scenario's tables (`oracle/wire/scenarios/m2.sh`: dup, mow, ranged + ADD PARTITION; no INSERT), wait two tablet reports, save every tablet's `/api/meta/header/<id>?byte_to_base64=true` to `headers/`, stop the BE gracefully, copy `meta/` and `stream_load/` to `cpp-wal/`. Then four rounds of start → `CREATE TABLE` one table, `DROP TABLE` the previous round's table `FORCE` → stop (each restart replays the log into a level-0 table file; the fourth round reaches the level-0 compaction trigger), save headers again, copy to `cpp-compacted/`. Save headers for every tablet the BE holds (its `/tablets_json` route lists them; confirm the route in `be/src/service/http_service.cpp`), the FE's internal tables included, and write `keys.txt` per fixture: `tabletmeta_<tablet_id>_<schema_hash>` for each, sorted (the cross-check for Tasks 3–5; tablets dropped in the rounds must be absent). README: how captured, the RocksDB sha256, what each directory shows.
- [ ] **Step 6: Verify the fixtures show what later tasks need.** `cpp-wal/meta` has `CURRENT`, `MANIFEST-*`, at least one `.log` with records; `cpp-compacted/meta` has at least one `.sst` whose block trailers carry compression type 1 (Snappy) — check with `xxd`. If no Snappy block appears, ledger it and widen Step 5 until one does (more rounds or more tables).
- [ ] **Step 7: Commit** (`oracle: swap cluster with the reference C++ BE; meta fixtures`).

### Task 2: `doris-rocks` — log files, write batches, version edits

**Files:**
- Create: `crates/doris-rocks/{Cargo.toml,src/lib.rs,src/error.rs,src/coding.rs,src/log.rs,src/write_batch.rs,src/version_edit.rs}`, `crates/doris-rocks/tests/log.rs`
- Modify: workspace `Cargo.toml` (member, `doris-rocks` path dependency, `snap = "1"`)

**Interfaces:**
- Produces:
  - `pub enum RocksError { Io { path: PathBuf, source: std::io::Error }, Corrupt { path: PathBuf, what: String }, Unsupported { path: PathBuf, what: String }, Locked { path: PathBuf } }` (thiserror; `Unsupported` names e.g. a compression codec)
  - `log::LogWriter::new(file: File, log_number: u64) -> LogWriter`, `add_record(&mut self, data: &[u8]) -> io::Result<()>`, `sync(&mut self) -> io::Result<()>`; `log::read_records(path: &Path) -> Result<Vec<Vec<u8>>, RocksError>` (stops quietly at a torn or zero-filled tail; errors on a bad checksum before the tail)
  - `WriteBatch { sequence: u64, ops: Vec<BatchOp> }`, `enum BatchOp { Put { cf: u32, key: Vec<u8>, value: Vec<u8> }, Delete { cf: u32, key: Vec<u8> } }`, `WriteBatch::encode(&self) -> Vec<u8>`, `WriteBatch::decode(&[u8]) -> Result<WriteBatch, String>` (single deletes decode as `Delete`; any other record type is an error naming it)
  - `VersionEdit` (fields for comparator, log_number, prev_log_number, next_file_number, last_sequence, max_column_family, min_log_number_to_keep, column family id, add/drop column family, new files `{level, number, size, smallest, largest, smallest_seqno, largest_seqno}`, deleted files), `encode`/`decode`; decode skips unknown custom fields of new-file entries the way 5.14.2 does and errors on an unknown tag

- [ ] **Step 1: Write the failing tests** in `tests/log.rs`:
  - `reads_every_record_of_the_cpp_manifest`: `read_records(cpp-wal/meta/MANIFEST-*)` decodes to `VersionEdit`s naming column families `default`, `doris`, `meta` (ids from the fixture) with comparator `leveldb.BytewiseComparator`.
  - `reads_every_batch_of_the_cpp_log`: the fixture's `.log` decodes to batches whose `Put` keys in the `meta` column family include every `tabletmeta_` key of `cpp-wal/keys.txt`.
  - `round_trips_records_across_blocks`: records of 10 B, 40 KiB and 100 KiB written with `LogWriter` read back identical (spanning the 32,768-byte block size, FIRST/MIDDLE/LAST fragments).
  - `a_torn_tail_is_ignored`: write three records, truncate the file in the middle of the third → two records, no error; append 100 zero bytes after a whole record → same.
  - `a_bad_checksum_before_the_tail_is_corruption`: flip a byte in the first of three records → `RocksError::Corrupt`.
  - `batches_and_edits_round_trip`: `decode(encode(x)) == x` for a batch with puts and deletes in two column families, and for an edit with every field set.
- [ ] **Step 2: Run** `cargo test -p doris-rocks` → FAIL (crate missing).
- [ ] **Step 3: Implement** `coding.rs` (varint32/64, fixed32/64 little-endian, length-prefixed slices), `log.rs` (7-byte header: masked crc32c of type+payload, length, type; FULL=1, FIRST=2, MIDDLE=3, LAST=4; trailer of fewer than 7 bytes is zero-filled; crc mask `((c >> 15) | (c << 17)) + 0xa282ead8`), `write_batch.rs` (12-byte header: sequence, count; record tags from `db/dbformat.h`), `version_edit.rs` (tags from `db/version_edit.cc`). Every constant cites its 5.14.2 file.
- [ ] **Step 4: Run** `cargo test -p doris-rocks` → PASS; `cargo clippy --all-targets` clean.
- [ ] **Step 5: Commit** (`doris-rocks: RocksDB 5.14.2 log files, write batches and version edits`).

### Task 3: `doris-rocks` — block-based table files

**Files:**
- Create: `crates/doris-rocks/src/table/{mod.rs,block.rs,reader.rs,builder.rs,properties.rs}`, `crates/doris-rocks/tests/table.rs`

**Interfaces:**
- Consumes: `coding`, `RocksError` (Task 2).
- Produces:
  - Internal keys: `InternalKey { user_key: Vec<u8>, sequence: u64, kind: ValueKind }`, `enum ValueKind { Value, Deletion }`, with 5.14.2's 8-byte trailer `(sequence << 8) | type`.
  - `table::read_table(path: &Path) -> Result<Vec<(InternalKey, Vec<u8>)>, RocksError>` (every entry, in file order; verifies block checksums; decompresses Snappy and LZ4; any other codec is `Unsupported` naming it).
  - `table::TableBuilder::new(file: File, props: TableProps) -> TableBuilder`, `add(&mut self, key: &InternalKey, value: &[u8])` (keys in internal-key order), `finish(self) -> Result<FileMeta, RocksError>`; `TableProps { column_family_id: u32, column_family_name: String, fixed_prefix_len: Option<usize> }`; `FileMeta { size: u64, smallest: InternalKey, largest: InternalKey, smallest_seqno: u64, largest_seqno: u64 }`.

- [ ] **Step 1: Write the failing tests** in `tests/table.rs`:
  - `reads_the_cpp_tables`: every `.sst` of `cpp-compacted/meta` reads, entries come in internal-key order, at least one block was Snappy, and every user key starts with `tabletmeta_` or another prefix of the spec's §4.1.
  - `reads_the_cpp_properties`: the properties block of a fixture table names comparator `leveldb.BytewiseComparator` and, for the `meta` column family, prefix extractor `rocksdb.FixedPrefix.4`.
  - `round_trips_a_table`: 5,000 entries (keys `tabletmeta_<n>_<hash>`, 2 KiB values, sequences descending per key, one deletion) written by `TableBuilder` read back identical by `read_table`; the returned `FileMeta` has the first and last keys.
  - `written_properties_match_the_cpp_ones`: a written table's properties block holds every property name a fixture table holds (values may differ for sizes, counts and times).
  - `a_flipped_byte_is_corruption`: flip one byte in a data block → `RocksError::Corrupt` naming the file.
- [ ] **Step 2: Run** `cargo test -p doris-rocks --test table` → FAIL.
- [ ] **Step 3: Implement.** Reading: footer (last 53 bytes for format versions ≥ 1: checksum type, metaindex handle, index handle, padding, `format_version`, magic `0x88e241b785f4cff7`; accept format versions 0–2 as 5.14.2 does), metaindex, index, data blocks with restart arrays, 5-byte block trailers (compression type, masked crc32c), properties block. Writing: `format_version = 2`, crc32c checksums, uncompressed blocks (type 0), 4 KiB data blocks, restart interval 16 (index block 1), a properties block with the names a fixture table has, an empty filter section (no filter policy).
- [ ] **Step 4: Run** → PASS; clippy clean.
- [ ] **Step 5: Commit** (`doris-rocks: block-based table files`).

### Task 4: `doris-rocks` — the database

**Files:**
- Create: `crates/doris-rocks/src/db.rs`, `crates/doris-rocks/tests/db.rs`

**Interfaces:**
- Consumes: Tasks 2–3.
- Produces:
  - `pub struct DbOptions { pub column_families: Vec<CfOptions>, pub flush_log_bytes: u64 }` (default `flush_log_bytes` 4 MiB), `pub struct CfOptions { pub name: String, pub fixed_prefix_len: Option<usize> }`
  - `Db::open(dir: &Path, opts: &DbOptions) -> Result<Db, RocksError>`: creates a missing database with every listed column family; on an existing one, takes the `LOCK` file, reads `CURRENT`, the manifest, every live table and every log at or after the manifest's log number, refuses a column family on disk that `opts` does not list (as 5.14.2 does), then flushes (Step 3) so it starts from one table file per column family and a new log.
  - `Db::get(&self, cf: &str, key: &[u8]) -> Option<&[u8]>`, `Db::scan_prefix(&self, cf: &str, prefix: &[u8]) -> Vec<(&[u8], &[u8])>` (key order), `Db::write(&mut self, batch: Batch, sync: bool) -> Result<(), RocksError>` (`Batch::put(cf, key, value)`, `Batch::delete(cf, key)` by name), `Db::flush(&mut self) -> Result<(), RocksError>`, `Db::close(self) -> Result<(), RocksError>` (syncs the log).

- [ ] **Step 1: Write the failing tests** in `tests/db.rs` (on copies of the fixtures in a temp dir):
  - `opens_the_cpp_databases`: `cpp-wal/meta` and `cpp-compacted/meta` open with `default`, `doris`, `meta` (prefix 4); `scan_prefix("meta", b"tabletmeta_")` returns exactly the keys of each `keys.txt` (values are compared with the headers in Task 5).
  - `opens_the_cpp_stream_load_database`: `cpp-wal/stream_load` opens with `default` and `meta`.
  - `writes_survive_reopen`: put 100 keys, delete 10, close, reopen → 90 keys, values equal.
  - `writes_survive_a_kill`: put 100 keys, copy the directory while the `Db` is still open (what a killed process leaves), reopen the copy → 100 keys.
  - `a_torn_log_tail_loses_only_the_last_write` (Review Focus 1): put 3 keys, copy the dir, truncate the copy's log in the middle of the last record, reopen → 2 keys.
  - `a_crash_mid_flush_keeps_the_old_state` (Review Focus 2): after a flush writes its table file and manifest but before `CURRENT` is switched (fault injection: `DbOptions` field `#[doc(hidden)] pub fail_before_current: bool`, default false), reopen → the old key set, and the unreferenced files are gone.
  - `a_second_open_is_locked` (Review Focus 3): open twice → the second is `RocksError::Locked` naming `<dir>/LOCK`.
  - `the_log_stays_bounded` (Review Focus 4): 20,000 puts of 2 KiB values → after each, the current log is under `flush_log_bytes + one batch`; reopen returns 20,000 keys.
  - `a_new_database_has_the_cpp_files`: a fresh `Db::open` + one put + close leaves `CURRENT`, `IDENTITY`, `LOCK`, one `MANIFEST-*`, one `OPTIONS-*`, a `.log`; `CURRENT` names the manifest with a trailing newline.
- [ ] **Step 2: Run** `cargo test -p doris-rocks --test db` → FAIL.
- [ ] **Step 3: Implement.** In memory: one `BTreeMap<Vec<u8>, Vec<u8>>` per column family plus `last_sequence`. Open replays tables (newest file wins by sequence) then logs (in log-number order). Flush, the only way files change: write one table per non-empty column family (all live keys, sequence 0, level 6 — as a bottommost compaction leaves them), write a new manifest holding one full snapshot edit (column families with ids, comparator, log number of the new log, next file number, last sequence, the new files), write a new `OPTIONS-<n>` modeled on the fixture's, then switch `CURRENT` by write-to-temp and rename, start the new log, delete files no longer referenced. Flush runs at open and when the log passes `flush_log_bytes`. `IDENTITY` holds a random UUID. `LOCK` is held with `std::fs::File::try_lock` (an advisory lock; ledger that the C++ BE uses `fcntl`, so this guards doris-rs against itself, and the swap check never runs both at once).
- [ ] **Step 4: Run** → PASS; `cargo test` and clippy clean.
- [ ] **Step 5: Commit** (`doris-rocks: the database`).

### Task 5: `doris-storage` — headers in `meta/`

**Files:**
- Create: `crates/doris-storage/src/olap_meta.rs`, `crates/doris-storage/tests/olap_meta.rs`
- Delete: `crates/doris-storage/src/header_store.rs`
- Modify: `crates/doris-storage/src/{lib.rs,error.rs,data_dir.rs,tablet_manager.rs}`, `crates/doris-storage/Cargo.toml`, `crates/doris-be/src/{config.rs,be.rs}`, `crates/doris-storage/tests/{data_dir.rs,tablets.rs}`, `crates/doris-rs/tests/process.rs`

**Interfaces:**
- Consumes: `doris_rocks::{Db, DbOptions, CfOptions, Batch}` (Task 4).
- Produces:
  - `OlapMeta::open(root: &Path) -> Result<OlapMeta, StorageError>` (`meta/` with `default`, `doris`, `meta` prefix 4); `save_tablet_meta(&mut self, meta: &TabletMetaPb, sync: bool)`; `remove_tablet_meta(&mut self, tablet_id: i64, schema_hash: i32)`; `tablet_metas(&self) -> Result<Vec<TabletMetaPb>, StorageError>`; `first_data_key(&self) -> Option<String>` (first key with a prefix of `rst_`, `ppi_`, `dlb_`, `binlog_`, `pui_`, `gc_`, `tgc_`); `has_legacy_header(&self) -> bool` (`hdr_`); `pub fn tablet_meta_key(tablet_id: i64, schema_hash: i32) -> String` → `tabletmeta_<id>_<hash>`.
  - `DataDir::meta(&mut self) -> &mut OlapMeta` (replaces `headers()`); `DataDir::open` no longer refuses `meta/`.
  - `TabletManager::open(dirs: Vec<DataDir>, opts: TabletOptions)`, `pub struct TabletOptions { pub sync_tablet_meta: bool }`; the first root gets `stream_load/` (database with `default`, `meta`).
  - `StorageError` gains `DataRoot { root: PathBuf, tablet_id: Option<i64>, key: Option<String> }` ("<root> holds a C++ BE's tablet data (tablet <id>); doris-rs can hold tablets with data from milestone 4"), `LegacyHeader { root }`, `BadHeader { key, why }`, `Rocks(#[from] RocksError)`; `ForeignRoot` is removed.
  - `BeConfig::sync_tablet_meta: bool` (key `sync_tablet_meta`, default false).

- [ ] **Step 1: Write the failing tests** in `tests/olap_meta.rs`:
  - `loads_the_cpp_headers`: a `DataDir` over a copy of `cpp-wal` (as a root: `meta/` plus the start folders) and `TabletManager::open` → the tablets equal the decoded `headers/*.json` (pbjson, as M2's header test) and `tablet_infos()` lists them.
  - `headers_land_in_meta`: `create_tablet` for the M2 fixture's CREATE requests, close, then `Db::open(<root>/meta)` directly → `tabletmeta_<id>_<hash>` keys whose values decode to the headers; `<root>/doris-rs-meta` does not exist.
  - `moves_the_temporary_store`: a root with M2's `doris-rs-meta/tabletmeta_<id>_<hash>` files → after open, the tablets are in `meta/` and `doris-rs-meta/` is gone.
  - `refuses_a_root_with_data`: `cpp-wal` root plus one header whose `rs_metas` has `num_segments = 1` → `DataRoot` naming that tablet; a root with an `rst_` key → `DataRoot` naming the key; nothing in the root changed (compare a recursive listing with sizes before and after).
  - `refuses_a_legacy_header`: an `hdr_1_1` key → `LegacyHeader`.
  - `rejects_a_header_under_the_wrong_key`: a header for tablet 7 stored as `tabletmeta_8_<hash>` → `BadHeader`.
  - `creates_stream_load_on_the_first_root_only`: two roots → `stream_load/CURRENT` in the first only.
  - `half_created_tablets_are_cleared` (Review Focus 5): M2's existing test of that name, moved to headers in `meta/`.
- [ ] **Step 2: Run** `cargo test -p doris-storage` → FAIL.
- [ ] **Step 3: Implement** as the interfaces say. Load-time checks follow `DataDir::load` (`be/src/storage/data_dir.cpp:347` and on): legacy key first, then data keys and data rowsets (refusal), then each `tabletmeta_` header (key matches id and hash, nonzero `tablet_uid`, a running tablet has versions, a rowset without its schema is saved again). `save_tablet_meta` writes with `sync = TabletOptions::sync_tablet_meta`. Rowset ids are already version 2 (`TabletManager::next_rowset_id`, M2); Task 5's header comparison checks them against the C++ BE's. Update `process.rs`: `a_foreign_root_stops_the_start` becomes `a_root_with_data_stops_the_start` (the error names the root and says "milestone 4").
- [ ] **Step 4: Run** `cargo test` → PASS (all crates); clippy clean.
- [ ] **Step 5: Commit** (`doris-storage: tablet headers in meta/, the C++ BE's startup checks`).

### Task 6: Drop as the C++ BE does it

**Files:**
- Modify: `crates/doris-storage/src/tablet_manager.rs`, `crates/doris-be/src/{be.rs,config.rs}`, `crates/doris-storage/tests/tablets.rs`
- Create: `crates/doris-be/src/sweeper.rs`

**Interfaces:**
- Produces: `TabletManager::drop_tablet` rewrites the header with `tablet_state = TABLET_SHUTDOWN` and keeps it in a shutdown list (the tablet leaves reports and stats at once); `TabletManager::sweep_shutdown(&mut self) -> Result<usize, StorageError>` moves each shut-down tablet's directory to `trash/` and deletes its key; `sweeper::spawn(tablets, cfg) -> SweeperHandle` runs the sweep on the C++ BE's schedule (`be/src/storage/olap_server.cpp`, `_garbage_sweeper_thread_callback`: first round after `min_garbage_sweep_interval`, then the interval it computes from disk usage, up to `max_garbage_sweep_interval`); `BeConfig::{min_garbage_sweep_interval, max_garbage_sweep_interval}` (180, 3600).

- [ ] **Step 1: Write the failing tests** in `tests/tablets.rs`:
  - `a_drop_marks_the_tablet_shut_down`: after DROP, the `meta/` value for the tablet decodes with state SHUTDOWN, its directory is still in `data/`, and `tablet_infos()` no longer lists it.
  - `the_sweep_moves_shut_down_tablets_to_trash`: then `sweep_shutdown()` → 1; the directory is under `trash/<YYYYmmddHHMMSS>.<n>/<tablet>/<hash>` and the key is gone.
  - `shut_down_tablets_are_swept_after_a_restart`: DROP, reopen, `sweep_shutdown()` → 1 (open put it on the list, not in the tablet map).
  - Replace M2's `a_drop_that_fails_on_disk_keeps_the_tablet` with `a_sweep_that_fails_on_disk_keeps_the_key` (an unwritable `trash/` → error, key and directory remain, next sweep retries).
- [ ] **Step 2: Run** `cargo test -p doris-storage --test tablets` → FAIL.
- [ ] **Step 3: Implement**, reading the C++ sweep (`tablet_manager.cpp`, `start_trash_sweep`) for order: move the directory, then delete the key. `sweeper.rs` computes the interval as `_garbage_sweeper_thread_callback` does; test its interval function against values worked out by hand from the C++ formula at disk usage 0, 0.5 and 0.95.
- [ ] **Step 4: Run** `cargo test` → PASS; clippy clean.
- [ ] **Step 5: Commit** (`doris-storage: drop marks SHUTDOWN, a sweep moves to trash`).

### Task 7: The swap check

**Files:**
- Create: `oracle/swap/swap-check.sh`, `oracle/swap/compare.py`, `oracle/tests/test_swap_compare.py`
- Modify: `oracle/Makefile` (`swap-check`), `crates/doris-be/src/agent.rs` (one stderr line per received task: `doris-rs be: task <TYPE> signature=<n>`)

**Interfaces:**
- Consumes: `oracle/swap/lib.sh` (Task 1), the doris-rs binary built with Tasks 2–6.
- Produces: `make swap-check` → last line `swap check: PASS` or `swap check: FAIL (<first reason>)`, exit status 0/1.

- [ ] **Step 1: Write the failing tests** in `oracle/tests/test_swap_compare.py` for `compare.py`, which takes two snapshots (JSON: `tablets` from `SHOW TABLETS` per table, `backend` from `SHOW BACKENDS`, `tasks` = task types received by the outgoing and incoming BE) and prints differences:
  - `test_identical_snapshots_pass`.
  - `test_a_changed_tablet_field_fails` for each of TabletId, ReplicaId, BackendId, SchemaHash, Version, PathHash, State.
  - `test_clocks_versions_and_facts_are_ignored`: `LastReportTime`, `LastStartTime`, `LastUpdateTime`, `Version` (binary), `CpuCores`, `Memory`, `TotalCapacity`, `AvailCapacity`, `DataUsedCapacity`, `TrashUsedCapacity` differ → pass.
  - `test_a_drop_or_clone_task_fails`: `tasks` contains `DROP` or `CLONE` → fail naming it.
  Run `make -C oracle test` → FAIL; implement `compare.py`; → PASS.
- [ ] **Step 2: Write `swap-check.sh`:** `swap-up` on an empty `SWAP_ROOT`; doris-rs starts, tables created (M2 shapes), two tablet reports, snapshot S0; stop doris-rs, start the C++ BE, wait alive and two reports, snapshot S1, `compare S0 S1`; create a table and drop one doris-rs created, two reports, snapshot S2; stop the C++ BE, start doris-rs, two reports, snapshot S3, `compare S2 S3`; create a table and drop one the C++ BE created; two reports; verify `SHOW TABLETS` lists the new table and not the dropped one. Before each swap, assert no tablet holds data (`SHOW TABLETS` row counts are 0) — a failure there is a harness failure. Tasks received come from doris-rs's stderr and the C++ BE's `be.INFO` (the line `agent_server.cpp` logs per submitted task; confirm its text in the source).
- [ ] **Step 3: Run** `make -C oracle swap-check` and fix until `swap check: PASS`. Each doris-rs fix comes with a test in the owning crate's test file (format findings in `doris-rocks`, header findings in `doris-storage`). Ledger each finding.
- [ ] **Step 4: Commit** (`oracle: the swap check`).

### Task 8: Docs and the finished milestone

**Files:**
- Modify: `docs/adr/0003-cpp-data-directory-in-pure-rust.md` (consequences: the temporary store is gone; roots with data refused until M4), `oracle/README.md` (swap cluster, `make meta-fixtures`, `make swap-check`), `crates/doris-be/tests/fixtures/m2/README.md` (the layout's `stream_load` and drop notes, if changed), this plan ("As built")

- [ ] **Step 1: Run** `make -C oracle fe-up`, `make rust-be`, `RUN_ID=m3-full-1 make epic-check TARGET=rust` (the M2 gates must still hold: `make epic-grade` ok with the committed ratchets). A failing suite is triaged like M2's Task 11.
- [ ] **Step 2: Write the docs** listed above.
- [ ] **Step 3: Run** `cargo test`, `cargo clippy --all-targets`, `make -C oracle test`, `make -C oracle swap-check`. Expected: all pass, `swap check: PASS`.
- [ ] **Step 4: Commit** (`M3: docs, As built`).
