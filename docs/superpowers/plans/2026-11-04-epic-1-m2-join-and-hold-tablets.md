# Epic 1 · M2: Join the Cluster and Hold Tablets — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `doris-rs be --config <file>` joins the single-profile cluster as its only BE, holds tablets across restarts in the C++ directory layout, and the gate suites that need only that pass on both gates.

**Architecture:** Three new crates. `doris-storage` owns data directories and tablets (C++ layout, `TabletMetaPB` headers in a temporary store outside `meta/`). `doris-be` owns the services: Thrift servers (heartbeat, BackendService), the FrontendService client, agent tasks, reports, and the HTTP and brpc listeners. `doris-rs` is the binary. Every behavior is pinned by fixtures captured from the reference C++ BE, compared after an explicit, cited list of deployment- and time-dependent fields.

**Tech Stack:** Rust 1.93; `thrift` 0.25 sync processors (generated in `doris-thrift`) on std threads; `tokio` + `hyper` 1 for HTTP/1.1 and the brpc port's HTTP/2 gRPC; `prost` (+ `pbjson` for fixture comparison only); oracle harness in standard-library Python.

**Spec:** `docs/superpowers/specs/2026-11-04-epic-1-m2-join-and-hold-tablets-design.md` (parent: `docs/superpowers/specs/2026-11-02-epic-1-reporting-design.md`).

## As built (differences from the tasks below)

The tasks below are the plan as written. Implementation changed these:

- **Review Focus 1 and 3 were wrong about the C++ BE.** It refuses `submit_tasks` before the first
  heartbeat (CANCELLED "Have not get FE Master heartbeat yet") and answers `DROP` of an unknown
  tablet with NOT_FOUND "could not find tablet <id>"; doris-rs does the same.
- **Reports.** Four, not three: an index-policy report every 10 s joins task, disk and tablet.
  None is sent immediately after a tablet change; all are sent on their intervals and at once
  when a heartbeat changes the master or epoch. `report_version` is `time(nullptr) * 100000` plus
  one per change, as in the C++ BE.
- **Crates.** A dev-only `doris-fixtures` crate reads the captured fixtures for the storage and BE
  tests. `http::serve` takes only a listener.
- **Fixtures.** Headers cover the 7 scenario tablets; the FE's own tablets hold audit-log data
  and are left out of header, tablet-report and tablet-stat comparisons.
- **Eligibility.** `grade.py` counts eligible suites with derive's needs rule (a suite needs its
  directory's load suites), plus `oracle/needs_supplement.json`: declared needs the traffic
  corpus cannot show, each with its evidence. Three kinds so far: a load suite outside the gate,
  a stream load the corpus lost, and `state:global wait_timeout=31000` (63 suites whose JDBC
  connect query reads a global variable that `test_query_sys_tables` sets partway through the run
  and cannot reach on doris-rs yet). Eligible: 155, not ~226.
- **FE-only cluster.** A separate FE does not get `force_olap_table_replication_num = 1` and
  `disable_balance = true` from the entrypoint; `fe-only.yml` sets them, as `reference-split.yml`
  does.
- **Result.** Run `m2-full-2` (2026-11-05): 155 eligible suites pass both gates; the ratchets
  hold 250 out-gate and 81 strict-gate suites.

## Global Constraints

- No C++ or RocksDB dependency, in the binary or in tests. Nothing is ever written to `<root>/meta`.
- doris-rs never crashes or exits on a request it does not support: it answers with a Doris error status (`TStatusCode::NOT_IMPLEMENTED_ERROR`, code 3) whose message names the missing method, task type or route.
- Every field the C++ BE sends is sent with the C++ BE's value or a declared deployment fact; fixture comparisons may overwrite only the fields in the task's cited mask list.
- Default ports are the C++ BE's: heartbeat 9050, be_port 9060, brpc 8060, webserver 8040.
- Version string in heartbeats: `doris-4.1.5-release-917b20be838` (the reference's).
- doris-rs runs natively on the host; Docker appears only in `oracle/`. Harness code is standard-library Python.
- `.cache/doris-src` is read-only. The repo is public: no secrets.
- Each task leaves `cargo test`, `make -C oracle test` and `cargo clippy --all-targets` green.

## Review Focus

1. **Tasks before the first heartbeat:** the FE can send `submit_tasks` before doris-rs knows the master address; the task still runs and its `finishTask` is delivered once the master is known (Task 6 test).
2. **Repeated `CREATE` for an existing tablet** (FE retry after timeout): succeeds without a second tablet, as the C++ BE does (Task 4 test).
3. **`DROP` of an unknown tablet:** succeeds, as the C++ BE does (Task 4 test).
4. **Crash between writing a header and creating its directory:** after restart, a header without a directory or a directory without a header leaves no reported tablet and no stray state (Task 3 test).
5. **Heartbeat carrying a different `cluster_id`** than the root's `cluster_id` file: rejected with the C++ BE's error, the file untouched (Task 5 test).

---

### Task 1: Reference fixtures for M2

**Files:**
- Create: `oracle/wire/scenarios/m2.sh`
- Modify: `oracle/wire/bin/capture.sh` (take `SCENARIO` and `OUT` from the environment; defaults unchanged), `oracle/Makefile` (target `be-fixtures`)
- Create: `crates/doris-be/tests/fixtures/m2/` (committed output: `messages/`, `manifest.json`, `headers/<tablet_id>.json`, `layout.txt`, `README.md`)
- Test: `oracle/tests/test_fixtures.py`

**Interfaces:**
- Produces: the fixture directory above. `manifest.json` lists messages as the wire corpus does (`oracle/wire/corpus/manifest.json`); `headers/<tablet_id>.json` is the reference BE's `GET /api/meta/header/<tablet_id>?byte_to_base64=true`; `layout.txt` is `find <storage_root> -not -path '*/meta/*' | sort` with the root replaced by `$ROOT`.

- [ ] **Step 1: Write the failing test** `test_fixture_set_is_complete`: loads `manifest.json` and asserts at least one call and one reply for each of `heartbeat`, `report`, `submit_tasks`, `finishTask`, `get_tablet_stat`, `publish_topic_info`, `get_dictionary_status`; that the decoded task types (from the manifest's `task_types` field, written by the scenario) include `CREATE`, `DROP` and `UPDATE_VISIBLE_VERSION`; that every tablet id in a `CREATE` task has a `headers/<id>.json`.
- [ ] **Step 2: Run** `make -C oracle test`. Expected: FAIL, fixture directory missing.
- [ ] **Step 3: Write `m2.sh`.** Same shape as `wire/scenario.sh`. Tables: a duplicate-key table (2 buckets, INT/BIGINT/VARCHAR/DATETIMEV2/DECIMAL64 columns); a unique-key merge-on-write table; a range-partitioned table with 2 partitions; one `ALTER TABLE ... ADD PARTITION`. Then, for every tablet from `SHOW TABLETS FROM <table>`, save the header JSON. Then write `layout.txt`, `DROP TABLE` one table, sleep past `report_tablet_interval_seconds` (60) and one `get_tablet_stat` cycle, and append the layout after the drop to `layout.txt` under a `# after drop` line.
- [ ] **Step 4: Capture.** Run `make up` (reference-single), then `make be-fixtures`, which runs `SCENARIO=wire/scenarios/m2.sh OUT=../crates/doris-be/tests/fixtures/m2 wire/bin/capture.sh` and the extract step. `README.md` records the date, image tag, and the command.
- [ ] **Step 5: Run** `make -C oracle test`. Expected: PASS.
- [ ] **Step 6: Commit** `oracle/wire/scenarios/m2.sh oracle/wire/bin/capture.sh oracle/Makefile oracle/tests/test_fixtures.py crates/doris-be/tests/fixtures/m2`.

### Task 2: Crates, config and the binary shell

**Files:**
- Create: `crates/doris-storage/{Cargo.toml,src/lib.rs}`, `crates/doris-be/{Cargo.toml,src/lib.rs,src/config.rs}`, `crates/doris-rs/{Cargo.toml,src/main.rs}`
- Modify: `Cargo.toml` (members; workspace deps `tokio`, `hyper`, `hyper-util`, `http-body-util`, `pbjson`, `pbjson-build`, `tempfile`)
- Test: `crates/doris-be/tests/config.rs`

**Interfaces:**
- Produces: `doris_be::config::BeConfig { be_port: u16, heartbeat_service_port: u16, brpc_port: u16, webserver_port: u16, storage_root_paths: Vec<PathBuf>, priority_networks: Option<String>, report_task_interval_seconds: u64, report_disk_state_interval_seconds: u64, report_tablet_interval_seconds: u64, host: Option<String> }` and `BeConfig::parse(text: &str) -> Result<BeConfig, ConfigError>`. The file format is the C++ `be.conf`: `key = value` lines, `#` comments, C++ key names, so an existing `be.conf` works unchanged. `storage_root_path` is `;`-separated; each entry may carry `,medium:HDD` / `,capacity:N` suffixes, kept and ignored in M2. `host` is the address the BE registers as, which doris-rs uses for `hash_of_path` and reports; when unset, the first local address matching `priority_networks`, else `127.0.0.1`. Unknown keys are kept and ignored.
- Produces: `doris-rs be --config <file>` parses the config and exits 0 (servers come in Task 9).

- [ ] **Step 1: Write the failing tests** `defaults_are_the_cpp_be_defaults` (empty text → ports 9050/9060/8060/8040, intervals 10/30/60), `reads_a_reference_be_conf` (the `BE_CONFIG_EXTRA` block of `oracle/compose/reference-single.yml` plus `storage_root_path = /a;/b,medium:SSD` → two roots, unknown keys ignored), `rejects_a_non_numeric_port` (error names the key).
- [ ] **Step 2: Run** `cargo test -p doris-be --test config`. Expected: FAIL to compile.
- [ ] **Step 3: Implement** the crates and `BeConfig::parse`. `doris-rs` uses only `std::env::args` (no CLI crate).
- [ ] **Step 4: Run** `cargo test -p doris-be --test config && cargo run -p doris-rs -- be --config /dev/null`. Expected: PASS, exit 0.
- [ ] **Step 5: Commit.**

### Task 3: Data directories in the C++ layout

**Files:**
- Create: `crates/doris-storage/src/{data_dir.rs,path_hash.rs,header_store.rs}`
- Test: `crates/doris-storage/tests/data_dir.rs`

**Interfaces:**
- Produces: `doris_storage::hash_of_path(identifier: &str, path: &str) -> u64`, the C++ `hash_of_path` (`be/src/util/string_util.cpp:26`): `std::hash<std::string>` of the identifier, then the boost-style `hash_combine` (`be/src/util/hash_util.hpp:426`) of each non-empty `/` segment. `std::hash<std::string>` is libstdc++'s `_Hash_bytes` (seed `0xc70f6907`); the test value settles it.
- Produces: `DataDir::open(root: &Path, host: &str) -> Result<DataDir, StorageError>`; `DataDir::cluster_id(&self) -> Option<i32>`; `DataDir::set_cluster_id(&mut self, id: i32) -> Result<(), StorageError>`; `DataDir::path_hash(&self) -> i64`; `DataDir::next_shard(&mut self) -> u64`; `DataDir::tablet_dir(&self, shard: u64, tablet_id: i64, schema_hash: i32) -> PathBuf`; `DataDir::move_to_trash(&self, dir: &Path) -> Result<(), StorageError>`; `DataDir::headers(&self) -> &HeaderStore`. 
- Produces: `HeaderStore` at `<root>/doris-rs-meta/` (temporary until the meta store milestone): `put(tablet_id: i64, schema_hash: i32, meta: &TabletMetaPB)`, `remove(tablet_id, schema_hash)`, `load_all() -> Vec<TabletMetaPB>`; one file per tablet named `tabletmeta_<tablet_id>_<schema_hash>`, the protobuf bytes, written to a temp name then renamed.
- Folder names, shard rule and `cluster_id` semantics come from `be/src/storage/olap_define.h:88-101` and `be/src/storage/data_dir.cpp` (`_init_cluster_id`, `set_cluster_id`, `get_shard`); `move_to_trash` follows `DataDir::move_to_trash` (timestamped folder under `trash/`).

- [ ] **Step 1: Write the failing tests:**
  - `path_hash_matches_the_reference`: `hash_of_path("127.0.0.1", "/opt/apache-doris/be/storage") == 8278071542442530434` (from the wire corpus' `TDisk.path_hash`).
  - `a_new_root_gets_the_cpp_folders`: after `open` on an empty temp dir, the top-level entries equal those in the fixture `layout.txt` minus `meta`, plus `doris-rs-meta`, and no `meta/` exists.
  - `cluster_id_is_written_once`: `set_cluster_id(7)` writes `cluster_id` containing `7`; reopening reads 7; `set_cluster_id(8)` then errors and the file still says 7.
  - `a_root_with_reference_contents_is_refused`: a root containing `meta/` → `StorageError::ForeignRoot` whose message says the root holds a C++ BE's metadata and doris-rs cannot use it before the meta store milestone; same for a root with `data/0/<n>/` but no `doris-rs-meta/`.
  - `headers_survive_reopen` and (Review Focus 4) `half_created_tablets_leave_no_trace`: a header without its tablet directory is dropped from `load_all` and removed; a tablet directory without a header is moved to `trash/` on `open`.
- [ ] **Step 2: Run** `cargo test -p doris-storage --test data_dir`. Expected: FAIL.
- [ ] **Step 3: Implement** the three modules.
- [ ] **Step 4: Run** the tests. Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 4: Tablets: create, drop, visible version, tablet info

**Files:**
- Create: `crates/doris-storage/src/{tablet.rs,tablet_manager.rs}`
- Modify: `crates/doris-proto/{Cargo.toml,build.rs}` (feature `json`: `pbjson-build` with `preserve_proto_field_names`, `ignore_unknown_fields`)
- Test: `crates/doris-storage/tests/tablets.rs` (dev-dependency `doris-proto` with `json`)

**Interfaces:**
- Consumes: `DataDir`, `HeaderStore` (Task 3).
- Produces: `TabletManager::open(dirs: Vec<DataDir>) -> Result<TabletManager, StorageError>`; `create_tablet(&mut self, req: &TCreateTabletReq) -> Result<TTabletInfo, StorageError>`; `drop_tablet(&mut self, req: &TDropTabletReq) -> Result<(), StorageError>`; `update_visible_version(&mut self, partition_id: i64, version: i64)`; `tablet_infos(&self) -> BTreeMap<i64, TTablet>` (report form); `tablet_stats(&self) -> Vec<TTabletStat>`; `report_version(&self) -> i64` (C++ `_s_report_version`: `unix_seconds * 10000` at start, `+1` on every tablet change). Also `cluster_id(&self) -> Option<i32>` and `set_cluster_id(&mut self, id: i32) -> Result<(), StorageError>`, applied to every `DataDir`, and `data_dirs(&self) -> &[DataDir]` for reports.
- `create_tablet` builds the `TabletMetaPB` as `TabletManager::_create_tablet_meta_unlocked` and the `TabletMeta` constructor do (`be/src/storage/tablet/tablet_manager.cpp:1519`, `tablet_meta.cpp`), including the initial empty rowset for versions [0, 1] with its `RowsetMetaPB`; then the tablet directory, then the header.

- [ ] **Step 1: Write the failing tests:**
  - `created_headers_match_the_reference`: for each `CREATE` task in the fixtures, `create_tablet`, then load the fixture `headers/<tablet_id>.json` into `TabletMetaPB` via pbjson and compare with the stored header. Before comparing, the test overwrites only these fields from the expected value, each with a comment citing the C++ line that makes it time- or randomness-dependent: `creation_time`, `tablet_uid`, every rowset's `rowset_id`, `rowset_id_v2`, `creation_time`, `newest_write_timestamp`, `shard_id`. Any other difference fails the test.
  - `finish_info_matches_the_reference`: the returned `TTabletInfo` equals the fixture `finishTask`'s `finish_tablet_infos` entry for that tablet, with `path_hash` set from the fixture (deployment fact).
  - `tablet_dir_follows_the_layout`: after creating the fixture tablets, the tree under `data/` matches `layout.txt` up to shard numbers.
  - (Review Focus 2) `create_is_idempotent`: a second identical `create_tablet` returns Ok and one tablet remains.
  - (Review Focus 3) `dropping_an_unknown_tablet_succeeds`.
  - `drop_moves_the_tablet_to_trash`: the directory lands under `trash/` and the header is gone, matching the `# after drop` part of `layout.txt`.
  - `tablets_survive_restart`: create, drop the `TabletManager`, reopen on the same roots → `tablet_infos()` equal before and after; `report_version` keeps increasing.
- [ ] **Step 2: Run** `cargo test -p doris-storage --test tablets`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the tests. Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 5: Heartbeat service

**Files:**
- Create: `crates/doris-be/src/{heartbeat.rs,master.rs}`
- Test: `crates/doris-be/tests/heartbeat.rs`

**Interfaces:**
- Consumes: `TabletManager::cluster_id` / `set_cluster_id` (Task 4).
- Produces: `MasterInfo` (shared, `Arc<RwLock<Option<TMasterInfo>>>`) with `MasterInfo::fe_address(&self) -> Option<TNetworkAddress>` and `backend_id()`; `HeartbeatHandler` implementing `HeartbeatServiceSyncHandler`. Behavior follows `be/src/agent/heartbeat_server.cpp` (`_heartbeat`): cluster id and token checks and assignment, epoch handling, `backend_ip` adoption, reply fields. `TBackendInfo`: ports from config, `be_rpc_port = -1`, version per Global Constraints, `be_node_role = "mix"`, `is_shutdown = false`, `arrow_flight_sql_port = 8050`, `be_mem` = physical memory, `be_start_time` = process start in ms, `fragment_executing_count = 0`, `fragment_last_active_time` and `supports_paimon_rust_reader` as the C++ BE sets them.

- [ ] **Step 1: Write the failing tests:**
  - `heartbeat_reply_matches_the_reference`: for each fixture heartbeat call, the handler's reply equals the fixture reply after overwriting only `be_start_time`, `fragment_last_active_time` (time) and `be_mem` (deployment fact `be-mem`).
  - `first_heartbeat_assigns_cluster_id`: a fresh root gets the call's `cluster_id` in its `cluster_id` file; `MasterInfo::fe_address()` is the call's `network_address`.
  - (Review Focus 5) `a_different_cluster_id_is_rejected`: root holds 7, call carries 8 → reply status and message equal to the C++ BE's for that case; the file still says 7.
- [ ] **Step 2: Run** `cargo test -p doris-be --test heartbeat`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the tests. Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 6: Agent tasks and the FrontendService client

**Files:**
- Create: `crates/doris-be/src/{agent.rs,fe_client.rs}`
- Test: `crates/doris-be/tests/agent.rs`

**Interfaces:**
- Consumes: `TabletManager` (Task 4), `MasterInfo` (Task 5).
- Produces: `trait FeClient: Send + Sync { fn finish_task(&self, req: TFinishTaskRequest) -> Result<TMasterResult, FeError>; fn report(&self, req: TReportRequest) -> Result<TMasterResult, FeError>; }` with `ThriftFeClient` (strict binary, unframed, to `MasterInfo::fe_address()`, as `dispatch.rs` describes the reference's ports) and a recording `FakeFe` in `doris_be::testing`.
- Produces: `AgentService::submit_tasks(&self, tasks: Vec<TAgentTaskRequest>) -> TAgentResult` (returns at once, as the C++ BE does; tasks run on worker threads) and `AgentService::running_tasks(&self) -> BTreeMap<TTaskType, BTreeSet<i64>>` for the task report. `CREATE`, `DROP` and `UPDATE_VISIBLE_VERSION` follow `be/src/agent/task_worker_pool.cpp` (which of them send `finishTask`, and with which fields). Every other task type sends `finishTask` with `NOT_IMPLEMENTED_ERROR` and the message `doris-rs does not support agent task <TTaskType name> yet`. `finishTask` retries on failure as the C++ BE does and waits while no master is known.

- [ ] **Step 1: Write the failing tests:**
  - `create_and_drop_finish_like_the_reference`: replay the fixture `submit_tasks` calls in order through a `FakeFe`; the `submit_tasks` replies equal the fixture replies, and the recorded `finishTask` requests equal the fixture `finishTask` calls, after overwriting only `backend.host` (fact `be-host`), `report_version` (time), and `path_hash` (fact).
  - `an_unsupported_task_finishes_with_an_error`: an `ALTER` task → one `finishTask` with status code 3 and a message containing `ALTER`; the process keeps serving.
  - (Review Focus 1) `tasks_before_the_first_heartbeat_finish_later`: submit a `CREATE` with no master known → no `finishTask` yet; set the master → exactly one `finishTask` arrives.
- [ ] **Step 2: Run** `cargo test -p doris-be --test agent`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the tests. Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 7: Reports and the rest of BackendService

**Files:**
- Create: `crates/doris-be/src/{report.rs,backend_service.rs}`
- Test: `crates/doris-be/tests/report.rs`, `crates/doris-be/tests/backend_service.rs`

**Interfaces:**
- Consumes: Tasks 4–6.
- Produces: `Reporter::spawn(cfg: &BeConfig, tablets, agent, fe: Arc<dyn FeClient>, master: MasterInfo)`: task, disk and tablet reports on the config intervals, plus an immediate tablet report after a tablet change, as `be/src/agent/report_worker` / `task_worker_pool.cpp` do; `Reporter::build_tablet_report(&self) -> TReportRequest` and `build_disk_report`, `build_task_report` for tests. Disk numbers come from `statvfs` on each root and the size of `data/` and `trash/`.
- Produces: `BackendHandler` implementing `BackendServiceSyncHandler`. `submit_tasks` delegates to `AgentService`; `get_tablet_stat`, `publish_topic_info`, `get_dictionary_status` answer as the C++ BE does (`be/src/service/backend_service.cpp`); every other method returns a result whose status is `NOT_IMPLEMENTED_ERROR` with the message `doris-rs does not support BackendService.<method> yet`. A method whose result type has no status field returns a Thrift application exception with that message instead. Handlers run inside `catch_unwind`; a panic becomes that error, never a dead connection pool.

- [ ] **Step 1: Write the failing tests:**
  - `reports_match_the_reference`: after replaying the fixture `CREATE` tasks, the built disk and tablet reports equal the fixture `report` calls that follow them, after overwriting only `backend.host`, `report_version`, `root_path` (fact `be-data-path`), the capacity fields (deployment, already masked by probe marks), and `num_cores`/`pipeline_executor_size` (fact).
  - `background_calls_match_the_reference`: fixture `get_tablet_stat`, `publish_topic_info`, `get_dictionary_status` calls → replies equal the fixture replies (stat values compared exactly).
  - `every_other_method_errors_and_names_itself`: for each BackendService method in `doris_thrift::gen::methods::METHODS` not in the supported set, a call with default arguments returns code 3 (or an application exception) whose message contains the method name.
- [ ] **Step 2: Run** `cargo test -p doris-be --test report --test backend_service`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the tests. Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 8: HTTP and brpc listeners

**Files:**
- Create: `crates/doris-be/src/{http.rs,brpc.rs}`
- Test: `crates/doris-be/tests/http.rs`

**Interfaces:**
- Produces: `http::serve(addr, state) -> impl Future` (hyper, HTTP/1.1). `GET /api/health` and `GET /api/running_pipeline_tasks/{n}` answer with the C++ BE's status and body (`be/src/service/http/action/health_action.cpp`, `running_pipeline_tasks_action` or its equivalent; bodies taken from the anchor run's traffic). Every other route answers as the C++ BE answers an action that fails: its JSON error shape with `msg` naming the route template from `oracle/wire/be_http_routes.txt` (e.g. `doris-rs does not support /api/compaction/run yet`), or 404 with the C++ body for routes not in that file.
- Produces: `brpc::serve(addr) -> impl Future` (hyper, HTTP/2 prior knowledge, gRPC framing). Every call `/doris.PBackendService/<method>` answers grpc-status 0 with a message whose field 1 is `PStatus { status_code: 3, error_msgs: ["doris-rs does not support PBackendService.<method> yet"] }`, the leading status field every `P*Result` in `internal_service.proto` carries. Non-gRPC bytes (`baidu_std`) close the connection without a panic.

- [ ] **Step 1: Write the failing tests** `health_matches_the_reference`, `running_pipeline_tasks_matches_the_reference`, `an_unsupported_route_names_itself` (`/api/compaction/run` → error JSON naming the route), `grpc_calls_get_a_not_implemented_status` (a `fetch_data` request → decodes as `PFetchDataResult` with status code 3 naming `fetch_data`), `baidu_std_bytes_do_not_crash` (send `PRPC` + garbage, then a valid gRPC call on a new connection still answers).
- [ ] **Step 2: Run** `cargo test -p doris-be --test http`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the tests. Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 9: The binary: all four ports, start and restart

**Files:**
- Modify: `crates/doris-rs/src/main.rs`, `crates/doris-be/src/lib.rs` (`pub fn run(cfg: BeConfig) -> Result<(), BeError>` and `Be::start(cfg) -> Result<Be, BeError>` with `Be::shutdown(self)` for tests)
- Test: `crates/doris-rs/tests/process.rs`

**Interfaces:**
- Consumes: everything above. Thrift servers use the generated `*SyncProcessor`s over strict binary, unframed transport, one thread per connection.
- Produces: `doris-rs be --config <file>` listens on the four ports, logs one line per port to stderr, refuses foreign roots (Task 3) with exit code 1 and the error message.

- [ ] **Step 1: Write the failing tests** (ports from the config, chosen free by the test):
  - `the_binary_answers_on_every_port`: spawn `doris-rs be`, then a heartbeat over the generated `HeartbeatServiceSyncClient`, `GET /api/health`, a gRPC call, and a `BackendService.get_tablet_stat` call all answer.
  - `tablets_survive_a_process_restart`: heartbeat with a fake FE address (a `FakeFe` Thrift server in the test), send the fixture `CREATE` tasks, wait for the tablet report, kill the process, start it again, wait for its first tablet report → the same `tablets` map.
  - `a_foreign_root_stops_the_start`: a root containing `meta/` → exit code 1, stderr names the root.
- [ ] **Step 2: Run** `cargo test -p doris-rs`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `cargo test` (whole workspace). Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 10: The FE-only cluster and the rust target

**Files:**
- Create: `oracle/compose/fe-only.yml`, `oracle/clusters/rust.facts.json`, `oracle/rust/be.conf`
- Modify: `oracle/targets/rust.env`, `oracle/Makefile` (targets `fe-up`, `fe-down`, `rust-be`), `oracle/checker/grade.py` (eligible suites), `oracle/README.md`
- Test: `oracle/tests/test_grade.py`

**Interfaces:**
- `fe-only.yml`: the reference image and the same `FE_CONFIG_EXTRA` as `reference-single.yml`, starting only the FE (the all-in-one image's FE start script), FE ports published on the host, healthy when the FE answers on 9030. How the FE container reaches doris-rs on the host and which address doris-rs registers with is settled here for macOS with OrbStack (e.g. `ALTER SYSTEM ADD BACKEND "host.docker.internal:9050"`, with whatever FE setting a host name needs); the facts file records it.
- `make rust-be` builds `doris-rs` in release mode and runs `doris-rs be --config oracle/rust/be.conf` with `storage_root_path` under `oracle/results/rust/storage` (untracked); `make fe-up` adds the backend once.
- `grade.py` gains `eligible`: gate suites whose needs (from the reference's `needs.json`) are all in `scores/epic1-supported.json`, and reports `eligible failing` per gate. The report line: `eligible: N; failing out-gate: A; failing strict: B`.

- [ ] **Step 1: Write the failing test** `test_eligible_suites_follow_supported_features` in `test_grade.py`: two suites, supported features cover one → `eligible == [that suite]`, and a failing eligible suite is listed under its gate.
- [ ] **Step 2: Run** `make -C oracle test`. Expected: FAIL.
- [ ] **Step 3: Implement** `grade.py`, then the compose file, target, facts and Makefile targets.
- [ ] **Step 4: Verify** `make -C oracle test` passes; then `make fe-up && make rust-be` (second terminal) and `make sql` → `SHOW BACKENDS` lists doris-rs `Alive: true` within one heartbeat interval.
- [ ] **Step 5: Commit.**

### Task 11: The suite run, scores and docs

**Files:**
- Modify: `oracle/scores/epic1-supported.json`, `oracle/scores/epic1-out.txt`, `oracle/scores/epic1-strict.txt`, `oracle/README.md`, `docs/superpowers/specs/2026-11-02-epic-1-reporting-design.md` (§6 milestone table: new M3 "C++-compatible data directory", write and read loop becomes M4), this plan ("As built")
- Create: `docs/adr/0003-cpp-data-directory-in-pure-rust.md`

- [ ] **Step 1: Mark M2's features supported:** `task:CREATE`, `task:DROP`, `task:UPDATE_VISIBLE_VERSION`, `rpc:BackendService.submit_tasks`, `rpc:FrontendService.finishTask`, and the `type:*` features that appear in the eligible suites' `CREATE` tasks (types are needed only to hold a tablet's schema; later milestones re-check them for execution). Run `make epic-grade RUN=results/reference-single/<self-check run>` and confirm the eligible count is about 226 (168 without BE work + 58).
- [ ] **Step 2: Run** `make epic-check TARGET=rust` with `make fe-up` and `make rust-be` running.
- [ ] **Step 3: Fix until every eligible suite passes both gates.** Each failure is triaged against the fixtures and the C++ source; a fix comes with a test in the owning task's test file. A failure that needs a feature beyond M2 is not fixed: its suite leaves the eligible set because the needs index missed a feature, and that miss is fixed in `needs.rs` (with a test), never by an ignore entry.
- [ ] **Step 4: Ratchets:** write the passing suites to `epic1-out.txt` and `epic1-strict.txt`; rerun `make epic-grade` → `ok`.
- [ ] **Step 5: Docs:** ADR 0003 (the data directory uses the C++ layout so a node can switch binaries in place; the RocksDB-format `meta/` store is pure Rust with no RocksDB dependency, its own milestone; alternatives: own format, linking RocksDB); the epic spec's table; `oracle/README.md` (FE-only cluster, `make rust-be`, M2 numbers); "As built" here.
- [ ] **Step 6: Run** `cargo test && make -C oracle test`. Expected: PASS. **Commit.**
