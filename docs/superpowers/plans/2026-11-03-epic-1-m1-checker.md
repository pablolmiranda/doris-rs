# Epic 1, Milestone 1: the checker — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `oracle/` so that every later milestone of Epic 1 is graded by the upstream p0 suites on two gates (the suite's own result and byte-equal MySQL replies), with a per-suite FE↔BE traffic corpus, a needs index and ratchet scores, and prove it by checking the reference against itself.

**Architecture:** A static scope script picks the in-scope suites. Four serial reference runs (two on the all-in-one cluster, two on a split FE+BE cluster with different deployment facts) run with a MySQL tap in front of the FE and, on the first run, a packet capture on the BE ports. Offline tools turn the tap logs into canonical per-suite replies and probe marks, the pcap into a per-suite traffic corpus, and the corpus into a needs index. `make epic-check` runs a target the same way and gates on ratchets.

**Tech Stack:** Standard-library Python 3.11 (`unittest`, `asyncio`, `gzip`, `difflib`), bash, Docker Compose, tshark/tcpdump (already used by `oracle/wire`), Rust (`doris-wire`, `doris-thrift`, `doris-proto`, plus `flate2`).

**Spec:** `docs/superpowers/specs/2026-11-02-epic-1-reporting-design.md` (§4 is this milestone). ADRs `docs/adr/0001…`, `0002…`.

## As built (differences from the tasks below)

The tasks below are the plan as written. Implementation (#12) changed these, in agreement with
the test seams settled for #12 (`grade RUN_DIR`, `derive RUN_DIRS`, MySQL decoding on synthetic
bytes, the doris-wire round-trip, live runs):

- **Layout.** All checker code is in `oracle/checker/` (`tap.py`, `mysql_wire.py`, `replies.py`,
  `marks.py`, `derive.py`, `grade.py`, `scope.py`, `traffic.py`, `records.py`, `rank.py`); the
  derived reference products live in one committed directory, `oracle/epic1/` (`suites.txt`,
  `flaky.txt`, `baseline.json`, `replies/`, `marks.json`, `traffic/`, `reaches_be.txt`,
  `needs.json`, `ranking.json`). Ratchets and the supported-feature list are in `oracle/scores/`.
- **Seams.** Most behavior is tested through `derive.derive` and `grade.grade` on synthetic run
  directories (`oracle/tests/runfake.py`) rather than per-module unit tests.
- **Traffic.** A run's pcap becomes the run's `traffic.bin.gz` (`checker/traffic.py`) right after
  the run; `derive` splits it per suite. The wire extractor now streams tshark's JSON.
- **Binary rows** keep the server's exact value encoding instead of being re-rendered as text.
- **Real runs** added: OK info is length-encoded under `CLIENT_SESSION_TRACK`; a suite's
  exchanges are ordered connection by connection (interleaving varies); `COM_QUIT` is not an
  exchange; cell facts can be scoped to columns (`CpuCores`, ports) so ordinary values are
  never substituted.
- **Harness bug fixed:** `run_suites.sh` passed the conf directory to `--conf`, which upstream
  ignores, so every run used the source tree's default conf.
- **Cluster B** reports the host's CPU count whatever its cpuset; recorded under `not_varied`.
- **Five reference runs**, not four: a1 and a2 on one cluster A, a3 on a fresh cluster A, b1 and
  b2 each on a fresh cluster B. The fifth was added after a self-check found rare variation four
  runs had not shown.
- **Cluster B memory:** the BE gets 75% of its 10g container (`BE_MEM_LIMIT`); 30% and 40% ran
  out of memory under the suites.
- **Live facts:** BE cores and memory are filled from `SHOW BACKENDS` at run time
  (`checker/facts.py`, `"from_backends"` in `clusters/*.facts.json`), so no host value is committed.
- **Corpus size:** HTTP bodies over 64 KiB are stored as length plus SHA-256 (`body_omitted`);
  `get_tablet_stat` is background; gRPC frames tshark cannot name are listed in the manifest
  (`unnamed_grpc`), not kept. The corpus is 48 MB in plain git. The pcap is streamed
  (`checker/pcapstream.py`).
- **Attribution:** a connection belongs to the suite whose window holds its handshake; its
  exchanges outside that window are unattributed. A suite's connections are compared in order of
  their content, not their time. Commands with no reply (`COM_QUIT`, `COM_STMT_CLOSE`,
  `COM_STMT_SEND_LONG_DATA`) are dropped.
- **Background traffic** is declared, each entry with the code that sends it, in
  `oracle/background_traffic.json`: the harness's (the framework's query-timeout plugin, a
  `show backends` connection every 5 minutes and BE polling every minute, and the image's
  health check) and the cluster's periodic RPCs, which replace `BACKGROUND_METHODS`. It is left
  out of replies and the corpus and counted in the manifest. The relaxation rule below gains
  this third, declared source.
- **Reaches the BE** means any request to the BE (gRPC, HTTP, Thrift `BackendService` calls),
  not a list of methods. **HTTP records** are named by the BE route template they hit
  (`oracle/wire/be_http_routes.txt`), so ids and table names do not split one endpoint into many
  features. **`reportExecStatus`** is a suite's need only for a query its own plans started; late
  reports from the previous suite's queries are not.
- **Lost bytes:** `traffic-holes.json` lists every direction that lost bytes, a capture hole or
  a message cut off at the end, not only holes.
- **Codec patch 4:** profile info strings are bytes, not strings (the BE writes raw key bytes).
- **No per-statement skip:** a reply that varied beyond any mark makes its suite unstable, as
  the spec says (41 unstable suites).
- **Self-check result:** `.out` 1622/1622, strict 1489/1498 compared; the 9 are rare Doris
  nondeterminism, listed in `oracle/README.md`.

## Global Constraints

- Reference: Doris `4.1.5`, suites from `4.1.5-release` (`oracle/versions.env`); nothing pins another version.
- Harness code is standard-library Python only; no `pip install`. Containers used: the reference image, the existing runner image, `nicolaka/netshoot:v0.16` (capture) and `python:3.11-slim` (tap).
- Relaxations come only from variation the reference itself shows (probe marks), from declared deployment facts, and from declared harness traffic (`oracle/background_traffic.json`, each entry with its source). No hand-written masks, ever.
- Single profile only: one FE, one BE.
- `parallel_pipeline_task_num` is pinned to `4` globally on every cluster before any suite runs.
- The repo is public: no credentials, tokens or host-specific paths in committed files.
- Every commit ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Python tests live in `oracle/tests/test_*.py`, import their module by inserting its `bin/` directory into `sys.path` (as `test_compare.py` does), and run with `make -C oracle test`.
- Suite keys everywhere use the format `results.json` already uses: directory path with `/` → `.`, then file stem, then suite name (`nereids_p0.join.load.load`).

## Review Focus

1. **Polling loops** (`waitForSchemaChange`, `SHOW ALTER TABLE` until `FINISHED`) send the same statement a varying number of times. Expected: consecutive identical statements on one connection collapse to the last one, so polling never shows up as a difference. Test in Task 7 (`test_consecutive_identical_statements_collapse_to_the_last`).
2. **TLS:** Connector/J tries TLS when the server offers `CLIENT_SSL`, and an encrypted session can't be recorded. Expected: the tap clears `CLIENT_SSL` in the server greeting, and the session stays plain. Test in Task 6 (`test_tap_clears_client_ssl_in_greeting`).
3. **`load` suites:** other suites in the same directory depend on them. Expected: a suite's needs include its directory's `load` suites' needs, so a blocked load shows up as blocking its dependents. Test in Task 5 (`test_suite_needs_include_directory_load_suites`).
4. **Traffic outside any suite** (heartbeats, reports, `publish_topic_info`, and messages between suites). Expected: it goes to `background`, never to a suite, and never counts towards "reaches the BE". Test in Task 4 (`test_background_methods_and_gaps_are_not_attributed`).
5. **Large and unusual MySQL replies:** packets of 16 MiB or more split across frames, `LOAD DATA LOCAL INFILE` (`allowLoadLocalInfile=true` is in the JDBC URL), and multi-result replies. Expected: all of them decode. Tests in Task 6 (`test_large_packet_is_joined`, `test_local_infile_exchange`, `test_more_results_exists`).

---

## File Structure

```
oracle/
  bin/scope.py                 static scope rules → suites/epic1.scope.json, suites/epic1.txt
  bin/parse_teamcity.py        (modify) add started_ms/finished_ms per suite
  bin/cluster_setup.sh         pins parallel_pipeline_task_num on a running cluster
  bin/run_suites.sh            (modify) PROXY=1 and CAPTURE=1 switches, serial mode
  bin/epic_baseline.sh         the four reference runs and everything derived from them
  bin/ratchet.py               ratchet gate and the two scores
  compose/reference-split.yml  cluster B: FE and BE in separate containers, other facts
  compose/tap.yml              MySQL tap sidecar in the FE container's network namespace
  compose/capture.yml          (modify) also port 8040, generic target container
  clusters/single.facts.json   deployment facts of cluster A
  clusters/split.facts.json    deployment facts of cluster B
  targets/reference-split.env  where cluster B listens
  targets/*.env                (modify) FE_CONTAINER for the tap
  replies/bin/tap.py           TCP tap: forwards, records chunks with timestamps
  replies/bin/mysql_wire.py    packets, handshake, commands, replies → canonical form
  replies/bin/sessions.py      tap logs → per-suite exchange lists
  replies/bin/marks.py         facts substitution, probe marks, strict comparison
  replies/reference/           committed: <suite>.jsonl.gz canonical replies, marks.json
  traffic/bin/extract_suites.py  pcap → per-suite records (Thrift, gRPC, HTTP stream load)
  traffic/corpus/              committed: <suite>.bin.gz, manifest.json, reaches_be.txt
  needs/index.json             committed needs index
  needs/supported.json         features doris-rs supports (empty in M1)
  needs/bin/rank.py            ranking
  scores/epic1-out.txt, scores/epic1-strict.txt   ratchets (empty in M1)
  suites/epic1.txt             directories to run; suites/epic1.suites.txt suite keys
crates/doris-wire/
  src/suites.rs                reads traffic/corpus records
  src/bin/doris-needs.rs       corpus → needs/index.json
  tests/suite_roundtrip.rs     every per-suite message re-encodes byte for byte
```

---

### Task 1: Static scope and suite keys

**Files:**
- Create: `oracle/bin/scope.py`, `oracle/tests/test_scope.py`, `oracle/tests/fixtures/scope/` (small `.groovy`/`.sql` fixtures)
- Create (output, committed): `oracle/suites/epic1.scope.json`, `oracle/suites/epic1.txt`

**Interfaces:**
- Produces: `suite_key(groovy_path: str, root: str) -> str`; `exclusion_reasons(text: str) -> list[str]`; `scan(suites_root: str, dirs: list[str]) -> dict[str, dict]` mapping key → `{"file": rel_path, "reasons": [...]}` (empty reasons = in scope); CLI `scope.py SRC_SUITES_ROOT p0-core.txt OUT_JSON OUT_DIRS`.

Rule names (the strings in `reasons`): `variant`, `text_index`, `agg_types`, `aggregate_key`, `complex_types`, `arrow_flight`, `ai_functions`, `vector`, `external_catalog`, `debug_points`. The text scanned for a suite is its `.groovy` file plus every `.sql` file in the same directory and its subdirectories. `epic1.txt` lists the `p0-core` directories with at least one in-scope suite.

- [ ] **Step 1: Write the failing tests**

```python
def test_suite_key_matches_results_format(self):
    # file nereids_p0/join/load.groovy containing suite("load")
    self.assertEqual(scope.suite_key(f"{ROOT}/nereids_p0/join/load.groovy", ROOT), "nereids_p0.join.load.load")

def test_rules(self):
    cases = {
        "k1 VARIANT NULL": ["variant"],
        "INDEX idx (c) USING INVERTED": ["text_index"],
        "INDEX idx (c) USING NGRAM_BF": ["text_index"],
        "where c MATCH_ANY 'x'": ["text_index"],
        "v BITMAP BITMAP_UNION": ["agg_types"],
        "select to_bitmap(1)": ["agg_types"],
        "h HLL HLL_UNION": ["agg_types"],
        "AGGREGATE KEY(k1)": ["aggregate_key"],
        "c ARRAY<INT>": ["complex_types"],
        "c MAP<STRING, INT>": ["complex_types"],
        "c STRUCT<a:INT>": ["complex_types"],
        "j JSON NULL": ["complex_types"],
        "j JSONB": ["complex_types"],
        "arrow_flight_sql \"select 1\"": ["arrow_flight"],
        "select AI_GENERATE('x')": ["ai_functions"],
        "INDEX v (emb) USING ANN": ["vector"],
        "select l2_distance(a, b)": ["vector"],
        "CREATE CATALOG hive PROPERTIES": ["external_catalog"],
        "GetDebugPoint().enableDebugPointForAllBEs('x')": ["debug_points"],
    }
    for text, want in cases.items():
        self.assertEqual(scope.exclusion_reasons(text), want, text)

def test_no_false_positives(self):
    for text in ["create table test_variant_like (k INT)", "\"format\" = \"json\"",
                 "select bitmap_count_x from t", "c VARCHAR(10) -- array of things",
                 "DUPLICATE KEY(k1)", "UNIQUE KEY(k1)", "select json_extract(s, '$.a')"]:
        self.assertEqual(scope.exclusion_reasons(text), [], text)

def test_sql_files_next_to_the_suite_count(self):
    # fixture dir: d/suite.groovy (clean) + d/ddl/t.sql (AGGREGATE KEY)
    result = scope.scan(FIXTURES, ["d"])
    self.assertEqual(result["d.suite.suite"]["reasons"], ["aggregate_key"])
```

- [ ] **Step 2: Run** `make -C oracle test` → expect FAIL (`No module named 'scope'`).

- [ ] **Step 3: Implement `oracle/bin/scope.py`.** One case-insensitive regex per rule. A suite file is any `*.groovy` that calls `suite("…")`; the key uses the first `suite("name")` in the file. `reasons` are sorted and unique.

- [ ] **Step 4: Run** `make -C oracle test` → PASS.

- [ ] **Step 5: Generate the outputs** with `python3 oracle/bin/scope.py .cache/doris-src/regression-test/suites oracle/suites/p0-core.txt oracle/suites/epic1.scope.json oracle/suites/epic1.txt`. Expected: the JSON lists every `p0-core` suite, `arrow_flight_sql_p0` has no in-scope suite, and the script prints the in-scope count and the count per reason. Read 10 excluded suites per reason and check that each reason is real. If one is a false positive, add it to `test_no_false_positives` and fix the rule.

- [ ] **Step 6: Commit**

```bash
git add oracle/bin/scope.py oracle/tests/test_scope.py oracle/tests/fixtures/scope oracle/suites/epic1.scope.json oracle/suites/epic1.txt
git commit -m "oracle: static scope rules pick Epic 1's p0-core suites"
```

---

### Task 2: Suite time windows in results

**Files:**
- Modify: `oracle/bin/parse_teamcity.py`, `oracle/tests/test_parse_teamcity.py`

**Interfaces:**
- Produces: each suite entry in `results.json` gains `started_ms: int` and `finished_ms: int` (UTC epoch milliseconds, from the `timestamp` attribute of `testStarted`/`testFinished`). Task 7 consumes them.

- [ ] **Step 1: Write the failing test**

```python
def test_window_from_timestamps(self):
    lines = [
        "##teamcity[testStarted name='a.b.c' flowId='f' timestamp='2026-11-02T06:11:48.136+0000']",
        "##teamcity[testFinished name='a.b.c' flowId='f' duration='170' timestamp='2026-11-02T06:11:48.305+0000']",
    ]
    r = parse_teamcity.parse_lines(lines)["a.b.c"]
    self.assertEqual(r["started_ms"], 1793599908136)
    self.assertEqual(r["finished_ms"], 1793599908305)
```

- [ ] **Step 2: Run** `make -C oracle test` → FAIL (`KeyError: 'started_ms'`).
- [ ] **Step 3: Implement it.** Parse the timestamp with `datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f%z")`. Leave the existing fields unchanged.
- [ ] **Step 4: Run** `make -C oracle test` → PASS.
- [ ] **Step 5: Commit** `oracle: results.json records each suite's start and finish time`.

---

### Task 3: Cluster B, deployment facts, cluster setup

**Files:**
- Create: `oracle/compose/reference-split.yml`, `oracle/targets/reference-split.env`, `oracle/clusters/single.facts.json`, `oracle/clusters/split.facts.json`, `oracle/bin/cluster_setup.sh`
- Modify: `oracle/targets/reference-single.env` (add `FE_CONTAINER=doris-oracle-doris-1`), `oracle/Makefile` (`PROFILE=split` works with `up`/`down`; `up` runs `cluster_setup.sh` after `--wait`)

**Interfaces:**
- Produces: `PROFILE=split`, a compose project `doris-oracle-split` with services `fe` and `be` on network `doris-oracle`; target `reference-split` with `FE_HOST=fe`, `FE_CONTAINER=doris-oracle-split-fe-1`, `TARGET_NETWORK=doris-oracle`. Facts file format, consumed by Task 8:

```json
{"cluster": "split",
 "facts": [
   {"name": "be-host", "values": ["172.x.y.z"], "match": "substring"},
   {"name": "be-port", "values": ["9160"], "match": "cell"},
   {"name": "be-heartbeat-port", "values": ["9150"], "match": "cell"},
   {"name": "be-http-port", "values": ["8140"], "match": "cell"},
   {"name": "be-brpc-port", "values": ["8160"], "match": "cell"},
   {"name": "be-cores", "values": ["2"], "match": "cell"},
   {"name": "be-mem-limit", "values": ["..."], "match": "cell"},
   {"name": "be-data-path", "values": ["/opt/doris-alt-storage"], "match": "substring"}]}
```

Cluster B uses the same image and settings as A (`extends` the `doris` service of `reference-single.yml`, so `FE_CONFIG_EXTRA`/`BE_CONFIG_EXTRA` stay identical). The BE differs on purpose:
- `DORIS_ROLE=be`, with the address set by the container IP (pinned with `ipv4_address` so it is stable across runs);
- `BE_HEARTBEAT_PORT=9150` and `BE_HTTP_PORT=8140`, with `be_port = 9160`, `webserver_port = 8140`, `heartbeat_service_port = 9150`, `brpc_port = 8160` and `storage_root_path = /opt/doris-alt-storage` appended to `BE_CONFIG_EXTRA`;
- `BE_MEM_LIMIT=30%` (A uses 40%);
- `cpuset: "0-1"`.

`cluster_setup.sh FE_CONTAINER` runs `SET GLOBAL parallel_pipeline_task_num = 4` through `docker exec … mysql`, then prints `SHOW BACKENDS`.

- [ ] **Step 1: Bring up A** (`make -C oracle up`) and B (`make -C oracle down; make -C oracle up PROFILE=split`). Expected: both become healthy, and `SHOW VARIABLES LIKE 'parallel_pipeline_task_num'` shows `4` on each.
- [ ] **Step 2: Fill the facts files from `SHOW BACKENDS`** on each cluster: address, the four ports, `CpuCores`, the memory limit as rendered, and the data path. Every B value must differ from A's. If cores or memory can't be made to differ, write that in `split.facts.json` under `"not_varied": [...]` with the reason. Don't hide it.
- [ ] **Step 3: Run the smoke suites against B:** `bin/run_suites.sh reference-split suites/smoke.txt`. Expected: the same pass count as `baselines/4.1.5-single-smoke.json` (180 passed). Stream load must work over the BE's container address.
- [ ] **Step 4: Commit** `oracle: split reference cluster with different deployment facts, and pinned fragment parallelism`.

---

### Task 4: Per-suite traffic corpus

**Files:**
- Create: `oracle/traffic/bin/extract_suites.py`, `oracle/tests/test_traffic_extract.py`
- Modify: `oracle/wire/bin/extract.py` (gRPC extraction from `tshark -T fields` instead of `-T json`, so memory stays bounded; every message gains `time_ms`); `oracle/compose/capture.yml` (`network_mode: container:${CAPTURE_TARGET}`, filter adds `tcp port 8040`)

**Interfaces:**
- Consumes: Task 2's windows (`results.json`); the `extract.py` functions `syn_streams`, `segments`, `thrift_messages`, `grpc_messages` (all keep their names, and messages gain `time_ms`).
- Produces:
  - `http_stream_loads(segs, streams) -> list[dict]`: one record per HTTP/1.1 exchange on port 8040. It holds `method`, `path`, `headers` (list of pairs), `body`, `status` and `response_body`. Content-Length and chunked bodies are both supported.
  - `attribute(messages, windows) -> dict[str, list]`: maps a suite key or `"background"` to that suite's messages in capture order.
  - `BACKGROUND_METHODS = {"heartbeat", "report", "publish_topic_info", "get_dictionary_status"}`.
  - The corpus file `traffic/corpus/<suite>.bin.gz`, a sequence of records. Each record is a little-endian `u32` header length, a header JSON (`protocol`, `service`, `method`, `kind`, `time_ms`, plus `tcp_stream`/`seqid`/`h2_stream`/`compressed` as in the wire manifest), then a `u32` data length and the data. For HTTP, the data is the raw request or response bytes.
  - `traffic/corpus/manifest.json` (reference identity, pcap sha256, per-suite message counts and sha256 of the file).
  - `traffic/corpus/reaches_be.txt`: suites with at least one non-background `exec_plan_fragment*`, `submit_tasks`, tablet-writer, or HTTP stream-load message.

- [ ] **Step 1: Write the failing tests**

```python
def test_background_methods_and_gaps_are_not_attributed(self):
    windows = {"s1": (100, 200), "s2": (300, 400)}
    msgs = [m("exec_plan_fragment", 150), m("heartbeat", 160), m("submit_tasks", 250), m("fetch_data", 350)]
    out = extract_suites.attribute(msgs, windows)
    self.assertEqual([x["method"] for x in out["s1"]], ["exec_plan_fragment"])
    self.assertEqual([x["method"] for x in out["s2"]], ["fetch_data"])
    self.assertEqual(sorted(x["method"] for x in out["background"]), ["heartbeat", "submit_tasks"])

def test_overlapping_windows_are_an_error(self):
    with self.assertRaises(ValueError):
        extract_suites.attribute([], {"a": (0, 10), "b": (5, 20)})

def test_http_content_length_and_chunked(self):
    # two synthetic exchanges on one stream: PUT with Content-Length, PUT with chunked body
    loads = extract_suites.http_stream_loads(SEGS, STREAMS)
    self.assertEqual([l["body"] for l in loads], [b"1,a\n", b"2,b\n"])
    self.assertEqual(loads[0]["status"], 200)

def test_corpus_file_roundtrip(self):
    path = os.path.join(self.tmp, "s.bin.gz")
    extract_suites.write_records(path, [({"method": "x"}, b"\x00\x01")])
    self.assertEqual(list(extract_suites.read_records(path)), [({"method": "x"}, b"\x00\x01")])
```

- [ ] **Step 2: Run** `make -C oracle test` → FAIL.
- [ ] **Step 3: Implement it.** `extract_suites.py PCAP RESULTS_JSON SUITE_KEYS OUT_DIR` writes the corpus for the suite keys in `SUITE_KEYS`, and only those. Windows must not overlap, because the run is serial. Change `grpc_messages` to read `frame.number, frame.time_epoch, tcp.stream, tcp.srcport, http2.streamid, http2.type, http2.data.data, http2.request.full_uri` as fields. Multi-valued fields are comma-separated, one entry per HTTP/2 frame in the packet.
- [ ] **Step 4: Run** `make -C oracle test` → PASS.
- [ ] **Step 5: Regression check on the old capture.** Run `make -C oracle wire-extract PCAP=wire/captures/20261102-085436.pcap`, then `git status oracle/wire/corpus`. Expected: the only change in `manifest.json` is the added `time_ms` field (check with `git diff --stat`, then `git checkout oracle/wire/corpus` if you prefer to keep the manifest unchanged); the message files are unchanged. Then run `cargo test -p doris-wire`, which should pass.
- [ ] **Step 6: Commit** `oracle: per-suite FE<->BE traffic corpus, with stream-load HTTP and suite attribution`.

---

### Task 5: Rust round-trip over the per-suite corpus and the needs index

**Files:**
- Create: `crates/doris-wire/src/suites.rs`, `crates/doris-wire/tests/suite_roundtrip.rs`, `crates/doris-wire/src/bin/doris-needs.rs`, `oracle/needs/bin/rank.py`, `oracle/tests/test_needs_rank.py`, `oracle/needs/supported.json` (`{"features": []}`)
- Modify: `crates/doris-wire/Cargo.toml`. Add `flate2` as a workspace dependency, and move `doris-thrift`, `doris-proto`, `prost` and `thrift` to `[dependencies]` because the binary uses them. Also move the round-trip helpers that `tests/thrift_roundtrip.rs` and `tests/grpc_roundtrip.rs` share into `src/roundtrip.rs`, so the new test reuses them.

**Interfaces:**
- Produces (Rust):
  - `doris_wire::suites::SuiteRecord { header: serde_json::Value, data: Vec<u8> }` and `doris_wire::suites::read_suite(path: &Path) -> io::Result<Vec<SuiteRecord>>`.
  - `doris_wire::suites::corpus_dir() -> PathBuf`, which is `oracle/traffic/corpus`.
  - `doris_wire::roundtrip::{thrift(bytes) -> Result<(), String>, grpc(method, kind, bytes) -> Result<(), String>, plan(bytes) -> Result<(), String>}`. Each returns `Err` with the first differing offset.
- Produces (needs index): `doris-needs CORPUS_DIR > oracle/needs/index.json`, shaped `{suite: {"rpc": [..], "task": [..], "plan_node": [..], "sink": [..], "expr": [..], "function": [..], "type": [..]}}`. Values are sorted, unique, and spelled as the Thrift enum names (`OLAP_SCAN_NODE`, `RESULT_SINK`, `FUNCTION_CALL`, `INT`, `CREATE`…). Function names come from `TFunctionName.function_name`, in lowercase. Sources:
  - `rpc`: `service.method` for every message;
  - `task`: `TAgentTaskRequest.task_type` in `submit_tasks`;
  - the other keys: `TPipelineFragmentParamsList` in `exec_plan_fragment*` (fragment plan nodes, output sink type, expression node types, function names, slot and expression scalar types).
- Produces (Python): `rank(index: dict, supported: set[str], suites: list[str], dir_loads: dict[str, list[str]]) -> list[dict]`. Each entry is `{"feature": "plan_node:HASH_JOIN_NODE", "alone": n, "total": m}`, sorted by `alone` descending, then `total` descending, then name. Features are spelled `"<key>:<value>"`. The CLI is `rank.py INDEX SUPPORTED SUITE_KEYS [--top N]`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn every_suite_message_reencodes_identically() {
    // Skips with a printed note when oracle/traffic/corpus has no .bin.gz yet (before Task 10).
    for each suite file, for each record:
      thrift → roundtrip::thrift, grpc → roundtrip::grpc (and plan for exec_plan_fragment*), http → skipped
      collect failures; Thrift set-order differences pass only via doris_wire::canonical, as today
    assert!(failures.is_empty(), "{failures:#?}");
}
```

```python
def test_alone_counts_only_single_blockers(self):
    index = {"s1": {"plan_node": ["A"]}, "s2": {"plan_node": ["A", "B"]}, "s3": {"plan_node": ["B"]}}
    r = needs_rank.rank(index, set(), ["s1", "s2", "s3"], {})
    self.assertEqual(r[0], {"feature": "plan_node:A", "alone": 1, "total": 2})

def test_supported_features_do_not_block(self):
    r = needs_rank.rank({"s1": {"plan_node": ["A"]}}, {"plan_node:A"}, ["s1"], {})
    self.assertEqual(r, [])

def test_suite_needs_include_directory_load_suites(self):
    index = {"d.load.load": {"task": ["CREATE"]}, "d.q.q": {"plan_node": ["A"]}}
    r = needs_rank.rank(index, {"plan_node:A"}, ["d.q.q"], {"d.q.q": ["d.load.load"]})
    self.assertEqual(r, [{"feature": "task:CREATE", "alone": 1, "total": 1}])
```

- [ ] **Step 2: Run** `cargo test -p doris-wire` and `make -C oracle test` → FAIL (missing module and function).
- [ ] **Step 3: Implement it.** First move the existing helpers into `src/roundtrip.rs` with no behavior change, and keep the existing tests green. Then add `suites.rs`, the binary and `rank.py`. `dir_loads` is built by the CLI: for each suite, the `load` suites (suite name `load`) whose key shares its directory prefix.
- [ ] **Step 4: Run** `cargo test -p doris-wire` and `make -C oracle test` → PASS.
- [ ] **Step 5: Commit** `doris-wire: per-suite corpus round-trip and the needs index; oracle: needs ranking`.

---

### Task 6: MySQL tap and reply decoding

**Files:**
- Create: `oracle/replies/bin/tap.py`, `oracle/replies/bin/mysql_wire.py`, `oracle/tests/test_tap.py`, `oracle/tests/test_mysql_wire.py`, `oracle/compose/tap.yml`

**Interfaces:**
- `tap.py --listen 0.0.0.0:9031 --upstream 127.0.0.1:9030 --out DIR` accepts connections and forwards bytes both ways. For each connection it writes `DIR/<conn_id>.jsonl`, one line per chunk: `{"t_ms": int, "dir": "c2s"|"s2c", "hex": str}`. In the first server packet (the greeting), it clears capability bit `CLIENT_SSL` (`0x0800`) in the lower capability flags before forwarding, and records the bytes as forwarded. It stops on SIGTERM and flushes every file.
- `compose/tap.yml` runs `python:3.11-slim` with `network_mode: container:${FE_CONTAINER}`, mounts `oracle/replies/bin` read-only and an output directory, and runs `tap.py`.
- `mysql_wire.py` produces:
  - `packets(stream: bytes) -> list[tuple[int, bytes]]`: `(sequence_id, payload)`. Payloads of 16 MiB or more that span several frames are joined.
  - `decode_session(c2s: bytes, s2c: bytes) -> list[Exchange]`.
  - `Exchange = {"command": str, "statement": str|None, "params": list|None, "reply": Reply}`.
  - Commands handled: `COM_QUERY`, `COM_STMT_PREPARE`, `COM_STMT_EXECUTE` (the statement is the prepared text; parameters are decoded), `COM_STMT_CLOSE` and `COM_STMT_SEND_LONG_DATA` (no reply), `COM_STMT_RESET`, `COM_PING`, `COM_INIT_DB`, `COM_QUIT`, `COM_SET_OPTION`, `COM_FIELD_LIST`. The handshake (greeting and login result) is the first exchange, with command `"handshake"`.
  - `Reply` in the spec's canonical form (§4.5) is one of:
    - `{"type": "ok", "affected_rows", "last_insert_id", "status", "warnings", "info"}`;
    - `{"type": "err", "code", "sqlstate", "message"}`;
    - `{"type": "resultset", "columns": [{catalog, schema, table, org_table, name, org_name, charset, length, col_type, flags, decimals}], "rows": [[hex|None, ...]], "status", "warnings"}`;
    - `{"type": "prepare_ok", "statement_id": None, "columns", "params"}` (the statement id is dropped because it varies);
    - `{"type": "local_infile", "file": str, "then": Reply}`;
    - `{"type": "multi", "parts": [Reply, ...]}` when `SERVER_MORE_RESULTS_EXISTS` is set.
  - Binary-protocol rows are decoded with the column types into the same row form: each value is the hex of its text rendering, as the server would send it in the text protocol. That way one comparison serves both protocols. Whether `CLIENT_DEPRECATE_EOF` applies is read from the negotiated capabilities.

- [ ] **Step 1: Write the failing tests** (packet bytes are built by small helpers in the test file)

```python
def test_tap_clears_client_ssl_in_greeting(self):
    # fake server sends a greeting with CLIENT_SSL set; client side reads through the tap
    self.assertEqual(caps_seen_by_client & 0x0800, 0)
    self.assertEqual(recorded_s2c_bytes, bytes_seen_by_client)

def test_tap_forwards_and_records_both_directions(self): ...  # echo server, 3 round trips, jsonl has 6 chunks in order

def test_query_resultset_text(self):
    ex = mysql_wire.decode_session(C2S_QUERY, S2C_RESULTSET)[1]
    self.assertEqual(ex["statement"], "select 1, null")
    self.assertEqual(ex["reply"]["rows"], [["31", None]])
    self.assertEqual(ex["reply"]["columns"][0]["name"], "1")

def test_err_packet(self):
    r = mysql_wire.decode_session(C2S_QUERY, S2C_ERR)[1]["reply"]
    self.assertEqual((r["code"], r["sqlstate"], r["message"]), (1105, "HY000", "errCode = 2, detailMessage = x"))

def test_deprecate_eof_and_classic_eof_give_the_same_reply(self): ...
def test_large_packet_is_joined(self):      # payload of 0xFFFFFF + 10 bytes in two frames
def test_local_infile_exchange(self):       # 0xFB request, client file packets, empty packet, OK
def test_more_results_exists(self):         # two result sets then OK → type "multi" with 3 parts
def test_prepare_execute_binary_rows_match_text_form(self):  # INT, DECIMAL, DATETIME(3), VARCHAR, NULL
def test_truncated_stream_is_an_error_not_a_hang(self):
```

- [ ] **Step 2: Run** `make -C oracle test` → FAIL.
- [ ] **Step 3: Implement `tap.py`** with `asyncio`: one pair of copy tasks per connection, with timestamps from `time.time_ns() // 1_000_000`. **Implement `mysql_wire.py`** as a reader that walks the client packets and consumes the matching server packets for each command.
- [ ] **Step 4: Run** `make -C oracle test` → PASS.
- [ ] **Step 5: Live check.** With cluster A up, start the tap through `compose/tap.yml` (`FE_CONTAINER=doris-oracle-doris-1`), then run `docker exec doris-oracle-doris-1 mysql -uroot -h127.0.0.1 -P9031 -e "select 1; show backends"`. Expected: normal output. Then decoding the recorded connection gives a handshake, two exchanges, and the `SHOW BACKENDS` result set with the A facts.
- [ ] **Step 6: Commit** `oracle: MySQL tap and canonical reply decoding (text and binary protocol)`.

---

### Task 7: Linking replies to suites

**Files:**
- Create: `oracle/replies/bin/sessions.py`, `oracle/tests/test_sessions.py`

**Interfaces:**
- Consumes: `mysql_wire.decode_session`; Task 2's windows.
- Produces:
  - `suite_exchanges(tap_dir: str, windows: dict[str, tuple[int, int]]) -> dict[str, list[dict]]`. Every exchange gets the `t_ms` of its first client chunk and its connection id. An exchange belongs to the suite whose window contains `t_ms`; the rest go to `"unattributed"`. Within a suite, exchanges are ordered by `t_ms`.
  - `collapse(exchanges) -> list`: on one connection, a run of consecutive exchanges with the same command, statement and params collapses to its last exchange. Each item gains `conn` (the connection's index in order of first appearance within the suite), so connection ids never matter.
  - `write_suite(path, exchanges)` and `read_suite(path)` handle `<suite>.jsonl.gz`.

- [ ] **Step 1: Write the failing tests**

```python
def test_consecutive_identical_statements_collapse_to_the_last(self):
    ex = [q("SHOW ALTER TABLE COLUMN", "RUNNING"), q("SHOW ALTER TABLE COLUMN", "RUNNING"),
          q("SHOW ALTER TABLE COLUMN", "FINISHED"), q("select 1", "1")]
    out = sessions.collapse(ex)
    self.assertEqual([e["reply"]["rows"] for e in out], [[["FINISHED"]], [["1"]]])

def test_identical_statements_on_different_connections_do_not_collapse(self): ...
def test_connection_ids_become_first_appearance_order(self): ...
def test_exchanges_outside_windows_are_unattributed(self): ...
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement it.** **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `oracle: link recorded MySQL exchanges to suites, collapsing polls`.

---

### Task 8: Deployment facts, probe marks, strict comparison

**Files:**
- Create: `oracle/replies/bin/marks.py`, `oracle/tests/test_marks.py`

**Interfaces:**
- Consumes: facts files (Task 3), per-suite exchange lists (Task 7).
- Produces:
  - `substitute(exchange, facts: dict) -> dict`. A `cell` fact replaces a whole row value or column-definition field equal to one of its values with `<name>`. A `substring` fact replaces every occurrence inside row values, error messages, `info` and statement text. Rows are compared as hex, so matching works on the decoded bytes, and the result is re-encoded.
  - `derive_marks(runs: list[list[dict]]) -> dict` takes one suite's exchanges from each reference run (run 0 is the anchor, after substitution). It returns `{"status": "stable"|"unstable", "reason": str|None, "exchanges": {i: mark}}`. A mark is any of `{"unordered": true}`, `{"masked_columns": [j...]}`, `{"masked_column_meta": [[j, field]...]}`, `{"message_pattern": regex}`, `{"statement_pattern": regex}`, `{"masked_fields": ["affected_rows"|"last_insert_id"|"info"|"warnings"]}`. Rules:
    - the runs have different exchange counts after collapsing, or different reply types at the same index → the suite is `unstable`;
    - the rows are equal as a multiset but not in order → `unordered`;
    - a column's values differ → that column is masked;
    - error messages differ → their differing spans, taken from a `difflib.SequenceMatcher` over the messages, become `.*?` in an escaped regex;
    - statement texts differ (for example generated labels) → `statement_pattern`, built the same way;
    - other scalar fields differ → `masked_fields`.
  - `compare(expected: list, marks: dict, actual: list) -> dict | None`. It returns `None` when the replies are equal under the marks. Otherwise it returns the first difference as `{"index", "statement", "field", "expected", "actual"}`. Unstable suites are never compared; the caller reports them separately.
  - The CLI is `marks.py derive REF_RUN_DIRS... --facts F... --suites KEYS --out replies/reference/` and `marks.py compare REF_DIR CAND_DIR --facts CAND_FACTS --suites KEYS --json OUT`.

- [ ] **Step 1: Write the failing tests**

```python
def test_cell_fact_and_substring_fact(self):
    facts = {"facts": [{"name": "be-port", "values": ["9050"], "match": "cell"},
                       {"name": "be-host", "values": ["127.0.0.1"], "match": "substring"}]}
    ex = rs([["127.0.0.1", "9050", "19050"]])
    self.assertEqual(rows(marks.substitute(ex, facts)), [["<be-host>", "<be-port>", "19050"]])

def test_error_prefix_address_is_substituted(self):  # "(127.0.0.1)[E-1]x" → "(<be-host>)[E-1]x"
def test_row_order_variation_marks_unordered(self):
def test_value_variation_masks_only_that_column(self):
def test_error_message_variation_becomes_a_pattern(self):  # "query id 1a2b" vs "query id 9f8e"
def test_count_mismatch_marks_suite_unstable(self):
def test_compare_reports_first_difference(self):
    d = marks.compare([rs([["1"]])], {"status": "stable", "exchanges": {}}, [rs([["2"]])])
    self.assertEqual((d["index"], d["field"]), (0, "rows"))
def test_compare_applies_masks_before_multiset(self):
def test_column_meta_difference_is_a_difference(self):  # same rows, decimals 2 vs 3
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement it.** **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `oracle: deployment-fact substitution, probe marks and strict reply comparison`.

---

### Task 9: Running with tap and capture; ratchets

**Files:**
- Modify: `oracle/bin/run_suites.sh`. With `PROXY=1`, it starts `compose/tap.yml` against `$FE_CONTAINER` before the framework, points `jdbcUrl`/`targetJdbcUrl` at port `9031`, stops the tap afterwards, and moves the logs to `$OUT/tap/`. With `CAPTURE=1`, it starts `compose/capture.yml` with `CAPTURE_TARGET=$BE_CONTAINER` and writes `$OUT/traffic.pcap`. `SERIAL=1` passes `-parallel 1 -suiteParallel 1 -actionParallel 1`. Every switch it used is recorded in `meta.json`.
- Modify: `oracle/targets/reference-split.env` (`BE_CONTAINER=doris-oracle-split-be-1`), `oracle/targets/reference-single.env` (`BE_CONTAINER=doris-oracle-doris-1`)
- Create: `oracle/bin/ratchet.py`, `oracle/tests/test_ratchet.py`, `oracle/scores/epic1-out.txt`, `oracle/scores/epic1-strict.txt` (both start with a comment line and no suites)

**Interfaces:**
- `ratchet.py --results R --strict STRICT_JSON --suites KEYS --reaches-be FILE --scores-dir oracle/scores` prints, for each gate, `passing X of Y epic suites; Z of W that reach the BE`. It exits 1 if a suite listed in a scores file is not passing at that gate, printing each one. It prints, without failing, the suites that now pass and are not listed yet. A suite passes the strict gate only if it also passed its `.out` check.

- [ ] **Step 1: Write the failing tests**

```python
def test_listed_suite_failing_fails_the_gate(self): ...
def test_new_passes_are_reported_not_failed(self): ...
def test_strict_requires_out_pass(self): ...
def test_two_numbers(self):
    out = ratchet.summary({"a": "passed", "b": "passed", "c": "failed"}, ["a", "b", "c"], {"b", "c"})
    self.assertEqual(out, {"passing": 2, "total": 3, "passing_be": 1, "total_be": 2})
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement it.** **Step 4: Run** → PASS.
- [ ] **Step 5: Live check on the smoke list.** Run `PROXY=1 CAPTURE=1 SERIAL=1 bin/run_suites.sh reference-single suites/smoke.txt`. Expected: the smoke pass count equals the baseline's; `$OUT/tap/` holds one `.jsonl` per connection; `$OUT/traffic.pcap` exists; and `sessions.py` attributes exchanges to the smoke suites with no overlapping windows.
- [ ] **Step 6: Commit** `oracle: suite runs behind the MySQL tap and packet capture, and ratchet scores`.

---

### Task 10: The four reference runs and their products

**Files:**
- Create: `oracle/bin/epic_baseline.sh`
- Modify: `oracle/Makefile`. Add `epic-baseline` and `epic-check TARGET=… FACTS=…`, and list both in the help header.
- Create (outputs, committed):
  - `oracle/suites/epic1.suites.txt`;
  - additions to `oracle/suites/ignore.txt`;
  - `oracle/baselines/4.1.5-single-epic1.json`;
  - `oracle/replies/reference/<suite>.jsonl.gz` and `marks.json`;
  - `oracle/traffic/corpus/`;
  - `oracle/needs/index.json` and `oracle/needs/ranking.txt`.

**What `epic_baseline.sh` does, in order:**
1. A fresh cluster A (`down`, `up`), then run 1 with `PROXY=1 CAPTURE=1 SERIAL=1`, then run 2 with `PROXY=1 SERIAL=1`.
2. A fresh cluster B, then runs 3 and 4 with `PROXY=1 SERIAL=1`. Each run uses `suites/epic1.txt`.
3. `epic1.suites.txt` lists the suites that are in scope (`epic1.scope.json`) and passed all four runs. In-scope suites that passed some runs but not all are appended to `ignore.txt`, each with `# flaky on reference: runs <ids>`.
4. `baselines/4.1.5-single-epic1.json` takes run 1's entries for those suites.
5. `marks.py derive` over the four runs' tap logs, linked by `sessions.py`, writes `replies/reference/`. Run 1's exchanges are the expected replies.
6. `extract_suites.py` on run 1's pcap writes `traffic/corpus/`.
7. `doris-needs` writes `needs/index.json`, and `rank.py --top 40` writes `needs/ranking.txt`.

**What `epic-check` does:**
1. Run the target with `PROXY=1 SERIAL=1`.
2. `compare.py` against the epic baseline. This is a report only; its exit status is ignored.
3. `marks.py compare` writes `results/<target>/latest/strict.json`.
4. `ratchet.py` is the gate.

- [ ] **Step 1: Write `epic_baseline.sh` and the Makefile targets.** Before running for real, dry-run with `SUITES_OVERRIDE=suites/smoke.txt`. Expected: every step above finishes on the smoke list in under 30 minutes, and every output file exists.
- [ ] **Step 2: Run `make -C oracle epic-baseline` in the background.** Expected: about 10 hours (each serial run takes about 2.5 h; the `p0-core` baseline took 2 h 16 min). The script logs each phase with a timestamp to `results/epic-baseline.log`.
- [ ] **Step 3: Check the size before committing.** Run `du -sh oracle/replies/reference oracle/traffic/corpus`. **If the total is over 100 MB, stop and ask the user** how to store it: Git LFS, a release asset fetched by `make`, or committing only marks and digests. Don't commit until they have chosen.
- [ ] **Step 4: Run `cargo test -p doris-wire`** → PASS. `every_suite_message_reencodes_identically` now runs over the real corpus. A message that fails to re-encode is a codec bug: fix it in `doris-thrift`/`doris-wire`, as the wire corpus work did, and never skip it.
- [ ] **Step 5: Commit** `oracle: Epic 1 reference baseline, reply marks, traffic corpus and needs index`.

---

### Task 11: Self-check and documentation

**Files:**
- Modify: `oracle/README.md` (a section "Epic 1 checker": the pieces, the runs, `make epic-check`, the two gates, the ratchets, and the coverage summary: in-scope count, exclusions per reason, flaky count, unstable-for-strict count, the reaches-the-BE count, and the first 15 lines of `needs/ranking.txt`), `README.md` (one line pointing to it)

- [ ] **Step 1: Run the self-check on a fresh cluster A:** `make -C oracle down up && make -C oracle epic-check TARGET=reference-single FACTS=clusters/single.facts.json`. Expected:
  - `compare.py` reports no regressions;
  - `strict.json` shows every stable suite equal;
  - `ratchet.py` prints `passing N of N` on both gates, where N is the line count of `epic1.suites.txt`.

  A strict difference here means a missing mark or an undeclared fact. Fix the declaration or re-derive the marks with a fifth run added; never edit marks by hand.
- [ ] **Step 2: Write the README sections** from the real numbers.
- [ ] **Step 3: Run** `make -C oracle test`, `cargo test --workspace` → PASS.
- [ ] **Step 4: Commit** `oracle: Epic 1 checker self-check passes; document the checker and its coverage`.

---

## Notes for the executor

- Tasks 1, 2 and 4–8 are offline, apart from Task 4's regression check on the existing capture and Task 6's live check. Task 3 and the live steps need Docker with the reference image (`make -C oracle doctor`).
- Task 10's runs are long. Start them in the background, and don't poll; the script's log says when each phase ends.
- If the split cluster can't use another address, ports, memory or cores, record that under `not_varied` in its facts file, as Task 3 says, and mention it in the README.
