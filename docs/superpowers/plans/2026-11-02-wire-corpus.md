# Wire Golden Corpus Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capture real FE↔BE RPC traffic from the 4.1.5 reference and prove Rust codecs decode and re-encode every message byte-for-byte.

**Architecture:** A capture sidecar shares the reference container's network namespace from boot; a Python extractor turns the pcap into committed per-message files plus a manifest; three crates (`doris-proto`, `doris-thrift`, `doris-wire`) hold the schemas and the conformance tests.

**Tech Stack:** docker compose, `nicolaka/netshoot:v0.16` (tcpdump 4.99, tshark 4.6), python3 stdlib, Rust 1.93 with `prost` 0.14 and `thrift` 0.25, Thrift compiler 0.24.0 in `doris-rs/oracle-runner:17`.

**Spec:** `docs/superpowers/specs/2026-11-02-wire-corpus-design.md`

## Global Constraints

- Reference pinned at `4.1.5-release`; IDL and protos copied verbatim from `.cache/doris-src/gensrc/{thrift,proto}` (plus the generated `gensrc/build/thrift/*.thrift`).
- The pcap is never committed; extracted messages and `manifest.json` are, and are never hand-edited.
- Only messages from TCP streams whose SYN was captured enter the corpus.
- Generated Thrift code is exempt from lints; every other crate is `unsafe_code = "forbid"`, clippy `-D warnings` and rustfmt clean.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. A TCP segment retransmitted or delivered out of order in the capture: reassembly by sequence number must not duplicate or drop bytes. Unit test in Task 2.
2. A capture that ends mid-message: the trailing partial message is dropped, never stored. Unit test in Task 2.
3. A Thrift `string` field holding bytes that are not UTF-8: the generated Rust type would reject it. The round-trip test reports such a message by name rather than aborting the run. Task 5.
4. A gRPC message with the compressed flag set: stored and reported, not decoded as protobuf. Task 2 records the flag; Task 5 asserts none are compressed.
5. A protobuf message carrying a field the pinned schema lacks: prost drops it and the re-encode differs; the test names the method and the first differing offset. Task 5.

---

### Task 1: Capture harness

**Files:** Create `oracle/compose/capture.yml`, `oracle/wire/scenario.sh`, `oracle/wire/bin/capture.sh`, `oracle/wire/README.md`; modify `oracle/Makefile` (`wire-capture`), `.gitignore` (`oracle/wire/captures/`).

**Interfaces:** Produces `oracle/wire/captures/<run>.pcap` (untracked) from `make wire-capture`.

- [ ] Step 1: Write `capture.yml` (capture service as in the spec, `-s 0 -U`, ports 9020/9050/9060/8060, output dir from `CAPTURE_DIR`) and `scenario.sh` (runs inside the reference: wait for an alive BE; CREATE DATABASE/TABLE; INSERT; SELECT; stream load of three rows with `-H "Expect:100-continue"`; SELECT count; DROP TABLE FORCE; sleep 25 s for periodic reports).
- [ ] Step 2: Write `capture.sh`: `docker compose ... up -d --force-recreate --no-deps doris`, then immediately `up -d capture`, wait for healthy, run `scenario.sh` via `exec -T doris`, stop the capture, print the pcap path.
- [ ] Step 3: Run `make wire-capture`. Expected: a pcap in which tshark lists SYN-opened streams on all four ports and the calls `heartbeat`, `report`, `submit_tasks`, `finishTask`, `reportExecStatus`, `exec_plan_fragment`, `fetch_data`, and at least one of `loadTxnBegin`/`streamLoadPut`/`loadTxnCommit`.
- [ ] Step 4: Commit.

### Task 2: Extraction into a committed corpus

**Files:** Create `oracle/wire/bin/thrift_split.py`, `oracle/wire/bin/extract.py`, `oracle/tests/test_thrift_split.py`, `oracle/tests/test_tcp_reassembly.py`, `oracle/tests/test_wire_manifest.py`; generate `oracle/wire/corpus/{manifest.json,messages/}`.

**Interfaces:**
- `thrift_split.read_message(buf: bytes, pos: int) -> ThriftMessage(name, mtype, seqid, start, end)` raising `Incomplete` when the buffer ends early and `ValueError` on a non-strict header; `split_messages(buf) -> list[ThriftMessage]` stops at a trailing partial message.
- `extract.reassemble(segments: list[(seq_raw:int, payload:bytes)]) -> bytes` (sorted by sequence, overlaps de-duplicated, a gap raises `Gap`).
- `manifest.json`: `{"reference": {...}, "capture": {"pcap": name, "sha256": hex}, "messages": [{"file", "protocol": "thrift"|"grpc", "port", "service", "method", "kind": "call"|"reply"|"exception"|"oneway"|"request"|"response", "seqid"|"h2_stream", "tcp_stream", "compressed": bool (grpc), "bytes", "sha256"}]}` in capture order.

- [ ] Step 1: Write the three Python test files (synthetic strict TBinary messages covering every field type, nested structs, maps, lists, a truncated tail; segment lists with a duplicate, an overlap, an out-of-order pair, and a gap; manifest: files exist with matching sha256 and size, every Thrift file re-splits to exactly one message with the recorded name, required methods present).
- [ ] Step 2: Run them — Expected: FAIL (modules and corpus missing).
- [ ] Step 3: Implement `thrift_split.py` and `extract.py` (tshark `-T fields` for SYN streams and segments with `tcp.seq_raw`; gRPC via `-d tcp.port==8060,http2 -Y grpc` with `http2.streamid`, `grpc.compression_flag`, `grpc.message_data`, and the `:path` from HEADERS on the same tcp/h2 stream).
- [ ] Step 4: Run `make wire-extract`; then all Python tests pass; commit.

### Task 3: `doris-proto` crate

**Files:** Create `crates/doris-proto/{Cargo.toml,build.rs,src/lib.rs,proto/*.proto,proto/README.md}`; move the four protos out of `crates/doris-segment/proto/`; make `doris-segment` depend on `doris-proto` and re-export it as `pb`; add `internal_service`, `data`, `descriptors`, `runtime_profile`.

**Interfaces:** `doris_proto::doris::{PExecPlanFragmentRequest, ...}` and `doris_proto::doris::segment_v2::*`; `doris_segment::pb` keeps both paths working.

- [ ] Step 1: Move and add protos; `cargo test -p doris-segment` — Expected: all 28 tests still pass (refactor guard).
- [ ] Step 2: Add `crates/doris-proto/tests/compiles.rs` asserting `PExecPlanFragmentRequest::default()` encodes to an empty buffer; run; commit.

### Task 4: `doris-thrift` crate

**Files:** Create `crates/doris-thrift/{Cargo.toml,regen.sh,patches.md,src/lib.rs,src/gen/*.rs,src/dispatch.rs}`, `crates/doris-thrift/idl/*.thrift` (verbatim copies).

**Interfaces:** `doris_thrift::gen::<module>::*` (all 27 modules, `*Args`/`*Result` public); `doris_thrift::dispatch::roundtrip_call(service: &str, method: &str, kind: MessageKind, struct_bytes: &[u8]) -> Result<Vec<u8>, RoundTripError>` that decodes the method's args/result struct with `TBinaryInputProtocol` and re-encodes it with `TBinaryOutputProtocol`; `roundtrip_plan_compact(bytes) -> Result<Vec<u8>, _>` for `TPipelineFragmentParamsList`.

- [ ] Step 1: Write `regen.sh` (runs the pinned compiler in the runner image over `idl/`, applies the parquet patch and `pub` exposure with sed, writes `dispatch.rs` from each processor's `match` arms) and `patches.md`.
- [ ] Step 2: Run it; `cargo build -p doris-thrift` — Expected: compiles.
- [ ] Step 3: Unit test `tests/dispatch.rs`: a `heartbeat` call built with the generated client types round-trips; an unknown method name is `RoundTripError::UnknownMethod`. Run; commit.

### Task 5: Conformance tests over the wire corpus

**Files:** Create `crates/doris-wire/{Cargo.toml,src/lib.rs (manifest loader),tests/thrift_roundtrip.rs,tests/grpc_roundtrip.rs}`.

**Interfaces:** `doris_wire::Manifest::load()`, `messages()`, `read(&MessageEntry) -> Vec<u8>`.

- [ ] Step 1: Write the tests: `every_thrift_message_reencodes_identically` (header + struct via `dispatch`, collecting failures with method and first differing offset, asserting none), `every_grpc_message_reencodes_identically` (method → prost type table for every captured method; none compressed), `exec_plan_fragment_plans_reencode_identically` (nested compact Thrift), and a coverage assertion naming the required methods.
- [ ] Step 2: Run — Expected: FAIL first on missing crate, then on any real incompatibility, which is fixed or recorded as a Ruling with its cause.
- [ ] Step 3: All pass; clippy, fmt; commit.
