# Wire golden corpus

Real FE↔BE RPC traffic from the C++ reference (version pinned in `../versions.env`), so Rust
codecs can be checked for byte-for-byte compatibility with what the Java FE and C++ BE put on
the wire.

| Path | Role |
|---|---|
| `scenario.sh` | Statements and stream load run inside the reference while capturing |
| `bin/capture.sh` | Recreates the reference with `compose/capture.yml`, runs the scenario, prints the pcap path |
| `captures/` | Untracked pcaps |
| `bin/extract.py` | Reassembles each TCP connection and writes one file per RPC message into `corpus/` |
| `bin/thrift_split.py` | Finds `TBinaryProtocol` message boundaries by walking the field structure |
| `corpus/` | Committed messages plus `manifest.json` (method, kind, seqid or HTTP/2 stream, size, sha256) |

Capture with `make wire-capture` from `oracle/`. The reference container is recreated, so any
state in it (for example the `golden` database) is lost; committed corpora are unaffected.

Captured ports: 9020 FrontendService (BE→FE Thrift), 9050 HeartbeatService and 9060
BackendService (FE→BE Thrift), 8060 PBackendService (FE→BE gRPC). Thrift here is
`TBinaryProtocol` without framing, so a connection can only be split into messages from its
first byte; that is why the capture starts before the FE boots.

## Extraction

`make wire-extract [PCAP=captures/<name>.pcap]` (default: the newest capture) rebuilds `corpus/`.
Only connections whose handshake (SYN and SYN-ACK) is in the capture are split. Each direction is
reassembled by TCP sequence number from the initial sequence number, so a missing segment is an
error rather than a silently shorter stream. gRPC messages are taken from tshark's HTTP/2
dissection of port 8060, with the method read from the `:path` header of the same HTTP/2 stream.

Nothing is dropped silently. The manifest's `capture` section lists the connections whose
handshake was not captured (`streams_without_handshake`), the handshaken connections that
produced no message, with their size, first bytes and protocol (`streams_without_messages`), and
every direction whose bytes end inside a Thrift message (`trailing_bytes_dropped`); `extract.py`
prints them too. `tests/test_wire_manifest.py` requires the first and last lists to be empty,
allows only brpc connections in the middle one, and checks that calls and replies pair up. Thrift
calls and replies pair by order on a connection, not by sequence id: the C++ BE's Thrift clients
send every call with seqid 0.

In capture `20261102-085436` the connections without messages are two brpc `baidu_std`
connections on port 8060 (4,246 and 1,279 bytes): the BE calling its own `PBackendService`,
which happens on a single node too. They are not in this version of the corpus.

tshark's own Thrift dissector labels every TCP segment of a multi-segment message as a separate
call, so its per-method counts run higher than the corpus (for example 20 `report` frames for 11
messages). Count message starts, not frames, when cross-checking.

Corpus `20261102-085436` (280 messages, 33 connections):

| Service | Method | Messages |
|---|---|---|
| BackendService | `get_dictionary_status` | 8 call, 8 reply |
| BackendService | `publish_topic_info` | 41 call, 41 reply |
| BackendService | `submit_tasks` | 12 call, 12 reply |
| FrontendService | `finishTask` | 28 call, 28 reply |
| FrontendService | `loadTxnBegin` | 1 call, 1 reply |
| FrontendService | `loadTxnCommit` | 1 call, 1 reply |
| FrontendService | `report` | 11 call, 11 reply |
| FrontendService | `reportExecStatus` | 3 call, 3 reply |
| FrontendService | `streamLoadPut` | 1 call, 1 reply |
| HeartbeatService | `heartbeat` | 5 call, 5 reply |
| PBackendService | `cancel_plan_fragment` | 4 request, 4 response |
| PBackendService | `exec_plan_fragment` | 5 request, 5 response |
| PBackendService | `exec_plan_fragment_prepare` | 4 request, 4 response |
| PBackendService | `exec_plan_fragment_start` | 4 request, 4 response |
| PBackendService | `fetch_data` | 12 request, 12 response |

## Conformance

`cargo test -p doris-wire` decodes every corpus message with the Rust types and encodes it again:
Thrift messages through `doris_thrift::dispatch::roundtrip_message` (header and the method's
`*Args`/`*Result` struct), gRPC payloads with the `doris-proto` type of each method, and the
`TPipelineFragmentParamsList` plan that `exec_plan_fragment` and `exec_plan_fragment_prepare`
requests carry in `TCompactProtocol`.

On corpus `20261102-085436`: all 58 gRPC payloads and all 9 plans re-encode byte for byte; 220 of
222 Thrift messages do, and the other 2 (`submit_tasks` calls) differ only in the order of a
`set<i64>`. The Java FE writes sets and maps in `HashSet`/`HashMap` iteration order, while the
Rust types are `BTreeSet`/`BTreeMap` and write them sorted, as the C++ BE's `std::set` does. The
test accepts such a message only when its canonical form (`doris_wire::canonical`: set elements
and map entries sorted by their encoded bytes) equals the re-encoding's, and lists it on stderr.

The plans needed one fix: the `thrift` crate writes a compact field header whose id is exactly 15
above the previous field's in the long form, where Java and C++ use the one-byte delta form.
`doris_thrift::compact::CompactOutputProtocol` writes headers by the reference's rule.
