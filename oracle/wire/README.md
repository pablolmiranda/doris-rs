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
Only connections whose SYN is in the capture are used. Each direction is reassembled by TCP
sequence number from the initial sequence number, so a missing segment is an error rather than a
silently shorter stream, and every Thrift direction must split into whole messages with no bytes
left over. gRPC messages are taken from tshark's HTTP/2 dissection of port 8060, with the method
read from the `:path` header of the same HTTP/2 stream.

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
