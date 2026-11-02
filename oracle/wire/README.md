# Wire golden corpus

Real FE↔BE RPC traffic from the C++ reference (version pinned in `../versions.env`), so Rust
codecs can be checked for byte-for-byte compatibility with what the Java FE and C++ BE put on
the wire.

| Path | Role |
|---|---|
| `scenario.sh` | Statements and stream load run inside the reference while capturing |
| `bin/capture.sh` | Recreates the reference with `compose/capture.yml`, runs the scenario, prints the pcap path |
| `captures/` | Untracked pcaps |

Capture with `make wire-capture` from `oracle/`. The reference container is recreated, so any
state in it (for example the `golden` database) is lost; committed corpora are unaffected.

Captured ports: 9020 FrontendService (BE→FE Thrift), 9050 HeartbeatService and 9060
BackendService (FE→BE Thrift), 8060 PBackendService (FE→BE gRPC). Thrift here is
`TBinaryProtocol` without framing, so a connection can only be split into messages from its
first byte; that is why the capture starts before the FE boots.
