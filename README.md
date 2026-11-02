# doris-rs

A Rust re-implementation of [Apache Doris](https://github.com/apache/doris) with the goal of a
single statically linked binary that exposes the same interfaces and behavior as the current
Java Frontend plus C++ Backend.

## Principles

1. **Identical interface and behavior.** The MySQL wire protocol on 9030, the HTTP API on 8030 and
   8040 (including stream load), Arrow Flight SQL, the Thrift and Protobuf contracts in `gensrc/`,
   and the on-disk segment, index, and metadata formats are all treated as frozen specifications.
   A client, a tablet, or a peer process must not be able to tell which implementation it is
   talking to.
2. **The existing Doris test suite is the oracle.** The regression framework in the upstream repo
   is a black-box suite that talks only through public protocols. It runs unchanged against the
   reference C++ build and against this project. See `oracle/`.
3. **One native binary, easy to deploy and run.** `doris-rs` ships as a single statically linked
   executable started with one command and one config file. It is never containerized by this
   project; Docker appears only in `oracle/`, where it hosts the C++ reference cluster and the
   Java test runner that we use to measure ourselves and will never ship.
4. **Migrate one component at a time, with rollback.** Every step must leave a mixed cluster
   (old FE with new BE, or the reverse) working, and data written by the new code must stay
   readable by the old code.

## Layout

| Path | Purpose |
|---|---|
| `oracle/` | The conformance harness: reference cluster, suite runner, baseline, and gate |
| `crates/` | Rust crates (see Status) |
| `.cache/` | Untracked: pinned upstream source worktree, build outputs |

The upstream source is expected as a sibling checkout at `../doris`. The harness pins a release
tag from it; see `oracle/versions.env`.

## Status

- `oracle/`: conformance harness over the upstream regression suites, with committed baselines
  against the 4.1.5 reference, the golden on-disk corpus (`oracle/corpus/`), and the wire
  corpus of captured FE↔BE RPC messages (`oracle/wire/`).
- `crates/doris-segment`: reads V2-format segment files written by the reference and decodes
  every scalar column type, verified row by row against the corpus.
- `crates/doris-proto`: prost types for the pinned `.proto` files (segment format and
  `PBackendService`).
- `crates/doris-thrift`: Rust types generated from the pinned Thrift IDL, plus a compact
  protocol writer that matches the reference's field headers.
- `crates/doris-wire`: every message in the wire corpus decodes and re-encodes to the captured
  bytes (Thrift calls and replies, gRPC payloads, and the compact Thrift plans inside them), up
  to the order of set elements, which the Java FE writes in hash order.
