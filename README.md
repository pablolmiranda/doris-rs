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
3. **Migrate one component at a time, with rollback.** Every step must leave a mixed cluster
   (old FE with new BE, or the reverse) working, and data written by the new code must stay
   readable by the old code.

## Layout

| Path | Purpose |
|---|---|
| `oracle/` | The conformance harness: reference cluster, suite runner, baseline, and gate |
| `.cache/` | Untracked: pinned upstream source worktree, build outputs |

The upstream source is expected as a sibling checkout at `../doris`. The harness pins a release
tag from it; see `oracle/versions.env`.

## Status

Phase 0: oracle harness. No Rust code yet.
