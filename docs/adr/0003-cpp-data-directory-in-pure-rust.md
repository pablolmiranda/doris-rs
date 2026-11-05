# doris-rs keeps the C++ BE's data directory, in pure Rust

A node must be able to switch between the C++ BE and doris-rs on the same disks, in both directions, so people can move to doris-rs one node at a time and roll back. doris-rs therefore lays out every storage root exactly as the C++ BE does (`cluster_id`, `data/<shard>/<tablet_id>/<schema_hash>/`, `trash/`, the other start folders) and builds tablet headers (`TabletMetaPB`) byte for byte as the C++ BE builds them. The tablet metadata store the C++ BE keeps in `<root>/meta` is a RocksDB database; doris-rs will read and write that format with its own Rust code, and never depends on RocksDB.

## Considered Options

- **A doris-rs format of its own:** simplest, but a migrated node could never go back, and every migration would need a conversion step.
- **Linking RocksDB:** exact by construction, but it brings C++ into the binary, against the one-binary, no-C++ destination.

## Consequences

- The RocksDB-format store is its own milestone (epic 1, milestone 3), before any rowset metadata is written, so the first data doris-rs writes already lands in the real format. The write and read loop moves to milestone 4.
- Until that milestone, headers live in a temporary doris-rs-only store (`<root>/doris-rs-meta/`) and nothing is ever written to `<root>/meta`. doris-rs refuses a root holding a C++ BE's data; a C++ BE started on a doris-rs root is unsupported. Test clusters lose nothing: suites recreate their data.
- The store implements only the subset the BE uses (the pinned RocksDB 5.14.2's write-ahead log, table files, manifest; Snappy; put, delete, prefix scan), proven against `meta/` directories the reference writes and by the swap check.
