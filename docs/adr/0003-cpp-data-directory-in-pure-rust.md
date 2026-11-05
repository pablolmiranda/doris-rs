# doris-rs keeps the C++ BE's data directory, in pure Rust

A node must be able to switch between the C++ BE and doris-rs on the same disks, in both directions, so people can move to doris-rs one node at a time and roll back. doris-rs therefore lays out every storage root exactly as the C++ BE does (`cluster_id`, `data/<shard>/<tablet_id>/<schema_hash>/`, `trash/`, the other start folders) and builds tablet headers (`TabletMetaPB`) byte for byte as the C++ BE builds them. The tablet metadata store the C++ BE keeps in `<root>/meta` is a RocksDB database; doris-rs will read and write that format with its own Rust code, and never depends on RocksDB.

## Considered Options

- **A doris-rs format of its own:** simplest, but a migrated node could never go back, and every migration would need a conversion step.
- **Linking RocksDB:** exact by construction, but it brings C++ into the binary, against the one-binary, no-C++ destination.

## Consequences

- The RocksDB-format store is its own milestone (epic 1, milestone 3), before any rowset metadata is written, so the first data doris-rs writes already lands in the real format. The write and read loop moves to milestone 4.
- As built in milestone 3: the `doris-rocks` crate reads everything RocksDB 5.14.2 writes for the BE (manifest, logs, compressed and compacted table files) and writes logs, uncompressed table files and a snapshot manifest the C++ BE opens. The whole key set is held in memory, and a flush rewrites it into one table file per column family, so no compaction is implemented. `make -C oracle swap-check` proves both directions live against the reference C++ BE.
- M2's temporary header store (`<root>/doris-rs-meta/`) is moved into `meta/` on first start and removed.
- Until milestone 4, doris-rs refuses a root whose tablets hold data (rowsets with segments, or keys that exist only with data), naming the root and the tablet, and leaves it untouched.
- Dropping follows the C++ BE: the header is saved as SHUTDOWN and the trash sweep (at start, then every `min_garbage_sweep_interval`) moves the directory to trash with a `<tablet_id>.hdr` file and deletes the key.
- The `LOCK` file is taken with `flock`, the C++ BE uses `fcntl`; the two do not exclude each other, so running both binaries on one root at once is not prevented.
