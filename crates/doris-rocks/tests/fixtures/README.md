# C++ BE `meta/` fixtures

Captured from the reference C++ BE (`apache/doris:all-in-one-4.1.5`, RocksDB 5.14.2) by
`make -C oracle meta-fixtures` (`oracle/swap/capture-meta.sh`) on the swap cluster: the FE-only
cluster with the C++ BE in a host-network container, storage root `/tmp/doris-rs-oracle/swap`.
Internal loads are off (`enable_audit_plugin`, `enable_auto_analyze`), so no tablet holds data.

Format reference: RocksDB v5.14.2 source, `https://github.com/facebook/rocksdb/archive/refs/tags/v5.14.2.tar.gz`,
sha256 `25a5a087891681c7399f3558bc3da78e1a88937ef0a4e7454452e9cba11bf391` (unpacked read-only at
`.cache/rocksdb-5.14.2`).

| Directory | What it shows |
|---|---|
| `cpp-wal/` | The M2 scenario's tables (duplicate-key, merge-on-write, range-partitioned plus ADD PARTITION) and the FE's internal tables, then a graceful stop. `meta/` holds a manifest and one log, no table file: the BE syncs the log and closes without a flush. |
| `cpp-compacted/` | Four more starts of the same root, each creating a table and dropping the previous round's table. Each start replays the log into a level-0 table file (Snappy blocks, format version 2), so `meta/` holds table files, a newer manifest and a short log. A dropped tablet's header is rewritten as SHUTDOWN and deleted when the BE next starts, so only the last round's dropped table still has keys in `meta/` (SHUTDOWN); they are in `dropped.txt`. The FE sends that DROP after a tablet report, so those tablets' `headers/` were saved before it arrived and still say RUNNING. |

Each directory also has `stream_load/` (the stream load recorder's database on the first root),
`headers/<tablet>.json` (`GET /api/meta/header/<id>?byte_to_base64=true` for every tablet the BE
held) and `keys.txt` (`tabletmeta_<tablet_id>_<schema_hash>` for each, sorted). `LOCK` and `LOG*`
files are left out.
