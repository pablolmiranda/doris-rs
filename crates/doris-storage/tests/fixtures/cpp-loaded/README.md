# cpp-loaded

The reference C++ BE (apache/doris:all-in-one-4.1.5) after the M4 load dataset
(`oracle/swap/load-dataset.sql`): INSERT … VALUES and CSV/JSON stream loads into four
duplicate-key tables, and `m5.zstd`: one 20,000-row load whose pages the BE compresses with ZSTD. Captured by `make -C oracle loaded-fixture` (`oracle/swap/capture-loaded.sh`).

- `meta/`, `stream_load/`: the BE's RocksDB databases after a graceful stop
- `data/`: the tablet directories with their segment files
- `headers/<tablet_id>.json`: `GET /api/meta/header/<id>?byte_to_base64=true`, rowsets included
- `rowsets.txt`: `<tablet_id> <rowset_id> <start>-<end> <num_rows> <num_segments>` per rowset
- `layout.txt`: every file under `data/` and its size
- `replies/<name>.tsv`: the replies to `oracle/swap/readback-queries.sql`
