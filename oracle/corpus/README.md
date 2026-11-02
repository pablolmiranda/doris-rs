# Golden on-disk corpus

Tablets written by the C++ Doris reference (version pinned in `../versions.env`) together with
the exact rows they contain, so a Rust reader can be checked for byte-for-byte compatibility.

| Path | Role |
|---|---|
| `tables/*.sql` | DDL and INSERTs, run in name order into database `golden` on the reference |
| `bin/load.sh` | Recreates `golden` from `tables/` |
| `bin/expect.sh` | Captures expected rows (`expected/<table>.tsv`), row counts, and DDL from the reference |
| `bin/extract.py` | Copies each table's tablet directory and metadata out of the BE, writes `manifest.json` |
| `expected/` | Generated. Never edit by hand; regenerate from the reference. |
| `data/<table>/` | Generated. Raw `.dat` segment files and `tablet_meta.json` |
| `manifest.json` | Generated. Reference version, per-table tablet id, row count, file hashes |

Add a new table with `make corpus TABLES="<name>"` from `oracle/` while the reference cluster is
up: it loads, captures, and extracts only that table and merges it into `manifest.json`. A full
`make corpus` regenerates every table and changes every segment file's bytes (new rowset ids)
without changing their rows, so it is reserved for a reference version bump. Committed corpus
files are treated as immutable once the reader has been verified against them.

Rules: expected files always come from the reference, never from a hand edit; NULL appears in
expected TSVs as the bare word `NULL` (what the reference's mysql client prints), so no corpus
string value may equal `NULL`; every table sets `disable_auto_compaction` so rowset layout
survives between load and extract; corpus v1 stays under 2 MB; each new format axis (indexes, VARIANT, merge-on-write, ...) is a new table file
and a new corpus version noted in the manifest.

## Coverage (corpus v1, V2 storage format)

| Table | Format axes it pins down | Rust reader |
|---|---|---|
| `scalar_ints` | BOOL (RLE), TINYINT to LARGEINT (bit-shuffle), three rowsets, NULL runs, all-NULL rows | decoded |
| `scalar_others` | FLOAT, DOUBLE, DECIMAL64, DATEV2, DATETIMEV2(6), CHAR/VARCHAR/STRING dictionaries | decoded |
| `zstd_ints` | integers on a zstd table whose small flush is written uncompressed | decoded |
| `empty_table` | a zero-row load writes no segment | nothing to read |
| `lz4f_big`, `zstd_big` | LZ4F and ZSTD page bodies, multi-page columns via ordinal index pages, compressed dictionaries | decoded |
| `strings_fallback` | dictionary-to-plain fallback in CHAR, VARCHAR, STRING; CHAR cut at an interior NUL | decoded |
| `scalar_more` | DECIMAL32/128I, DATETIME(0) and (3), IPV4, IPV6, legacy DATE/DATETIME/DECIMALV2, multi-block 3- and 12-byte bit-shuffle | decoded |

Not covered yet, each a later corpus version: the V3 storage format (PLAIN_V2 dictionary
fallback, plain numerics, externalized column meta), DECIMAL256, TIMESTAMPTZ, JSONB, VARIANT,
ARRAY/MAP/STRUCT, HLL/BITMAP/QUANTILE_STATE/AGG_STATE, zone map/bloom/inverted indexes,
UNIQUE (merge-on-write) and AGGREGATE tables, and data written by older Doris releases.
