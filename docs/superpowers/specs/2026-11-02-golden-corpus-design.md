# Golden on-disk corpus and first Rust segment reader: design

Date: 2026-11-02. Status: approved direction ("keep building in the order you suggested").

## Intent

The second oracle after the regression suites: a committed set of tablets written by the C++
Doris 4.1.5 reference, with the exact rows they contain. A Rust reader that reproduces those rows
byte-for-byte proves format compatibility without any write-side risk. It is also the first real
Rust code in `doris-rs`, so the storage read path is where the migration starts.

## Goals

- A reproducible corpus: SQL that creates and fills tables on the reference, expected rows
  captured from the reference itself, and the raw tablet files plus metadata copied out of the BE.
- Coverage of the format axes a reader must handle, growing version by version. v1 covers:
  DUPLICATE tables with all scalar types (nullable and not), the default LZ4F codec plus ZSTD,
  multiple rowsets per tablet (several INSERTs), and multi-row pages.
- A Rust crate that parses the segment footer and decodes the first page encodings, with tests
  that iterate the corpus manifest and compare to the expected rows.

## Non-goals (v1)

Inverted indexes, VARIANT, merge-on-write delete bitmaps, AGGREGATE tables, cold storage, and
the Rust write path. Each is a later corpus version with its own plan.

## Anti-goals

- Hand-edited expected files. Expectations always come from the reference.
- Corpus data large enough to make `git clone` slow. v1 stays under 2 MB.
- Reading tablet files the Rust side was never meant to see (for example BE-private trash).

## Constraints

- Reference: `apache/doris:all-in-one-4.1.5` from `oracle/compose/reference-single.yml`, suites
  and protos from the `4.1.5-release` worktree at `.cache/doris-src`.
- Segment format facts (from `be/src/storage/segment/segment.cpp` and `segment_v2.proto`): the
  last 12 bytes of a `.dat` are `footer_length` (u32 little-endian), `checksum` (u32 crc32c of
  the footer bytes), and the magic `D0R1`; the `SegmentFooterPB` protobuf precedes them. Pages
  are `body | PageFooterPB | footer_size(u32) | crc32c(u32)`.
- Tablet files on the BE: `/opt/apache-doris/be/storage/data/<shard>/<tablet_id>/<schema_hash>/
  <rowset_id>_<segment_id>.dat`. Tablet metadata: `GET http://be:8040/api/meta/header/<tablet_id>`
  (JSON serialization of `TabletMetaPB`, includes `rs_metas` and the schema).
- Rust 1.93 stable, edition 2021. Protobuf via `prost` with `prost-build` and a vendored protoc
  (`protoc-bin-vendored`), compiling copies of `segment_v2.proto`, `olap_file.proto`,
  `olap_common.proto`, `types.proto` taken verbatim from the pinned tag (Apache-2.0, header kept).

## Decisions

- Corpus lives in `oracle/corpus/` (builder scripts, SQL, expected rows, manifest) and the
  binary tablet files under `oracle/corpus/data/` committed to git while small.
- Rust code lives in a Cargo workspace at the repo root, first crate `crates/doris-segment`.
- Expected rows are TSV produced by `mysql -N --batch` with `ORDER BY` on every column, so they
  are deterministic regardless of tablet layout. NULL is the literal `\N` as mysql prints it.
- The manifest is JSON: reference version and commit, each table's DDL, tablet id, schema hash,
  row count, and each file's relative path and sha256.

## Left to the builder

Exact SQL values for each type (must include min, max, zero, negative, NULL, and a repeated value
so dictionary and RLE paths are exercised), page-level test organization in Rust, and error types.
