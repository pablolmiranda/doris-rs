# M5 research: reading tablets for the OLAP scan

Paths: `SRC` = `.cache/doris-src/be/src` (4.1.5: olap code under `SRC/storage/`, exec under
`SRC/exec/`), `FE` = `.cache/doris-src/fe/fe-core/src/main/java/org/apache/doris`, `THRIFT` =
`.cache/doris-src/gensrc/thrift`, `PROTO` = `.cache/doris-src/gensrc/proto`, `SEG` =
`crates/doris-segment`, `FIX` = `crates/doris-storage/tests/fixtures/cpp-loaded`.
**[dump]** = observed by decoding `FIX/data/**/*.dat` with a scratch binary built against
`doris-segment` (scratchpad `m5/seg/`, not in the repo).

---

## 0. Bottom line

- A correct DUP_KEYS scan = **rowsets on the greedy version path [0, v]**, each rowset's segments
  in id order, every row of every segment, then **evaluate all of `TPlanNode.conjuncts` after
  reading**. Key ranges, zone maps, short-key seeks, `sort_info`, push-down agg, runtime/topn
  filters are all optimizations the BE may skip. Only delete predicates (rowset meta) and the MoW
  delete bitmap are mandatory filters — neither exists in M5's DUP-only, no-DELETE scope, but a
  reader must **refuse** (error) rather than ignore them.
- No order is required for `SELECT *` without ORDER BY (DUP: concatenation, plus parallel scanners).
- The existing `doris-segment` reader already decodes **every file in the cpp-loaded fixture** with
  the right values (0 errors, §6). Gaps are codecs/encodings the 4.1.5 BE writes only in other
  situations (big flushes → ZSTD pages; compaction; other `compression` properties) — §2.
- Lazy top-n materialization is avoidable: FE session var `topn_lazy_materialization_threshold`
  (default **1024**) ≤ 0 disables it; supporting it needs our own brpc `multiget_data_v2` server
  (the C++ BE calls itself over brpc even on one node) — §5.

---

## 1. Rowset / version selection

### 1.1 Inputs from FE
| Field | Meaning | Source |
|---|---|---|
| `TPaloScanRange.tablet_id` (5) | tablet | `THRIFT/PlanNodes.thrift:82-92` |
| `TPaloScanRange.version` (3) | **string**, decimal; read version v (partition visible version) | parsed with `from_chars`, `SRC/exec/operator/olap_scan_operator.cpp:596-600,664-666` |
| `schema_hash` (2), `version_hash` (4, deprecated), `hosts`, `db_name`, `partition_column_ranges` (7) | not used for reading (no BE reader of `partition_column_ranges`) | grep |
| **no key ranges** in TPaloScanRange | key ranges are derived BE-side from conjuncts + `TOlapScanNode.key_column_name/type` | `olap_scan_operator.cpp:951-975` |

Scanner reads version range `{0, v}` (`SRC/exec/scan/olap_scanner.cpp:78`).

### 1.2 Which rowsets
- `BaseTablet::capture_read_source({0,v})` → `capture_consistent_rowsets_unlocked` →
  `capture_consistent_versions_unlocked` (`SRC/storage/rowset_version_mgr.cpp:64-99,101-157,184-207`).
- Version path: `VersionGraph::capture_consistent_versions` (`SRC/storage/version_graph.cpp:417-480`).
  Vertices are version starts; a rowset [s,e] is an edge s → e+1; edges sorted descending. From
  vertex 0, **greedily take the edge reaching the largest vertex ≤ v+1**, repeat until v+1 is
  reached (`:440-466`). I.e. prefer the widest (compacted) rowset that does not pass v.
  No path → `InternalError "fail to find path in version_graph"`; reported as
  `VERSION_ALREADY_MERGED (-230)` when nothing is missing but v is inside a compacted rowset
  whose inputs are gone, else missed-version error (`rowset_version_mgr.cpp:69-85`), unless the
  session's `skip_missing_version` (`FE/qe/SessionVariable.java:502`).
- The graph includes **stale** rowsets (compaction inputs kept until sweep); a version is looked
  up in `_rs_version_map` (visible) then `_stale_rs_version_map` (`rowset_version_mgr.cpp:109-131`).
  For M5 (no compaction by doris-rs, swap at rest) visible rowsets suffice, but a C++-compacted
  root can hold stale metas: take them into the graph if the meta store has them.
- Result order = path order (ascending versions) (`:134-141`).
- Every tablet has the empty `[0-1]` rowset; empty rowsets (`num_segments 0`) just yield nothing.
  **[FIX]** `headers/1793889101822.json`: `[0-1] 0 rows`, `[2-2] 3 rows`, `[3-3] 0 rows, empty`
  (a load that sent this bucket no rows still commits a version).

### 1.3 Overlap
- `is_segments_overlapping() = num_segments > 1 && is_singleton_delta && segments_overlap != NONOVERLAPPING`
  (`SRC/storage/rowset/rowset_meta.h:297-299`). A multi-flush load ⇒ OVERLAPPING segments.
- Within a rowset the reader uses a **merge** iterator only when
  `need_ordered_result && overlapping && segments > 1` (`SRC/storage/rowset/beta_rowset_reader.h:62-64`),
  else a **union** iterator = segments in id order (`beta_rowset_reader.cpp:330-347`).
- `need_ordered_result` is **false for DUP_KEYS queries** (and MoW, aggregation, direct mode),
  true again only if `read_orderby_key` (`SRC/storage/tablet/tablet_reader.cpp:125-156`).
- Across rowsets: `VCollectIterator::init` sets `_merge=false` for READER_QUERY on DUP/MoW/direct
  mode and when rowsets are non-overlapping; `force_merge` (= `read_orderby_key`) re-enables it
  (`SRC/storage/iterator/vcollect_iterator.cpp:68-99`). Non-merge = Level1Iterator union of
  rowsets in order (`:185-192`). DUP's next-block function is `_direct_next_block`
  (`SRC/storage/iterator/block_reader.cpp:237-245`).
- `direct_mode` (`olap_scanner.cpp:336-350`) = `aggregation(is_preaggregation) || single_version ||
  push-agg`; `has_single_version` = one rowset starting at 0, or `[0-1]` empty + one `[2-y]`,
  non-overlapping (`SRC/storage/tablet/tablet_reader.h:102-110`).

### 1.4 UNIQUE / AGG (not M5; what they'd need)
| Table | Storage must | Where |
|---|---|---|
| UNIQUE MoR | k-way merge of all rowsets/segments by full key, keep newest version per key (seq column if any), then drop `__DORIS_DELETE_SIGN__=1` (FE adds conjunct) | `block_reader.cpp:246-252` (`_unique_key_next_block`), `vcollect_iterator.cpp:115-190` |
| UNIQUE MoW | no merge; apply **delete bitmap** per (rowset, segment, version) — mandatory | `rowset_version_mgr.cpp:142-144`, `SRC/storage/segment/segment_iterator.cpp:511-514`, `beta_rowset_reader.cpp:175-187` |
| AGG | merge by key + aggregate value columns unless `is_preaggregation` (TOlapScanNode.4) is true | `block_reader.cpp:256-258`, `olap_scan_operator.cpp:529-540` |
Unless `read_mor_as_dup` (TOlapScanNode.25) / `skip_storage_engine_merge` force direct mode
(`olap_scanner.cpp:338-344`).

---

## 2. Segment encodings and compression

### 2.1 Compression (`CompressionTypePB`, `PROTO/segment_v2.proto:46-56`)
Codec chosen per column from `ColumnMetaPB.compression` (`SRC/storage/segment/column_reader.cpp:292,1800`),
not footer `compress_type`. Page is compressed iff stored body len != `uncompressed_size`
(`SRC/storage/segment/page_io.cpp:203-219`; NO_COMPRESSION + mismatch = Corruption).

| PB | Written by 4.1.5? | On-disk bytes (`SRC/util/block_compression.cpp`) | `SEG` |
|---|---|---|---|
| NO_COMPRESSION 2 | yes: every memtable flush ≤ `segment_compression_threshold_kb`=256 KB (`SRC/storage/rowset/segment_creator.cpp:73,142-143,178-179`, `SRC/common/config.cpp:1005`) | — | ✅ |
| ZSTD 7 | **FE default** (`Config.default_compression_type="ZSTD"`, fe-common `Config.java:1973`) for big flushes **and all compaction output** (threshold applies only to `flush_single_block`; compaction writers inherit the tablet codec: `segment_creator.cpp:363-366`) | one standard frame, level 3, `ZSTD_c_checksumFlag=1` (XXH64 trailer), content size may be absent (`:1041-1200`, `:1135`) | ✅ (corpus) |
| LZ4F 5 | property `lz4f`; BE fallback for absent/LZ4HC/DEFAULT (`SRC/storage/tablet/tablet_meta.cpp:117,218-239`) | LZ4 frame, max256KB, block-linked, no checksums, no content size (`:315-470,548-553`) | ✅ (corpus) |
| LZ4 4 | property `lz4` | one raw LZ4 block, no prefix; decode with capacity = `uncompressed_size` (`:92-200`) | ❌ |
| SNAPPY 3 | property `snappy` | raw snappy (varint length prefix of snappy itself), no Hadoop framing (`:754-800`) | ❌ |
| ZLIB 6 | property `zlib` | zlib wrapper (2-byte header, deflate, adler32) (`:860-946`) | ❌ |
| LZ4HC 8, DEFAULT 1 | never: tablet_meta maps them to LZ4F | — | n/a |
FE accepts `no_compression|lz4|lz4f|lz4hc|zlib|zstd|snappy|default_compression`
(`FE/common/util/PropertyAnalyzer.java:1147-1170`). Compressed body kept only if it saves ≥ 10 %
(`SRC/storage/segment/page_io.cpp:53-71`, `column_writer.h:78`). Ordinal index pages and
zone-map pages are always raw; dict pages use the column codec (`column_writer.cpp:755-771`).

**Consequence for the swap check:** the C++ BE compacts the doris-rs root after a few loads
(cumulative compaction) → **ZSTD pages appear in rowsets doris-rs must read**, even for tiny tables.

### 2.2 Encodings (`SRC/storage/segment/encoding_info.cpp`)
| Type | Registered | V2 default (`:323-354`) | V3 default (`:357-387`) | `SEG` |
|---|---|---|---|---|
| TINYINT..LARGEINT | BIT_SHUFFLE, PLAIN, FOR | BIT_SHUFFLE | **PLAIN** | ✅ both |
| FLOAT, DOUBLE | BIT_SHUFFLE, PLAIN | BIT_SHUFFLE | BIT_SHUFFLE | ✅ |
| DECIMAL32/64/128I | BIT_SHUFFLE, PLAIN | BIT_SHUFFLE | BIT_SHUFFLE | ✅ |
| DATEV2, DATETIMEV2 | BIT_SHUFFLE, PLAIN, FOR | BIT_SHUFFLE | BIT_SHUFFLE | ✅ |
| BOOL | RLE, BIT_SHUFFLE, PLAIN | RLE | RLE | ✅ RLE, PLAIN |
| CHAR, VARCHAR, STRING | DICT, PLAIN, PLAIN_V2, PREFIX | DICT (fallback PLAIN) | DICT (fallback + dict page **PLAIN_V2**) | ✅ V2; ❌ PLAIN_V2 |
- RLE is registered for BOOL only (`:114,274`) → `SEG`'s RLE-integer branch (`SEG/src/column.rs:199-233`) is dead.
- Encoding is only ever `resolve_default_encoding` (`segment_writer.cpp:154`, `vertical_segment_writer.cpp:172`);
  no property/compaction override. So FOR, PREFIX (only MoW PK index), BIT_SHUFFLE BOOL are never
  written for our types.
- **V3** = table property `storage_format="V3"` (`FE/common/util/PropertyAnalyzer.java:1256-1277`, default V2):
  PLAIN ints; DICT fallback + dict page PLAIN_V2 (`column_writer.cpp:507-510`, `binary_dict_page.cpp:50-54`);
  footer `version=2` with **external column meta**: `footer.columns` empty, metas back-to-back from
  `col_meta_region_start` (field 11) with `column_meta_entries` (12) `{unique_id, length}`
  (`segment_writer.cpp:1128-1133`, `external_col_meta_util.cpp`, `column_meta_accessor.cpp:191`).
- PLAIN_V2 page: `(varuint32 len, bytes)* | num_elems u32 LE` (`binary_plain_page_v2.h:18-25`); CHAR strip `\0`.

### 2.3 Dictionary
Data page = `u32 LE mode | body`: 5 DICT → bit-shuffled INT page of codes (only format);
2 PLAIN → BinaryPlainPage V1; 8 PLAIN_V2 (V3). Writer switches to plain permanently once the dict
fills (`binary_dict_page.cpp:168,180-185,205-206`). One dict page per column, type DICTIONARY_PAGE,
`dict_page_footer.encoding` = 2 (V2) / 8 (V3), compressed with the column codec.

### 2.4 Indexes and null map
- Ordinal index: root = the data page (1 page) or **exactly one uncompressed leaf** — never
  multi-level (`ordinal_page_index.h:52`, `ordinal_page_index.cpp:59-70,83-140`). `SEG` handles both.
  IndexedColumn (page zone maps) is single-leaf too (`indexed_column_writer.cpp:196-215`); zone map
  pages hard-coded PLAIN/NO_COMPRESSION (`zone_map_index.cpp:322-328`).
- Null map: `values | nullmap`, RLE bit_width 1, 1 = NULL, present only if column nullable and page
  has a NULL (`column_writer.cpp:842-857`, `parsed_page.h:48-57`); unchanged in V3. `SEG` ✅.
- Type/nullability come from the segment's ColumnMetaPB (`column_reader.cpp:284-292`).
- Column absent from footer → tablet-schema default or NULL (`segment.cpp:845-860,887-889`) —
  not `ColumnMetaPB.default_value`.

### 2.5 Reader gap list (`SEG` vs what 4.1.5 can write for M5 types, DUP)
| # | Gap | Needed when | Priority |
|---|---|---|---|
| 1 | Whole-column decode only (`read_column` returns all rows); no column projection by uid list across segments, no batching/limit | every scan; fine for M5 sizes, memory = whole segment | build the scan API on top (M5) |
| 2 | LZ4 raw block, SNAPPY, ZLIB codecs (`SEG/src/page.rs:85-107` → `UnsupportedCompression`) | table property `compression` = lz4/snappy/zlib | M5 if cheap (lz4_flex block already a dep; snappy/zlib need pure-Rust crates) else clear error |
| 3 | V3 external column meta (`column_meta` only searches `footer.columns`, `SEG/src/column.rs:64-69`) | `storage_format="V3"` | refuse with clear error in M5 |
| 4 | PLAIN_V2 (dict mode 8, dict page encoding 8, column encoding 8; `SEG/src/binary.rs:92,134-165`) | V3 | with #3 |
| 5 | Missing-column default value | ALTER ADD COLUMN (light schema change) | out of M5 unless schema change is in scope |
| 6 | Rowset-level: delete_predicate, MoW delete bitmap | DELETE / UNIQUE MoW | refuse |
Not needed: PREFIX, FOR, BIT_SHUFFLE BOOL, RLE ints, multi-level index, bloom/inverted indexes.
The LZ4F block-linked and ZSTD-with-checksum paths are already exercised by the golden corpus
(`oracle/corpus`, `lz4f_big`, ZSTD tables) via `lz4_flex 0.14` / `ruzstd 0.9`.

---

## 3. Scan output order (DUP_KEYS)

- `SELECT *` without ORDER BY needs **no order**. C++ produces: rowsets in version-path order →
  segments in id order → rows in segment (= key-sorted) order, per scanner (§1.3). But
  `enable_parallel_scan` (session default **true**, `FE/qe/SessionVariable.java:1681`) splits
  DUP tablets across scanners (`ParallelScannerBuilder`, `olap_scan_operator.cpp:537-583`, min
  2M rows/scanner by default `:1698`), and several tablets/buckets run concurrently — order is
  not deterministic. Concatenation is a correct implementation.
- `read_orderby_key` is set iff `TOlapScanNode.sort_info` (9) is present — FE pushes a local TopN's
  sort onto the scan when it orders by a key prefix (`FE/nereids/glue/translator/PhysicalPlanTranslator.java:2710-2723`);
  `sort_limit` (10) = limit+offset. BE then merges by key (`tablet_reader.cpp:149-150`,
  `vcollect_iterator.cpp:84-96`, `_topn_next` `:259-`) and stops early
  (`olap_scanner.cpp:458-474`). **The SortNode stays in the plan** (`PhysicalPlanTranslator.java:2728`
  `addPlanRoot(sortNode)`), so ignoring `sort_info`/`sort_limit` and returning all rows is correct
  (just slower). Same for `topn_filter_source_node_ids` (TPlanNode 106) — the sort still applies.
- Within-segment row order is the memtable sort: key columns, NULLs first, equal keys by
  **descending input position** **[dump]** `all_types` v2: rows `(1,10,100)` come out
  `c_bigint 1001` then `1000` (inserted 1000 first).

---

## 4. Key ranges, predicates, conjuncts

| Thing | What C++ does | May doris-rs skip it? |
|---|---|---|
| `TPlanNode.conjuncts` | `_normalize_predicate` turns pushable ones into ColumnPredicates / key ranges; a conjunct fully pushed on a "key" column is **removed** from `_conjuncts` (for DUP every column counts as key: `_is_key_column` → `_storage_no_merge()`) (`SRC/exec/operator/scan_operator.cpp:316-335,467-475`, `olap_scan_operator.cpp:433-444,491-498`); the rest go to `common_expr_ctxs_push_down` or stay and are applied by `Scanner::_filter_output_block` (`SRC/exec/scan/scanner.cpp:164,184-186`) | **Yes**: the plan we receive still holds every conjunct. Evaluate all of them on the read block = same result. |
| key ranges (`key_column_name`/`key_column_type`, TOlapScanNode 2/3) | `_build_key_ranges_and_filters` builds scan keys from conjuncts on the key prefix (`olap_scan_operator.cpp:951-`); pushed to the reader as start/end keys (`olap_scanner.cpp:393-405`); segment seeks by short key + binary search (`segment_iterator.cpp:678,1708-1763`) | Yes (optimization). If implemented, must match KeyCoder semantics exactly or rows get lost. |
| column predicates / zone maps / bloom / inverted | row-range pruning (`segment_iterator.cpp:858,1028`) | Yes. |
| `push_down_agg_type_opt` (TPlanNode 48: NONE 0, MINMAX 1, COUNT 2, MIX 3, COUNT_ON_INDEX 4) | COUNT → emit `num_rows` default rows; MINMAX → 2 rows/segment from zone maps (`SRC/storage/iterator/vgeneric_iterators.cpp:57-96`, `SRC/storage/segment/segment.cpp:466-476`); the Agg node above still aggregates | Yes: returning the real rows gives the same aggregate. |
| `is_preaggregation` (4) | AGG only | n/a for DUP |
| runtime filters (TPlanNode.runtime_filters) | extra pruning; join re-checks | Yes (optimization; must not block waiting forever — treat as absent). |
| `sort_info` / `sort_limit` | §3 | Yes. |
| TPlanNode `limit` on the scan | stop after `limit` rows | exec-level; honoring it is optional for correctness only if a limit is re-applied above — **honor it** (cheap). |
| **Delete predicates** (`RowsetMetaPB.delete_predicate`, field 16, `PROTO/olap_file.proto:103`) | `TabletReadSource::fill_delete_predicates` collects every captured rowset that has one (`SRC/storage/tablet/base_tablet.cpp:2338-2346`); `DeleteHandler::init` keeps those with `version.first ≤ v` and `filter_version = version.first` (`SRC/storage/delete/delete_handler.cpp:556-578`); applied to every rowset with `end_version < filter_version` (`:628-660`, called from `beta_rowset_reader.cpp:130-133`) | **No — mandatory.** A `DELETE FROM t WHERE p` commits an (empty) rowset carrying `p`; rows of earlier rowsets matching `p` must vanish. Out of M5 scope ⇒ **fail the scan** if any captured rowset has `delete_predicate`. Not present in the fixture (0/64 rowsets). |
| MoW delete bitmap | §1.4 | Mandatory for MoW; out of scope ⇒ refuse UNIQUE MoW. |

Schema/column mapping (`olap_scanner.cpp:216-233,548-610`): if `TOlapScanNode.columns_desc` (8) is
set with `col_unique_id ≥ 0`, the scan schema is **rebuilt from it** (not the local tablet meta);
each output slot resolves by `col_unique_id` (else name); segment columns are found by unique id.
Slot nullable + column not null → wrap as nullable; slot NOT NULL + column nullable → error
`INVALID_SCHEMA`. A column absent from a segment's footer (added by schema change) reads as its
default value / NULL (`SRC/storage/segment/segment.cpp:778-790,844-860`) — needed once ALTER ADD
COLUMN is in scope.

---

## 5. Top-n lazy materialization (MATERIALIZATION_NODE)

### 5.1 When the FE plans it
- `topn_lazy_materialization_threshold` int, **default 1024** (`FE/qe/SessionVariable.java:1860-1863`);
  enabled iff > 0 (`:1877-1879`) and query `LIMIT ≤ threshold`
  (`FE/nereids/processor/post/materialize/LazyMaterializeTopN.java:82-85`). Not planned for tables
  with `light_schema_change=false` (`:146-156`).
- With it off, the **v1 two-phase read** may take over: `enable_two_phase_read_opt` (default true,
  `SessionVariable.java:2360`) and `limit < topn_opt_limit_threshold` (default 1024, `:2362`)
  (`FE/nereids/rules/rewrite/DeferMaterializeTopNResult.java:68,270-272,327-335`). To avoid both: set
  **`topn_lazy_materialization_threshold = 0` and `enable_two_phase_read_opt = false`** (global vars
  on the FE).

### 5.2 Row id column (v2)
- Name prefix `__DORIS_GLOBAL_ROWID_COL__` + table name (`FE/catalog/Column.java:67`,
  `LazyMaterializeTopN.java:159-161`; BE `SRC/common/consts.h:28`, matched by `starts_with`,
  `segment_iterator.cpp:1516`). Type STRING, NOT NULL, `col_unique_id = Integer.MAX_VALUE`; sent
  in `TOlapScanNode.columns_desc` (`FE/planner/OlapScanNode.java:1292-1298`).
- Value = 24 bytes, `GlobalRowLoacationV2` memcpy'd, little-endian (`SRC/storage/utils.h:279-324`):

| off | size | field |
|---|---|---|
| 0 | u8 | version = 0 (`FILE_LOCAL_ROW_ID`; 1 = Lance) (`utils.h:244-249`, `id_manager.h:240`) |
| 1..7 | 7 | zero |
| 8 | i64 | backend_id |
| 16 | u32 | file_id = index into the per-query `IdFileMap` |
| 20 | u32 | row_id = ordinal within the segment |

- `file_id = IdFileMap::get_file_mapping_id(FileMapping(tablet_id, rowset_id, segment_id))`, dense
  from 0 per query per BE (`SRC/storage/id_manager.h:52-68,169-182,222`); filled by
  `RowIdColumnIteratorV2` (`SRC/storage/segment/column_reader.cpp:2229-2253`,
  `segment_iterator.cpp:1516-1522`). Map created at scan open when an output slot has the prefix
  (`scan_operator.cpp:183-187`), expiry = now + `execution_timeout`; scanner pins every captured
  rowset with `add_temp_rowset` (`olap_scanner.cpp:500-505`).

### 5.3 Re-fetch
- `TPlanNodeType.MATERIALIZATION_NODE = 34`, `TPlanNode.materialization_node` (38);
  `TMaterializationNode {1 tuple_id, 2 intermediate_tuple_id, 3 nodes_info, 4 fetch_expr_lists,
  5 column_descs_lists, 6 slot_locs_lists, 7 fetch_row_stores, 8 gc_id_map, 9 column_idxs_lists}`
  (`THRIFT/PlanNodes.thrift:63,1340-1359,1756`).
- `MaterializationOperator` groups rowids by backend_id, calls
  **`PBackendService.multiget_data_v2(PMultiGetRequestV2)`** on every node in `nodes_info`
  (`SRC/exec/operator/materialization_opertor.cpp:304-410,486-549,591-610`; `PROTO/internal_service.proto:839-893,1271`).
  Request: `request_block_descs[{fetch_row_store, slots, column_descs, file_id[], row_id[], desc,
  column_idxs, row_location_version}]`, `be_exec_version`, `query_id`, `gc_id_map`, `wg_id`.
  Response: `status`, `blocks[{PBlock (LZ4), format, binary_row_data, profile}]`.
- Handler `PInternalService::multiget_data_v2` → `RowIdStorageReader::read_by_rowids`
  (`SRC/service/internal_service.cpp:2258-2303`, `SRC/exec/rowid_fetcher.cpp:536-750`): unknown
  query → one empty block per desc + OK; resolves file_id → (tablet, rowset, segment) → reads by
  ordinal; `gc_id_map` → drop the map.
- **Not RPC-free on one BE**: stubs come from `brpc_internal_client_cache()->get_client(host, port)`
  for every node, no local short-circuit (`materialization_opertor.cpp:591-610`,
  `SRC/util/brpc_client_cache.h:169-229`). FE lists all alive BEs in `nodes_info`
  (`FE/planner/MaterializationNode.java:138-151`). A doris-rs BE that runs this node must also
  serve `multiget_data_v2` on its own brpc port (and our exec engine may short-circuit locally,
  since it is the only consumer).
- v1 (two-phase read): column `__DORIS_ROWID_COL__`, `TResultSink.fetch_option`, RPC
  `multiget_data` (`consts.h:27`, `rowid_fetcher.cpp:242-252`).

---

## 6. Fixture check (`FIX`, 17 segments, 4 tables, all written by the C++ BE)

All 17 segments decode with today's `SEG` reader, **0 errors**, values correct (e.g. all_types v2:
LARGEINT ±2^127 bounds, DECIMAL128 38/10, DATEV2 0001-01-01..9999-12-31, CHAR zero-pad stripped).

| Column type (FieldType) | encoding | compression in meta | pages | null map | other |
|---|---|---|---|---|---|
| TINYINT(1) SMALLINT(3) INT(5) BIGINT(7) LARGEINT(9) | BIT_SHUFFLE (6) | NO_COMPRESSION (2) | 1 | when page has NULL | |
| BOOLEAN(24) | RLE (4) | 2 | 1 | yes | |
| FLOAT(10) DOUBLE(11) | BIT_SHUFFLE | 2 | 1 | | |
| DECIMAL32(31) 9/2, DECIMAL64(32) 18/4, DECIMAL128I(33) 38/10 | BIT_SHUFFLE | 2 | 1 | | precision/frac in meta |
| DATEV2(28) | BIT_SHUFFLE | 2 | 1 | | |
| DATETIMEV2(29) scale 3 | BIT_SHUFFLE | 2 | 1 | | `precision 18, frac 3` |
| CHAR(13) len 8, VARCHAR(17) len 34/18/66, STRING(26) len 2147483647 | DICT_ENCODING (5), dict page present, data pages mode DICT | 2 | 1 | | VARCHAR meta length = N+2 |

- Every column: `indexes = [ORDINAL_INDEX, ZONE_MAP_INDEX]`, ordinal root **is the data page**
  (no index pages anywhere — largest segment is 3020 rows / 123 KB).
- Footer `version` unset (V2), short key page present, no PK index; tablet schema
  `compression_type ZSTD`, `storage_format V2`, `num_short_key_columns 3` (all_types).
- **No compressed page, no PLAIN fallback page, no multi-page column exists in the fixture**:
  every flush was < 256 KB ⇒ `NO_COMPRESSION`. The ZSTD/LZ4F, dict→plain and ordinal-index-page
  paths are covered only by the older golden corpus (`oracle/corpus`).
- Rowsets: 17 NONOVERLAPPING with 1 segment, 47 with 0 segments (`[0-1]` + empty loads),
  none with `delete_predicate`, all VISIBLE.
- Multi-rowset tablet `1793889101813` (all_types): versions 2 (6 rows), 3 (3), 4 (2) — the
  CSV/JSON stream loads; a scan at v=4 must union all three.

---

## What M5 must build (storage side)

1. Version path: greedy widest-edge walk over rowset metas (visible, then stale) for `[0, v]`;
   error like `-230` when no path.
2. Refuse (clear error) rowsets with `delete_predicate`, UNIQUE/AGG tablets, V3/external-meta
   footers and anything §2 lists as unsupported — never silently return wrong rows.
3. Per rowset: segments `<tablet_path>/<rowset_id>_<i>.dat` for i in 0..num_segments, in order;
   per segment: columns by unique id from `columns_desc`; full scan; concatenate.
4. Evaluate every conjunct after reading; honor scan `limit`; ignore sort_info, key ranges, push-agg,
   runtime/topn filters.
5. Reader gaps from §2.5; expect ZSTD pages after the C++ BE compacts a doris-rs root.
6. Lazy materialization: either keep it off via FE session globals, or implement 24-byte row ids +
   per-query file map + `multiget_data_v2`.
