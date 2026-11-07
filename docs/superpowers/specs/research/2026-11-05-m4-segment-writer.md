# M4 research: writing segment v2 files the C++ 4.1.5 BE reads back

Paths: `SRC` = `.cache/doris-src/be/src` (in 4.1.5 the old `olap/rowset/segment_v2/` tree now lives
under `SRC/storage/segment/`, the indexes under `SRC/storage/index/`). `SEG` = `crates/doris-segment`.
Facts marked **[dump]** come from decoding the committed corpus with a scratch binary built against
`doris-segment` (scratchpad `dump/`, not part of the repo).

---

## 1. What the doris-segment reader supports today

| Area | Support | Where |
|---|---|---|
| Segment trailer | `footer PB | len u32 LE | crc32c(footer) u32 LE | "D0R1"`, crc checked, prost decode | `SEG/src/footer.rs:1-38` |
| Page framing | `body | PageFooterPB | footer_size u32 | crc32c(all before) u32`; "compressed" detected as stored body len != `uncompressed_size` | `SEG/src/page.rs:1-63` |
| Codecs | LZ4F (lz4_flex frame), ZSTD; NO_COMPRESSION only for uncompressed bodies; others error | `SEG/src/page.rs:85-103` |
| Ordinal index | root data page, or one index page of `(varint32 keylen, BE u64 ordinal, varint64 offset, varint32 size)` | `SEG/src/ordinal.rs:1-44` |
| Null map | per data page, RLE bit_width 1 trailing the values, `nullmap_size` from DataPageFooterPB; values stored only for non-null rows | `SEG/src/column.rs:136-186`, `SEG/src/rle.rs` |
| BIT_SHUFFLE | all fixed-width types (bshuf+LZ4 decode, 16-byte header) | `SEG/src/bitshuffle.rs`, `SEG/src/column.rs:78-118,198` |
| RLE | BOOL (verified); TINYINT..BIGINT (unverified, no 4.1.5 writer emits) | `SEG/src/column.rs:199-233` |
| DICT_ENCODING | CHAR/VARCHAR/STRING: dict page (PLAIN V1 words) + data pages in DICT (bit-shuffled i32 codes) or PLAIN fallback mode | `SEG/src/binary.rs` |
| PLAIN fixed-width | **not supported** (falls into `UnsupportedEncoding`) | `SEG/src/column.rs:234` |
| PLAIN binary column (meta encoding = PLAIN) | **not supported**: `read_binary_column` requires DICT (`SEG/src/binary.rs:~150`); but `decode_plain_v1` (BinaryPlainPage V1) exists and is tested | `SEG/src/binary.rs:31-70` |
| PLAIN_V2 / V3 format / external col meta | not supported | `SEG/src/binary.rs:7` |
| Types | BOOL, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL32/64/128I, legacy DECIMAL(V2, 12 B), DATEV2, DATE(v1, 3 B), DATETIMEV2, DATETIME(v1), IPV4, IPV6, CHAR/VARCHAR/STRING | `SEG/src/column.rs:20-58,275-347`, `SEG/src/values.rs` |
| Zone map / short key / bloom / PK index | not read at all (reader ignores them) | — |

Golden corpus (`oracle/corpus/README.md:28-42`, `tables/*.sql`): DUPLICATE KEY(k) tables, V2 storage
format, BOOL(RLE), ints (bitshuffle), FLOAT/DOUBLE/DECIMAL64/DATEV2/DATETIMEV2(6), CHAR/VARCHAR/STRING
dict incl. dict→plain fallback, LZ4F/ZSTD-compressed and uncompressed pages, multi-page columns via
ordinal index pages, DECIMAL32/128I, DATETIME(0/3), IPv4/6, legacy DATE/DATETIME/DECIMALV2. Not
covered: zone map/bloom/inverted index semantics (they are present in the files but unread), V3,
complex types, UNIQUE/AGG.

How tests use it: `tests/common/mod.rs` loads `expected/<table>.tsv` (`expected_rows`), decodes every
segment of the table via `read_column` per unique id, renders values like `mysql --batch --raw`
(`cells`, float compare by bits) and `assert_table_matches(table, &[(uid, name)])`
(`tests/common/mod.rs:285`). It also has from-scratch builders `data_page(...)` and a segment builder
(`tests/common/mod.rs:291-330`) used for negative tests. `footer_corpus.rs` checks every footer parses
and rows sum to `manifest.json` rows.

**[dump] Observed reference output** (`scalar_others`, rowset `...e122..._0.dat`, 500 rows, 8137 B):
- `SegmentFooterPB`: `version` unset (V2), `num_rows=500`, `short_key_index_page={6816,34}`, no PK
  index, all other footer fields unset.
- Every column: `column_id, unique_id, type, length, encoding, compression, is_nullable,
  default_value="", precision, frac, num_rows, be_exec_version=-1, result_is_nullable=false,
  function_name="NONE"`; indexes = `[ORDINAL_INDEX, ZONE_MAP_INDEX]` exactly.
- encodings: INT/FLOAT/DOUBLE/DECIMAL64/DATEV2/DATETIMEV2 = 6 (BIT_SHUFFLE); CHAR/VARCHAR/STRING = 5
  (DICT, with `dict_page`); BOOL = 4 (RLE) in `lz4f_big`.
- `compression = 2 (NO_COMPRESSION)` on this ZSTD table, because the flush was ≤ 256 KB (see §2.2).
- Ordinal index: `is_root_data_page=true` for single-page columns; `lz4f_big` seg 0 (73,440 rows)
  has 5 INT pages (≈16,384 values/64 KB page) behind an index page.
- Zone map: segment zone map strings like `min="1000" max="1499"` (INT), `"0"/"124.75"` (FLOAT),
  `"2024-01-01"` (DATEV2), `"2024-01-01 00:00:00.123456"` (DATETIMEV2(6)), DECIMAL64 as unscaled
  integer text, CHAR(8) **zero-padded** (`"c0\0\0\0\0\0\0"`); `page_zone_maps` = IndexedColumnMetaPB
  `{data_type 25, PLAIN, num_values = #data pages, ordinal root data page, NO_COMPRESSION}`.
- Short key page: footer `{SHORT_KEY_PAGE, uncompressed 6, num_items 1, key_bytes 5, offset_bytes 1,
  segment_id 0, num_rows_per_block 1024, num_segment_rows 500}`, body `02 80 00 03 E8 | 00`
  (= KEY_NORMAL_MARKER + sign-flipped big-endian INT 1000, then varint offset 0).
- File layout order: all data pages column by column (dict page right after each column's data
  pages), then ordinal index pages, then zone-map pages, then the short key page, then the footer.
  No file header and no magic at the start.

---

## 2. What the C++ writer emits for a DUPLICATE KEY table (defaults)

### 2.1 Writer selection
- Load path: `SegmentFlusher::flush_single_block` uses `VerticalSegmentWriter` when
  `config::enable_vertical_segment_writer` (default `true`, `SRC/common/config.cpp:475`)
  — `SRC/storage/rowset/segment_creator.cpp:67-86`.

### 2.2 Compression
- FE default: `Config.default_compression_type = "ZSTD"` (`fe/fe-common/.../Config.java:1973`),
  applied when the `compression` property is absent (`PropertyAnalyzer.java:1176-1189`). The corpus
  confirms: `scalar_ints` (no property) has `compression_type: ZSTD` in `tablet_meta.json`
  (the comment in `oracle/corpus/tables/01_scalar_ints.sql:1` saying "default (LZ4F)" is wrong).
- BE: `VerticalSegmentWriter::init` takes `_tablet_schema->compression_type()` unless the caller set
  one (`vertical_segment_writer.cpp:330-332`); `SegmentFlusher` forces `NO_COMPRESSION` when the flush
  block is ≤ `segment_compression_threshold_kb` = 256 KB (`segment_creator.cpp:73,142-143`,
  `config.cpp:1005`). Column meta records the codec (`vertical_segment_writer.cpp:173`).
- Pages are stored compressed only if compression saves ≥ 10 % (`compression_min_space_saving = 0.1`,
  `column_writer.h:78`; `page_io.cpp:53-73`); otherwise the raw body is stored and the reader sees
  stored len == uncompressed_size. Ordinal index pages, zone map pages and the short key page are
  always uncompressed (`ordinal_page_index.cpp:64`, `zone_map_index.cpp:328`,
  `vertical_segment_writer.cpp:1458-1460`).

### 2.3 Encodings (V2 storage format defaults, `encoding_info.cpp:320-351`)
| Type | Default | Also registered (readable) |
|---|---|---|
| TINYINT, SMALLINT, INT, BIGINT, LARGEINT | BIT_SHUFFLE | PLAIN, FOR (`:225-239`) |
| FLOAT, DOUBLE | BIT_SHUFFLE | PLAIN (`:246-249`) |
| CHAR, VARCHAR, STRING | DICT_ENCODING | PLAIN, PLAIN_V2, PREFIX (`:252-263`) |
| BOOL | RLE | BIT_SHUFFLE, PLAIN (`:274-276`) |
| DATE, DATEV2, DATETIME, DATETIMEV2 | BIT_SHUFFLE | PLAIN, FOR (`:279-290`) |
| DECIMAL(V2), DECIMAL32/64/128I | BIT_SHUFFLE | PLAIN (`:296-305`) |
| IPV4, IPV6 | BIT_SHUFFLE | PLAIN (`:308-311`) |
- V3 defaults switch the integer types to PLAIN (`:354-358`), so the C++ PLAIN fixed-width reader is
  a production path in 4.1.5, not dead code.
- Encoding chosen in `_init_column_meta` via `EncodingInfo::resolve_default_encoding`
  (`vertical_segment_writer.cpp:172`, `encoding_info.cpp:521-531`).

### 2.4 Pages
- `data_page_size` = tablet `storage_page_size` (default 65536; accepted range 4 KB..10 MB),
  `dict_page_size` = `storage_dict_page_size` (default 262144) — `vertical_segment_writer.cpp:278-283`,
  `options.h:27-28`. Corpus `tablet_meta.json` has `storage_page_size 65536`,
  `storage_dict_page_size 262144`.
- Data page body = `encoded values | [nullmap]`; nullmap only if the column is nullable **and** the
  page has a null; DataPageFooterPB `{first_ordinal, num_values (rows incl. nulls), nullmap_size}`;
  PageFooterPB `{type DATA_PAGE, uncompressed_size}` — `column_writer.cpp:817-880`.
- Nulls touch only the bitmap: `append_nulls` adds an RLE run, no value (`column_writer.cpp:583-588`).
- Null bitmap: `RleEncoder<bool>(buf, bit_width=1)`, 1 = NULL (`column_writer.cpp:55-62`).
- Page checksum: crc32c over body+footer+footer_size (`page_io.cpp:96-117`).

### 2.5 Indexes per column (`vertical_segment_writer.cpp:196-216`, `column_writer.cpp:489-530,734-805`)
- **Ordinal index**: always. 1 data page → `is_root_data_page=true`, pointer = that data page;
  else one INDEX_PAGE (leaf) of entries `put_length_prefixed_slice(BE u64 first_ordinal) +
  PagePointer::encode_to (varint64 offset, varint32 size)`, footer
  `IndexPageFooterPB{num_entries, type LEAF}` — `ordinal_page_index.cpp:40-72`, `index_page.cpp:30-52`.
- **Zone map**: `need_zone_map = column.is_key() || keys_type != AGG_KEYS` → every column of a DUP
  table (`vertical_segment_writer.cpp:205`). Per page a ZoneMapPB; pages with < 20 rows
  (`zone_map_row_num_threshold`, `config.cpp:935`) get `pass_all=true` (`column_writer.cpp:821-825`,
  `zone_map_index.cpp:259-262`). Segment zone map merged from pages (`zone_map_index.cpp:264-310`).
  `to_proto`: min/max = `serde->to_olap_string(v)` (empty when pass_all or no non-null), plus
  has_null/has_not_null/pass_all/has_positive_inf/has_negative_inf/has_nan (`zone_map_index.h:70-84`).
  Strings truncated to 512 B with last byte +1 on max (`zone_map_index.cpp:239-256`). Page zone maps
  are written as an IndexedColumn of type BITMAP(25), PLAIN (BinaryPlainPage V1 of serialized
  ZoneMapPB), NO_COMPRESSION, ordinal index only (`zone_map_index.cpp:313-338`,
  `indexed_column_writer.cpp:114-200`).
- **Bloom filter**: only for `bloom_filter_columns` / ngram index (`vertical_segment_writer.cpp:206-215`)
  → off by default.
- **Inverted / ANN**: only with index DDL → out of scope.
- **Primary key index**: only MoW (`vertical_segment_writer.cpp:337-355`) → out of scope.

### 2.6 Short key index (non-MoW tables, `vertical_segment_writer.cpp:353-355,1137-1238,1293-1312,1454-1461`)
- One entry every `num_rows_per_block` = 1024 rows (`tablet_meta.json`: `num_rows_per_row_block 1024`):
  rows 0, 1024, 2048, … `< num_rows` (`:1143-1148`).
- Entry = for each of the first `num_short_key_columns` key columns: NULL → `0x01`
  (KEY_NULL_FIRST_MARKER), else `0x02` + `KeyCoder::encode_ascending(value, index_length)`
  (`:1293-1310`; markers `SRC/common/consts.h:66-72`).
- KeyCoder (`SRC/storage/key_coder.h`): integers/decimals/LARGEINT = sign-bit-flipped big-endian
  (`:84-128`); DATEV2/DATETIMEV2 = big-endian unsigned, no flip (`:166-235`); CHAR = exactly
  `index_size` bytes of the (padded) value, CHECKs `index_size <= size` (`:298-318`); VARCHAR/STRING =
  first `min(index_size, len)` bytes (`:320-357`). `num_short_key_columns` and per-column
  `index_length` come from the tablet schema (FE-computed; corpus `num_short_key_columns: 1`).
- Page = `key_buf | varint32 offsets`, footer `ShortKeyFooterPB{num_items, key_bytes, offset_bytes,
  segment_id, num_rows_per_block, num_segment_rows}`, type SHORT_KEY_PAGE, uncompressed
  (`SRC/storage/index/short_key_index.cpp:17-41`); pointer in `SegmentFooterPB.short_key_index_page`.
- Segment min/max key (for rowset `segments_key_bounds`) = `_full_encode_keys` over all key columns
  of row 0 and row n-1 (`:1215-1220`).

### 2.7 Footer (`vertical_segment_writer.cpp:1470-1502`)
- `num_rows`, `columns[]` (ColumnMetaPB per column, `_init_column_meta` `:166-193`),
  `short_key_index_page`; `version` set only for V3 (`:1475-1481`).
- Trailer: `SegmentFooterPB bytes | u32 LE len | u32 LE crc32c(footer) | "D0R1"`; magic only at the
  end, no header (`:92-93,1489-1500`). Reader: `segment.cpp:578-650`.

### 2.8 Row order — confirmed sorted
- `MemTable::_sort` sorts rows by all key columns (`SRC/load/memtable/memtable.cpp:311-351`); for DUP
  ties are ordered by **descending** input position (`:323-342`) — later-inserted equal-key rows come
  first. Nullable key compare uses null_direction −1 (NULLs first).
- The reader relies on the sort: `_lookup_ordinal_from_sk_index` uses the short key blocks then
  binary-searches rows by key (`segment_iterator.cpp:1708-1763`). Writer also DCHECKs sortedness
  (`vertical_segment_writer.cpp:1218-1236`).

---

## 3. Required vs optional for the C++ reader

| Piece | Required? | Reader evidence |
|---|---|---|
| Trailer (len, crc32c, `D0R1`) | **Required**, crc verified | `segment.cpp:597-650` |
| `SegmentFooterPB.num_rows` | **Required** | `segment.cpp:367` |
| ColumnMetaPB `unique_id` | **Required** (columns resolved by uid; missing → default-value iterator / error for NOT NULL w/o default) | `segment.cpp:778-790,844-860`, `column_meta_accessor.cpp:60-65` |
| ColumnMetaPB `type`, `encoding` | **Required**; any *registered* (type, encoding) pair is accepted | `column_reader.cpp:350-360` |
| `compression` | Required for compressed pages; NO_COMPRESSION is valid (reference writes it for small flushes) | `column_reader.cpp:292,1800`; `page_io.cpp:203-209` |
| `is_nullable`, `length`, `precision/frac` | Set them like the reference (decimal precision/frac are back-filled from schema if missing: `segment.cpp:178-219`) | — |
| Ordinal index | **Required** for any non-empty column (`Corruption: missing ordinal index`) | `column_reader.cpp:392-395`; reader `ordinal_page_index.cpp:83-130` |
| Short key index page (DUP) | **Required**: `Segment::new_iterator` always calls `load_index`, which reads `_sk_index_page` and parses it (size must equal key_bytes+offset_bytes) | `segment.cpp:460-464,704-737`; `short_key_index.cpp:43-80` |
| Short key *contents* | **Must be correct**: key-range scans (`WHERE k = …`, prefix ranges) seek with it, then binary-search rows | `segment_iterator.cpp:700-712,1708-1763` |
| Zone map index | *Formally optional* (`has_zone_map()` checks everywhere: `segment.cpp:141,166,425`, `column_reader.cpp:472,486,631,2119`) **but** MIN/MAX pushdown (`TPushAggOp::MINMAX`, e.g. `SELECT MIN(c) FROM t`) builds `VStatisticsIterator` which calls `next_batch_of_zone_map` → `InternalError("segment zonemap not exist")` when absent. `segment_zone_maps_can_answer_agg` only falls back to a data scan when a zone map **exists with pass_all** (`segment.cpp:151-176,466-476`; `column_reader.cpp:446-465`; `storage/iterator/vgeneric_iterators.cpp:63-95`). ⇒ emit a zone map for every column. | |
| Zone map contents | `pass_all=true` is always safe: segment-level match returns true (`column_reader.cpp:508-512`), page filter keeps the page (`:526-531`), agg pushdown falls back to scanning (`segment.cpp:170-173`). If not pass_all, min/max must parse via `from_zonemap_string` (failure → pass_all, `zone_map_index.cpp:73-79`) and must be **true bounds**, else rows are wrongly pruned. FLOAT/DOUBLE without `has_nan` set → treated pass_all (`:84-94`). | |
| Page zone map count | Must equal number of data pages (index i ↔ ordinal page i) | `column_reader.cpp:517-560` |
| Dict page | Only if encoding = DICT and some data page is in DICT mode (read lazily on first such page) | `column_reader.cpp:2077-2113` |
| Bloom filter / inverted / PK index | Optional (absent by default) | `column_reader.cpp:362-386` |
| Footer `version`, footprints, `compress_type`, `file_meta_datas` | Optional; unset in reference V2 output **[dump]** | — |

**Can doris-rs use PLAIN for every type?** Yes for the C++ reader:
- Fixed-width PLAIN is registered for every needed type (`encoding_info.cpp:226-311`) →
  `PlainPageBuilder/Decoder` (`encoding_info.cpp:52-62`). Page = `u32 LE count | count × sizeof(CppType)
  LE values` and the decoder rejects any other size (`plain_page.h:34,70-83,112-127`). Its
  `next_batch`/`read_by_rowids` call `insert_many_fix_len_data` exactly like the bit-shuffle decoder
  (`plain_page.h:186-232` vs `bitshuffle_page.h:388-432`), so value semantics are identical
  (`column_vector.h:163-171`; only legacy DATE/DATETIME get conversion there).
- Binary PLAIN → `BinaryPlainPageBuilder/Decoder` V1 (`encoding_info.cpp:64-74`): `values | u32 LE
  offset per value | u32 LE count` (`binary_plain_page.h:97-108`). CHAR+PLAIN gets
  `BinaryPlainPageCharStripPreDecoder` (strips padding) (`encoding_info.cpp:452-457`).
- BOOL PLAIN registered (`encoding_info.cpp:276`); 1 byte per value.
- Caveat: doris-rs's own reader must learn PLAIN (fixed + binary column) for round-trip tests.
- Trade-off: PLAIN is bigger on disk and differs from the reference's choice; it does not change
  query results. `enable_low_cardinality_optimize` is DICT-only and just doesn't kick in.

---

## 4. Physical value layouts (storage CppType, little-endian in pages)

| SQL type | FieldType id (`SEG/src/column.rs:20-43`) | Stored bytes | Notes |
|---|---|---|---|
| BOOLEAN | 24 | 1 (0/1) | |
| TINYINT / SMALLINT / INT / BIGINT | 1 / 3 / 5 / 7 | 1 / 2 / 4 / 8, two's complement | |
| LARGEINT | 9 | 16, int128 | `types.h:117-119` |
| FLOAT / DOUBLE | 10 / 11 | IEEE 4 / 8 | cannot be key columns |
| DECIMAL32 / 64 / 128I | 31 / 32 / 33 | int32 / int64 / int128 unscaled | precision/frac in ColumnMetaPB; `types.h:147-149` |
| DATEV2 | 28 | u32 `day:5 | month:4 | year:23` from LSB | `SEG/src/values.rs:30-38`, `types.h:162-164` |
| DATETIMEV2(p) | 29 | u64 `usec:20 | sec:6 | min:6 | hour:5 | day:5 | month:4 | year:18` | scale in `frac`; `types.h:172-174`, `values.rs:39-52` |
| DATE (v1) / DATETIME (v1) | 14 / 15 | 3 B `y<<9|m<<5|d` / i64 `YYYYMMDDhhmmss` | FE 4.1.5 converts to V2 by default (`Config.java:2263,2770-2773`): skip in M4 |
| DECIMALV2 | 16 | 12 B (i64 int + i32 frac) | disabled by default: skip |
| CHAR(N) | 13 | value **zero-padded to N** bytes | `olap_data_convertor.cpp:540-565`; reader strips; zone map & short key use padded bytes |
| VARCHAR(N) / STRING | 17 / 26 | raw bytes | meta `length` copied from TabletColumn (corpus: VARCHAR(64) → 66, STRING → 2147483647) |

Zone-map text (`to_olap_string`, **[dump]**): ints decimal text; DECIMALV3 unscaled integer text;
DATEV2 `YYYY-MM-DD`; DATETIMEV2(6) `YYYY-MM-DD hh:mm:ss.ffffff`; floats shortest text (`124.75`);
CHAR padded raw bytes; VARCHAR/STRING raw bytes (≤512).

---

## 5. Rowset level

- Beta rowset files: `<tablet_path>/<rowset_id>_<seg>.dat` (`beta_rowset.cpp:330`); with no inverted
  index there are no `.idx` files (the corpus `data/<table>/` holds only `.dat` + `tablet_meta.json`).
  A zero-row load writes no segment (`segment_creator.cpp:69-71`; corpus `empty_table`).
- RowsetMetaPB fields observed for a DUP load **[dump of tablet_meta.json]**: `rowset_id_v2`,
  `rowset_type BETA_ROWSET`, `rowset_state VISIBLE`, `start/end_version`, `num_rows`,
  `total_disk_size = data_disk_size = Σ .dat sizes`, `index_disk_size 0`, `empty false`, `load_id`,
  `txn_id`, `creation_time`, `newest_write_timestamp`, `tablet_uid`, `num_segments`,
  `segments_key_bounds` (aggregated into one `{min_key,max_key}` with
  `segments_key_bounds_aggregated: true`), `segments_overlap_pb NONOVERLAPPING`,
  `segments_key_bounds_truncated false`, `num_segment_rows [..]`, `schema_version`,
  `has_variant_type_in_schema false`, embedded `tablet_schema`.
- `segments_file_size` (1001) / `enable_segments_file_size` (1004) absent in the corpus JSON; when
  disabled the BE uses −1 = stat the file (`rowset_meta.cpp:283-292`, `beta_rowset.cpp:290`).
  Safe to omit; if written it must be exact.
- `num_segment_rows`: if present its length must equal `num_segments`; with
  `enable_segment_rows_consistency_check` it is checked against footers; absent → read from footers
  (`beta_rowset.cpp:118-165`).
- `segments_key_bounds` = full-encoded keys of each segment's first/last row; used for rowset/segment
  pruning, so must be true bounds (`vertical_segment_writer.cpp:1215-1220`).
- Multiple segments in one rowset: NONOVERLAPPING only if key ranges don't overlap; one segment per
  load is simplest.

---

## 6. Suggested minimal writer and tests

### 6.1 Writer (V2 format, one segment per flush, rows pre-sorted)
1. Sort rows by key columns (NULLs first, bytewise strings, numeric order), ties by **descending**
   input position (memtable rule) — needed both for correctness (short-key seek) and for unordered
   reply equality.
2. Per column, in schema order: split into pages (≤ 64 KB of values, e.g. `65536 / size` fixed rows;
   binary pages close once `4 + Σ(len+4) > 65536`). Body = PLAIN values of non-null rows +
   RLE nullmap (only if the page has a NULL). Simplest RLE: only repeated runs
   (`varint(run_len << 1)`, one value byte); `rle.rs` decoder already accepts that.
3. Write each page as `body | PageFooterPB | len | crc32c`, uncompressed; set ColumnMetaPB
   `compression = NO_COMPRESSION` (or the tablet codec — uncompressed bodies are still readable).
4. Ordinal index: root data page if 1 page, else one LEAF index page.
5. Zone map: one ZoneMapPB per data page + segment ZoneMapPB. Phase A: all `pass_all=true`,
   `min=max=""`, has_null/has_not_null set truthfully, `has_nan` set (false) — always correct.
   Phase B (optional): real bounds per type using the reference text formats (fixed-width first).
   Page zone maps written as BinaryPlainPage V1 of serialized ZoneMapPB in one uncompressed data page
   (`IndexedColumnMetaPB{data_type 25, PLAIN, num_values, ordinal root data page, NO_COMPRESSION}`).
6. Short key page: entries at rows 0,1024,…; `num_short_key_columns` and `index_length` from the
   tablet schema sent by FE; NULL → `01`, else `02`+KeyCoder bytes; CHAR padded.
7. Footer: ColumnMetaPB as in §1 dump (column_id, unique_id, type, length, encoding, compression,
   is_nullable, default_value, precision, frac, num_rows, be_exec_version, result_is_nullable,
   function_name, indexes [ORDINAL, ZONE_MAP]); `num_rows`; `short_key_index_page`; trailer.
8. Return per segment: file size, num rows, min/max full-encoded keys → RowsetMetaPB.

Optional later: BIT_SHUFFLE (needs a bshuf+LZ4-block encoder, 16-byte header
`count | 16+bytes | padded_count | elem_size`, `bitshuffle_page.h:51,207-228`), RLE BOOL,
DICT strings, LZ4F/ZSTD bodies — to match reference bytes/size more closely.

### 6.2 Tests
1. **Unit round trip**: writer → `doris-segment` reader for every M4 type, nullable/non-null, empty
   strings, NULL runs, all-NULL pages, multi-page columns (ordinal index page), > 1024 rows
   (multi-entry short key). Requires adding PLAIN fixed + PLAIN binary column decode to the reader.
2. **Corpus re-encode**: for each corpus table (M4 types only), decode all segments with the reader,
   re-sort, write with doris-rs, decode again and run the existing `assert_table_matches`-style
   comparison against `expected/<table>.tsv`.
3. **Structural parity with the reference** (no new corpus needed): for the corpus rows, the
   short key page body + footer must be **byte-identical** to the reference's (deterministic given
   rows and schema); segment min/max keys equal `segments_key_bounds` in `tablet_meta.json`;
   ColumnMetaPB fields other than encoding/compression/indexes equal the reference footer;
   if Phase B zone maps: segment zone-map min/max strings equal the reference's.
4. **Reader-side checks of the new pieces**: add parsers for short key page and zone map indexed
   column to `doris-segment` (also useful for later key-range scans in doris-rs).
5. **Live swap check**: C++ BE on doris-rs's storage root; queries per table: `SELECT *` ordered and
   unordered, `WHERE k = c` / range on short key (exercises sk index + binary search), `WHERE v IS
   NULL`, `COUNT(*)`, `MIN/MAX(col)` (exercises zone map agg pushdown path), predicates on value
   columns (page zone map filtering). Optional pre-swap check: `meta_tool --operation=show_segment_data
   --file=<.dat>` (`SRC/tools/meta_tool.cpp:74,774,928-933`) if the release image ships it.

---

## What doris-rs must build

1. A segment writer crate/module (in `doris-segment` or a sibling) producing V2 segments:
   PLAIN pages for all M4 types, RLE null bitmaps, uncompressed page framing with crc32c,
   ordinal index (root page or one leaf index page), zone map index (pass_all first), short key
   index page, footer + `D0R1` trailer.
2. KeyCoder port (marker bytes, sign-flipped BE ints/decimals/LARGEINT, BE DATEV2/DATETIMEV2,
   CHAR padded/`index_length`, VARCHAR prefix) for short keys and full-encoded min/max keys.
3. Row sorter matching `MemTable::_sort` (key order, NULLs first, DUP ties by descending position).
4. Value conversion from INSERT/stream-load text to storage bytes: DATEV2/DATETIMEV2 bit packing,
   decimal unscaling with the column scale, CHAR zero padding to N, LARGEINT i128.
5. Reader extensions: PLAIN fixed-width, PLAIN binary columns, short key page and zone map parsing.
6. Per-segment outputs for RowsetMetaPB: file size, rows, key bounds; one segment per load.

## Open risks

- **MIN/MAX pushdown**: omitting zone maps makes `SELECT MIN/MAX` fail; a pass_all segment zone map
  avoids it (code path verified in source, not yet live).
- **Expr zone map** (`expr_zonemap::is_expr_zonemap_filter_enabled`, `segment.cpp:441-455`) reads
  segment zone maps through `build_segment_zonemap_context`; pass_all handling there was not traced
  to the end — confirm in the live check.
- **Equal-key row order**: replies without ORDER BY depend on the descending-position tie rule and on
  segment/rowset read order; the swap check should compare against the reference with that in mind.
- **num_short_key_columns / index_length** must be taken verbatim from FE's TCreateTabletReq schema;
  a mismatch breaks key-range seeks without errors.
- **CHAR**: must be zero-padded on disk (short-key KeyCoder CHECKs `index_size <= size`, a crash
  otherwise); CHAR values containing interior NULs are cut by the reader (corpus `strings_fallback`).
- **PLAIN for non-integer types** (FLOAT/DOUBLE/DATEV2/DATETIMEV2/DECIMAL/BOOL) is registered but
  is not a default in any 4.1.5 format, so less battle-tested in the C++ BE than BIT_SHUFFLE; falling
  back to BIT_SHUFFLE means writing a bshuf+LZ4 encoder.
- **Default compression is ZSTD**, not LZ4F; writing NO_COMPRESSION is valid but the C++ BE's own
  later compactions of doris-rs rowsets will rewrite with ZSTD — fine, just a size difference.
- `segments_file_size` absence was inferred from the JSON dump; if the 4.1.5 BE sets
  `enable_segments_file_size` and a stale value is written, opens fail — write it exactly or not at all.
- The test corpus has no zone-map/short-key semantic coverage; parity tests in §6.2 (3) are the only
  pre-swap evidence for those structures.
