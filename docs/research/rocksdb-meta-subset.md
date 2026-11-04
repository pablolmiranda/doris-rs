# The RocksDB format subset behind the BE's meta store

Research for issue #19 (child of map #14). Question: which parts of the
RocksDB 5.14.2 on-disk format does the C++ BE's `<root>/meta` store produce
and need, and which keys does the BE keep there? The answer scopes a
pure-Rust reader and writer with no RocksDB dependency.

Already decided: doris-rs reads anything a C++ BE writes there, and writes
what that RocksDB will open. Fixtures written by a real C++ BE and a swap
check prove it. We never link RocksDB.

**Sources.** Doris is the pinned tree at `.cache/doris-src`, commit
`917b20be83878e98f94e3ebf7efccc80346e3e50`. Doris paths below are relative
to it. RocksDB is the `v5.14.2` tarball, md5
`b72720ea3b1e9ca9e4ed0febfef65b14`, which matches `thirdparty/vars.sh:217`.
RocksDB paths are relative to
<https://github.com/facebook/rocksdb/tree/v5.14.2>. Line numbers are for
those exact trees.

**Not verified live.** No Doris container was running on 2026-11-04, so the
file listing in §3 comes from the source, not from a real `meta/` directory.
The first fixture task should confirm it.

---

## TL;DR

- **One DB per data dir, three column families, one of them used.** The BE
  opens `<root>/meta` with column families `default`, `doris` and `meta`.
  Every Doris key goes to `meta`, which has a 4-byte fixed-prefix extractor.
  `default` and `doris` exist but are never written
  (`be/src/storage/olap_meta.cpp:81-101`, `olap_define.h:129-148`).
- **No table or compaction options are set.** Doris sets nothing on the
  table, compression or compaction side, so stock 5.14.2 defaults apply:
  - block-based table, `format_version = 2`, `checksum = kCRC32c`;
  - 4 KiB blocks, restart interval 16, binary-search index, no filter
    policy;
  - Snappy compression for data and index blocks (Snappy is compiled in);
  - level compaction, 64 MiB write buffer, L0 trigger 4.
- **The only non-default column-family options** are on `meta`:
  `prefix_extractor = rocksdb.FixedPrefix.4` and
  `max_write_buffer_number = 5`. Neither changes the bytes of an SST.
  Both are recorded only as metadata: the SST properties and the OPTIONS
  file.
- **The write path is plain.** Writes are only `Put` and `Delete` (single or
  batched) into one column family. There is no Merge, SingleDelete,
  DeleteRange, 2PC or blob. WAL `sync` is off by default.
- **Ten key families**, all ASCII-prefixed with protobuf values except where
  noted (§5). The ones that matter first are `tabletmeta_<tablet_id>_<schema_hash>`
  → `TabletMetaPB` and `rst_<tablet_uid>_<rowset_id>` → `RowsetMetaPB`.
  Then `ppi_`, `dlb_` (binary big-endian key), `pui_`, `binlog_meta_`,
  `binlog_data_`, `gc_` and `tgc_`. `hdr_` is only scanned, to reject the
  old format.
- **Scope.**
  - A reader needs five pieces: the log-record reader (shared by WAL and
    MANIFEST), WriteBatch decoding, VersionEdit replay, a block-based SST
    reader (Snappy + CRC32C), and a merging iterator over the memtable and
    the levels.
  - A writer needs the same encoders plus one rule: always produce a
    consistent CURRENT → MANIFEST → {SST, WAL} set.
  - Rough size: reader about 2.5k LOC, writer about 2k more, plus fixtures.
    No pure-Rust crate can be adopted as is (§8).
- **Biggest risks** (§9):
  1. Snappy is a build-time accident of header detection, not a Doris
     choice.
  2. MANIFEST invariants that 5.14.2 enforces on open: log numbers,
     next-file and last-sequence, and listing every column family.
  3. Values far larger than a block or a WAL record.
  4. Fixed column-family ids are assumed, not guaranteed.

---

## 1. How Doris opens the store

`OlapMeta::init()` (`be/src/storage/olap_meta.cpp:76-121`):

| Setting | Value | Source |
|---|---|---|
| path | `<data_dir>/meta` (`META_POSTFIX = "/meta"`) | `olap_meta.cpp:57,86` |
| `create_if_missing`, `create_missing_column_families` | true | `:79-80` |
| `IncreaseParallelism()` | thread pools only; no format effect | `:78`; RocksDB `options/options.cc:511-516` |
| `info_log` | a custom `RocksdbLogger` that sends output to glog at WARN | `:81-82` |
| column family 0 | `"default"`, `ColumnFamilyOptions()` | `:89-90` |
| column family `"doris"` | `ColumnFamilyOptions()` | `:91` |
| column family `"meta"` | `max_write_buffer_number = config::rocksdb_max_write_buffer_number` (default 5, `common/config.cpp:1426`); `prefix_extractor = NewFixedPrefixTransform(4)` | `:94-97` |
| `WriteOptions.sync` | `config::sync_tablet_meta`, default **false** (`config.cpp:788`) | `:162,187,216,233,256` |
| close | `SyncWAL()`, `CancelAllBackgroundWork(wait=true)`, `Close()` | `:103-116` |

Because `info_log` is supplied, RocksDB never creates its own `LOG` or
`LOG.old.*` files. It only creates them when `info_log == nullptr`
(`db/db_impl_open.cc:46-47`).

`ColumnFamilyIndex` is `{DEFAULT=0, DORIS=1, META=2}` (`olap_define.h:129-133`).
That is the index into Doris's handle vector, **not** the RocksDB column
family id. On a fresh DB, `create_missing_column_families` creates the
missing families in descriptor order (`db/db_impl_open.cc:1106-1110`), so in
practice `doris` = 1 and `meta` = 2. A reader must still resolve families
**by name from the MANIFEST**.

Every call to `COLUMN_FAMILY_INDEX` in the BE passes `META_COLUMN_FAMILY_INDEX`
(see the grep list in §5). `DORIS_COLUMN_FAMILY_INDEX` is never used outside
its definition. The only `DEFAULT_COLUMN_FAMILY_INDEX` user is
`load/stream_load/stream_load_recorder.cpp`. That is a **separate** DB at
`<root>/stream_load`, opened with `DBWithTTL` (`:35,57-71`), and is out of
scope here.

## 2. How RocksDB is built in Doris, and which codecs that enables

- Version: `ROCKSDB_DOWNLOAD=.../v5.14.2.tar.gz` (`thirdparty/vars.sh:213-217`).
- Build: `PORTABLE=1 make USE_RTTI=1 static_lib` with
  `CFLAGS="-I ${TP_INCLUDE_DIR} -I .../snappy -I .../lz4"`
  (`thirdparty/build-thirdparty.sh:924-948`). No `ROCKSDB_DISABLE_*` flags
  are set.
- Patches (`thirdparty/patches/rocksdb-5.14.2.patch`, `rocksdb-mac-compile-fix.patch`):
  - a `mutable` mutex;
  - reordered structs in `metadata.h`;
  - a macOS compile fix.
  
  None of these touch the format.
- Codec detection: RocksDB's `build_tools/build_detect_platform:243-333`
  enables `-DSNAPPY`, `-DZLIB`, `-DBZIP2`, `-DLZ4` and `-DZSTD` whenever
  `$CXX $CFLAGS` can compile a stub that includes the header. Doris builds
  snappy 1.1.10, zlib 1.3.1, lz4 1.9.4, bzip2 1.0.8 and zstd 1.5.7 **before**
  rocksdb (package order in `build-thirdparty.sh` ~2359-2394; names in
  `vars.sh:103-134,233`). All five headers are therefore in
  `TP_INCLUDE_DIR`, so all five codecs are compiled in.
- What gets **written**: Doris never sets `compression`. The default is
  `Snappy_Supported() ? kSnappyCompression : kNoCompression`
  (`options/options.cc:98`). Data blocks and index blocks are
  Snappy-compressed (`enable_index_compression = true`, `include/rocksdb/table.h:224`;
  `table/block_based_table_builder.cc:803-806`). Two exceptions:
  - A block is stored raw (`kNoCompression`) when Snappy does not save at
    least 12.5% (`GoodCompressionRatio`, `block_based_table_builder.cc:96-122`).
  - Meta blocks (properties, metaindex, range-del) are always raw
    (`:783-793`).

  So the reader must handle **type 0 (none) and type 1 (Snappy)**. The
  other four codecs are compiled in but never chosen by Doris's settings.

## 3. Files in `<root>/meta`

From `db/filename.cc` in 5.14.2. All numbers are zero-padded to 6 digits.

| File | Who needs it | Notes |
|---|---|---|
| `CURRENT` | read + write | One line: `MANIFEST-NNNNNN\n`. Written to `NNNNNN.dbtmp`, then renamed. |
| `MANIFEST-NNNNNN` | read + write | Log-format file of VersionEdits (§4.2). RocksDB starts a **new** manifest on every open: `new_descriptor_log` (`db/version_set.cc:2808-2896`). |
| `NNNNNN.log` | read + write | WAL (§4.1). |
| `NNNNNN.sst` | read + write | Block-based table (§4.3). |
| `OPTIONS-NNNNNN` | write optional | INI dump of options. 5.14.2 `DB::Open` does not read it (`fail_if_options_file_error = false`, `include/rocksdb/options.h:869`), and rewrites it on every open. Readers can ignore it. Writers can skip it, or copy a template from a fixture. |
| `IDENTITY` | optional | DB UUID text. Created on open if missing (`db/db_impl_open.cc:343-346`). |
| `LOCK` | write | `flock` target. Must exist and be lockable. |
| `LOG*` | — | Not produced (see §1). |

## 4. The format pieces

### 4.1 WAL (`NNNNNN.log`)

- **Log record format** (`db/log_format.h`):
  - 32 KiB blocks (`kBlockSize = 32768`, `:35`).
  - Record header: `masked_crc32c(4) | len(2, LE) | type(1)` (`kHeaderSize = 7`, `:38`).
  - Types: `kFullType = 1`, `kFirstType = 2`, `kMiddleType = 3`, `kLastType = 4`.
  - The recyclable types 5-8 have an 11-byte header with a log number. They
    appear only when `recycle_log_file_num > 0`, and the default is 0
    (`options.h:557`), so Doris never produces them. A reader can still
    accept them cheaply.
  - A block tail shorter than 7 bytes is zero padding.
  - The CRC covers the type byte plus the payload.
  - Masking is `((crc >> 15) | (crc << 17)) + 0xa282ead8` (`util/crc32c.h:30-39`).
- **Payload = WriteBatch**:
  - Header: `sequence(fixed64) | count(fixed32)` (`kHeader = 12`,
    `db/write_batch_internal.h:68`). Then the records.
  - For the default family, a record is `tag | key | value`.
  - For any other family it is `tag | varint32 cf_id | key | value`, with
    length-prefixed slices.
  - Tags Doris produces, from `db/dbformat.h:35-52`:
    - `0x5 kTypeColumnFamilyValue`
    - `0x4 kTypeColumnFamilyDeletion`
    - only for CF 0, which Doris never writes: `0x1 kTypeValue` and `0x0 kTypeDeletion`.
  - A writer emits only those. A reader should reject the rest (Merge,
    SingleDelete, RangeDeletion, the 2PC markers `0x9-0xC`/`0x12`, Noop and
    blob) with a clear error rather than misread them.
- **Durability**: `sync = false` (§1), so a crashed BE can leave a torn
  tail. Match 5.14.2's default `kPointInTimeRecovery` (`options.h:844`):
  stop at the first corrupt or short record. Do not fail.
- **Large values**: a `TabletMetaPB` can be large.
  `tablet_meta_serialize_size_limit` defaults to 1.5 GiB
  (`config.cpp:1766`). A 32 KiB block means such a value spans many
  First/Middle/Last fragments, and both reader and writer must fragment
  correctly.

### 4.2 MANIFEST and VersionEdit

The MANIFEST uses the same log-record framing as the WAL. Each record is one
encoded `VersionEdit`, encoded as `varint32 tag` followed by its fields
(`db/version_edit.cc:23-55,88-185`).

| Tag | Field | Encoding |
|---|---|---|
| 1 `kComparator` | `"leveldb.BytewiseComparator"` | length-prefixed |
| 2 `kLogNumber` | min WAL number still needed by this CF | varint64 |
| 3 `kNextFileNumber` | | varint64 |
| 4 `kLastSequence` | | varint64 |
| 6 `kDeletedFile` | level, file number | varint32, varint64 |
| 9 `kPrevLogNumber` | | varint64 |
| 10 `kMinLogNumberToKeep` | (2PC only) | varint64 |
| 100 `kNewFile2` | level, number, size, smallest ikey, largest ikey, smallest_seq, largest_seq | path_id = 0 case |
| 102 `kNewFile3` | as above plus a path_id | not produced (single `db_path`) |
| 103 `kNewFile4` | as above plus custom-tag fields (`kNeedCompaction = 2`, `kMinLogNumberToKeepHack = 3`, `kPathId = 65`, `kTerminate = 1`) | written when a file is marked for compaction. The reader must parse it; the writer can avoid it |
| 200 `kColumnFamily` | CF id the edit applies to | varint32 |
| 201 `kColumnFamilyAdd` | CF name | length-prefixed |
| 202 `kColumnFamilyDrop` | — | |
| 203 `kMaxColumnFamily` | | varint32 |

A new DB starts with `MANIFEST-000001` holding comparator, `log_number = 0`,
`next_file = 2` and `last_sequence = 0` (`db/db_impl_open.cc:213-223`).
Edits that add the `doris` and `meta` families follow.

**Invariants that 5.14.2's `VersionSet::Recover` enforces on open**
(`db/version_set.cc:3236-3298`). A Rust writer must satisfy all of them:

- The comparator name matches.
- Next-file, log-number and last-sequence entries are all present.
- **Every column family in the MANIFEST is listed in the open call.** This
  one matters most: a writer must not add a family Doris does not open, or
  the BE fails with "You have to open all column families".
- `last_sequence` is at least every sequence number in the SSTs.
- A WAL with a number below a column family's `log_number` is skipped for
  that family during recovery.

### 4.3 SST: block-based table, `format_version = 2`

Defaults, since Doris passes no `BlockBasedTableOptions` (`include/rocksdb/table.h`):

| Option | Value | Line |
|---|---|---|
| `checksum` | `kCRC32c` | 103 |
| `block_size` | 4096 | 126 |
| `block_restart_interval` | 16 | 139 |
| `index_type` | `kBinarySearch` | 94 |
| `index_block_restart_interval` | 1 | 142 |
| `filter_policy` | nullptr | — |
| `whole_key_filtering` | true (no effect without a filter) | 175 |
| `format_version` | **2** | 219 |
| `enable_index_compression` | true | 224 |

**Layout:**

1. Data blocks, then the index block. Each block is followed by a 5-byte
   trailer, `compression_type(1) | masked_crc32c(4)`, with the CRC taken
   over the block plus the type byte (`table/format.h:181`).
2. Meta blocks, in this order (`block_based_table_builder.cc:691-793`):
   - filter: never, since there is no filter policy;
   - `rocksdb.properties`: always, raw;
   - compression dictionary: never, since `max_dict_bytes = 0`;
   - `rocksdb.range_del`: only if non-empty, and Doris never uses
     DeleteRange.

   The metaindex block maps those names to block handles.
3. Footer, 53 bytes for version ≥ 1 (`table/format.cc:111-134`): the
   checksum type byte, the metaindex handle, the index handle (each a pair
   of varint64), zero padding to 41 bytes, `version` as fixed32 (= 2), and
   the magic `0x88e241b785f4cff7` as fixed64 (`block_based_table_builder.cc:191`).
   The legacy 48-byte LevelDB footer, magic `0xdb4775248b80fb57`, is
   readable by RocksDB but never written by it at these settings.

**Block format** (same as LevelDB):

- Each entry is `varint32 shared | varint32 non_shared | varint32 value_len | key_delta | value`.
- Then `restarts[fixed32]` and `num_restarts(fixed32)`.
- Data-block keys are **internal keys**:
  `user_key | fixed64(seq << 8 | type)`, ordered user key ascending, then
  seq descending.
- Index-block entries map a separator internal key to a `BlockHandle`
  (varint64 offset, varint64 size). format_version 2 keeps the seq in index
  keys and does not delta-encode handles. Those changes come with
  format_version 3 and 4 in later RocksDB, so they are out of scope.

**Compression:** Snappy blocks are the raw (unframed) Snappy format. Under
format_version 2, the LZ4, Zlib, BZip2 and ZSTD encodings instead carry a
varint32 decompressed-size prefix (`table/format.cc:269-306`). Doris never
writes those (§2), so they are a "read later, if ever" item.

**Properties block:** a sorted key-value block. Values are varint64 for the
numeric properties and strings for names (`table/meta_blocks.cc:66-106`).
The fields include:

- `rocksdb.data.size`, `rocksdb.index.size`, `rocksdb.num.entries`,
  `rocksdb.num.data.blocks`, `rocksdb.raw.key.size`, `rocksdb.raw.value.size`;
- `rocksdb.format.version`, `rocksdb.column.family.id`,
  `rocksdb.column.family.name`, `rocksdb.comparator`, `rocksdb.compression`;
- `rocksdb.prefix.extractor.name` (= `rocksdb.FixedPrefix.4` for `meta`);
- `rocksdb.creation.time`, `rocksdb.oldest.key.time`, `rocksdb.fixed.key.length`;
- collector outputs such as `rocksdb.deleted.keys`, `rocksdb.merge.operands`
  and `rocksdb.block.based.table.*`.

A reader can ignore all of it. A writer should emit a plausible set: copy
the key set from a fixture SST and compute the numbers.

### 4.4 Flush and compaction

All defaults are in `include/rocksdb/options.h`:

- `write_buffer_size` is 64 MiB (`:190`).
- `level0_file_num_compaction_trigger` is 4 (`:223`).
- Compaction style is level.

`avoid_flush_during_recovery` is false (`:883`). That means **every BE
restart replays the WAL and flushes it into L0 SSTs**, then deletes the old
WAL. So a cleanly restarted BE's meta store is "SSTs plus a small fresh
WAL", and a running one can hold a large WAL.

Compaction rewrites SSTs into L1 and below. At the bottommost level it may
zero sequence numbers, which is harmless to a reader. A reader therefore
handles:

- many L0 files with overlapping key ranges (newest wins by seq);
- sorted, non-overlapping L1+ levels;
- the WAL as the newest layer.

A writer **does not need compaction**. It can:

- append WAL batches;
- when the WAL grows past a threshold, write a new L0 SST from its
  in-memory table and log a VersionEdit;
- optionally do a "full rewrite": read everything and write one L1 SST
  (or a few), plus one VersionEdit that deletes the old files.

RocksDB will compact whatever it is handed. The minimum valid output of a
writer is `CURRENT` + `MANIFEST` + one WAL. RocksDB 5.14.2 will open that
and flush it itself.

**Prefix extractor:**

- With no filter policy, no `memtable_prefix_bloom_size_ratio`, and a
  binary-search index, the 4-byte prefix extractor changes no bytes on disk.
  It appears only in SST properties and in OPTIONS.
- Doris's `iterate()` does a plain `Seek` and stops when the prefix no
  longer matches (`olap_meta.cpp:265-299`).
- A Rust reader should do **total-order** iteration plus a prefix check. It
  must not depend on prefix-bounded seek semantics.

## 5. What the BE stores in the `meta` column family

Collected from every `META_COLUMN_FAMILY_INDEX` call site:
`storage/tablet/tablet_meta_manager.{h,cpp}`,
`storage/rowset/rowset_meta_manager.{h,cpp}`, `storage/binlog.h`,
`storage/data_dir.cpp`, `storage/tablet/tablet.cpp`, and the txn and
engine code that calls them.

| Prefix (4-byte extractor prefix) | Key shape | Value | Written by | Source |
|---|---|---|---|---|
| `tabletmeta_` (`tabl`) | `tabletmeta_<tablet_id>_<schema_hash>` (decimal) | `TabletMetaPB` | `TabletMetaManager::save`, at create, every meta save and load-from-json | `tablet_meta_manager.h:34`, `.cpp:59,105,114` |
| `hdr_` | `hdr_<id>_<hash>` | old pre-PB header | **never written**; scanned at startup to detect and reject the old format | `tablet_meta_manager.h:32`, `data_dir.cpp:334-336` |
| `rst_` | `rst_<tablet_uid>_<rowset_id>` (uid is `UniqueId::to_string`, 16 hex `hi` + `-` + 16 hex `lo`, `util/uid_util.h:90-94`) | `RowsetMetaPB` (committed or pending rowsets) | `RowsetMetaManager::save` and `TxnManager::commit_txn`; rewritten in `data_dir` load-time conversion | `rowset_meta_manager.h:42`, `.cpp:45-169`, `data_dir.cpp:395-396` |
| `ppi_` | `ppi_<tablet_id>_<publish_version>` | `PendingPublishInfoPB` | `save_pending_publish_info` | `tablet_meta_manager.h:36`, `.cpp:175-186`, `data_dir.cpp:488` |
| `dlb_` | `"dlb_" + be64(tablet_id) + be64(version)`, **20 bytes, binary** | `DeleteBitmapPB` (merge-on-write) | `save_delete_bitmap`; range-removed by `remove_old_version_delete_bitmap` | `tablet_meta_manager.h:38`, `.cpp:211-296` |
| `pui_` | `pui_<tablet_id>_<partition_id>_<txn_id>` | `PartialUpdateInfoPB` | `RowsetMetaManager::save_partial_update_info` | `rowset_meta_manager.h:44`, `.cpp:603-690` |
| `binlog_meta_` (`binl`) | `binlog_meta_<tablet_uid>_<version:020d>_<rowset_id>` | `BinlogMetaEntryPB` | `_save_with_binlog`, only when a table has binlog enabled | `binlog.h:28-54`, `rowset_meta_manager.cpp:150-160` |
| `binlog_data_` (`binl`) | `binlog_data_<tablet_uid>_<version:020d>_<rowset_id>` | `RowsetMetaPB` | same | `binlog.h:57-72` |
| `gc_` (prefix `gc_` + 1st id char) | `gc_<rowset_id>` | `RemoteRowsetGcPB` | cold-storage (remote) rowset GC | `olap_define.h:157`, `tablet.cpp:2462-2468`, `data_dir.cpp:1015-1052` |
| `tgc_` | `tgc_<tablet_id>` | `RemoteTabletGcPB` | remote tablet GC | `olap_define.h:158`, `tablet.cpp:2487-2493`, `data_dir.cpp:1063-1095` |

Notes:

- `dlb_` is the only binary key. It relies on big-endian ids so that
  bytewise order equals numeric order. That makes it the one place where
  the Rust comparator **must** be plain bytewise `memcmp`; any
  locale-aware or string comparison breaks it.
- `gc_` is only 3 ASCII characters, so its 4-byte "prefix" includes the
  first character of the rowset id. This is harmless under total-order
  iteration and is one more reason not to depend on prefix seek.
- `tablet_meta_manager.cpp` also has JSON load and dump helpers. Those are
  conversions on top of `TabletMetaPB`, not extra keys.
- Startup (`DataDir::load`) scans `hdr_` (reject), then `rst_`,
  `tabletmeta_`, `ppi_` and `dlb_` in turn (`data_dir.cpp:335,410,455,503,655`).
  `pui_` is scanned by `StorageEngine` (`storage_engine.cpp:1130`). An empty
  meta store is a valid "no tablets" BE.
- For the first doris-rs slices (create table, load, query), only
  `tabletmeta_` and `rst_` are needed, plus `dlb_` for merge-on-write and
  `ppi_` for async publish. Binlog and remote GC can wait.

## 6. Minimal subset for doris-rs

**Read (must handle anything a C++ BE wrote):**

1. `CURRENT` → MANIFEST name.
2. MANIFEST log reader → VersionEdit replay. Handle tags 1-10, 100, 102,
   103 (custom tags) and 200-203. This yields the column-family map and the
   live files per level, plus `log_number` and `last_sequence`.
3. SST reader: footer (v1+ plus legacy), metaindex, index, data blocks;
   CRC32C verification; codecs none and Snappy; internal-key decoding.
4. WAL reader: log records with fragmentation, tolerant of a torn tail;
   WriteBatch decoding of value and deletion records (CF and non-CF forms).
   Replay only WALs with number ≥ that family's `log_number`.
5. Merged view: WAL > L0 (newest seq first) > L1..Ln, with deletion
   tombstones applied. Filter to the `meta` column family.

**Write (must be openable by 5.14.2 and loadable by the C++ BE):**

1. Fresh DB: `MANIFEST-000001` with the comparator, log/next/seq fields, and
   adds for `doris` (1) and `meta` (2) plus `kMaxColumnFamily = 2`. Then
   `CURRENT`, `IDENTITY`, `LOCK`, and an empty `000003.log` or similar.
2. Appends: WriteBatch records (`kTypeColumnFamilyValue/Deletion`, cf 2)
   into the current WAL. Monotone sequence numbers. fsync is policy, and
   the BE default is no fsync.
3. Flush: an SST writer (format_version 2, CRC32C, Snappy or none, 4 KiB
   blocks, restart interval 16, properties). Then one VersionEdit
   containing `kColumnFamily = 2`, `kNewFile2`, the new `log_number`,
   `next_file` and `last_sequence`. Then start a new WAL.
4. Optional full rewrite in place of compaction.
5. Optional OPTIONS file, a template copied from a C++ fixture.
   Reproducing it is not required.

**Opening an existing C++-written dir for write:** start a new MANIFEST
containing a snapshot edit, the way RocksDB itself does on every open, and
start a new WAL. Never append to a MANIFEST you did not create.

## 7. Proving it, with no RocksDB linked

The steps are:

- **Fixtures (oracle C++ BE, Docker):**
  1. Copy `<root>/meta` after a clean BE stop. That gives SSTs plus a small
     WAL.
  2. Copy it again from a running BE using `docker cp`, which may catch a
     torn WAL.
  3. Use DDL and loads that exercise `tabletmeta_` and `rst_`, a
     merge-on-write table (`dlb_`), partial update (`pui_`) and binlog.
  4. Use a tablet meta larger than 32 KiB, to force WAL fragmentation and
     multi-KiB SST entries.
  5. Run enough tablets to cross a flush or compaction.
  6. Record the expected key-value set via the BE's `meta_tool` (`be/src/tools/meta_tool.cpp`) or HTTP
     `/api/meta/header/<tablet_id>` (`service/http_service.cpp:264`), which gives a JSON `TabletMetaPB`.
- **Read check:** the Rust reader enumerates the same keys and decodes the
  same `TabletMetaPB`s as the oracle's dump.
- **Swap check:**
  1. The Rust writer produces a `meta/` directory, either from scratch or
     rewritten from a fixture.
  2. Mount it into an oracle C++ BE container with matching data files and
     start the BE.
  3. Pass if the BE opens the store (no MANIFEST or column-family error)
     and loads the tablets.
  4. Then let the C++ BE write and stop, and read the result back in Rust.
     That round trip covers RocksDB-written compaction output.

## 8. Existing pure-Rust code (checked on crates.io, 2026-11-04)

| Crate | What it is | Fit |
|---|---|---|
| `rocksdb`, `rust-rocksdb`, `ckb-rocksdb` | C++ bindings | Ruled out by the decision |
| `rocksdb-fileformat` 0.1.0 (2025-09, 328 downloads) | "RocksDB SST file format reader", with an `sst_file_writer` module | SST only: no WAL, MANIFEST or column families. Pulls `lz4` and `zstd`, which are C-backed `-sys` crates. Doc coverage 16%. A reference at most, not a dependency. |
| `rusty-leveldb` 4.0.2 (active, ~519k downloads) | Pure-Rust LevelDB | The closest ancestor format: same log record framing, block format and VersionEdit tags 1-9. But no column families, no tags 100+, a legacy footer and no CRC32C footer byte. Useful to read alongside; not adoptable as is. |
| `mmdb`, `fjall`/`lsm-tree`, `sled`, `redb`, `surrealkv` | Pure-Rust LSM or B-tree stores | Their own formats; none claims RocksDB compatibility |
| `snap` 1.1.2 (BurntSushi) | Pure-Rust Snappy, raw and framed | **Use this** for the one codec needed |
| `crc32c` | CRC32C with a software fallback | Use this, or hand-roll ~30 lines with a table |

Conclusion: **nothing to adopt**. The format pieces are small and well
documented, so they are best written in-tree. Only `snap` and a CRC32C
crate are worth depending on.

## 9. Size and risks

**Estimate**, in Rust LOC excluding tests:

- log-record reader and writer: about 250
- WriteBatch: about 200
- VersionEdit and VersionSet replay/snapshot: about 600
- block and SST reader: about 700
- SST writer with properties: about 600
- merged iterator and DB facade: about 400
- WAL and MANIFEST writer orchestration plus flush: about 500

That totals **about 3.3k LOC**, plus fixture harness and tests. The reader
half (about 1.9k LOC) is independently shippable: it unblocks "doris-rs
loads a C++ BE's tablets". The writer half then unblocks the swap check.

**Risks, highest first:**

1. **Compression depends on the build, not on Doris.** Snappy is the default
   only because `<snappy.h>` happened to be found. A distro or vendor build
   without it would write `kNoCompression`. That is harmless for reading,
   since the reader handles both.
   
   The danger runs the other way: if doris-rs writes Snappy into a RocksDB
   built without Snappy, open fails with "not supported". **Writer default:
   Snappy, to match the oracle, behind a switch to none.** Read every
   block's own type byte; never assume.
2. **`format_version` and checksum type.** These are stock (2 and CRC32C),
   but both are self-describing in the footer. Read them from the footer.
   Reject xxHash (type 2) explicitly, unless a fixture ever shows it.
3. **MANIFEST invariants** (§4.2): the column-family set, `log_number` per
   family, `last_sequence` ≥ the max seq, and `next_file` above every file
   number used. A violation means the BE fails to start or silently drops
   WAL data. The swap check is the guard.
4. **Large values.** WAL fragmentation and oversized SST blocks are easy to
   get wrong at the edges. Add a fixture with a multi-MiB `TabletMetaPB`.
5. **Column-family ids.** `meta` = 2 is true for DBs created by this code
   path, but always resolve the id by name.
6. **Torn WAL tail.** Match point-in-time recovery. Do not error on a
   short or corrupt final record.
7. **Seq-zeroing and L0 overlap after compaction.** A reader must order by
   (user key, seq desc) across files, not by file order. This is covered
   by the round-trip fixture.
8. **OPTIONS file.** Low risk: 5.14.2 does not read it on open. If a later
   Doris bumps RocksDB and turns on options checking, revisit this.
