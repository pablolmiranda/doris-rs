# Epic 1 · M3: C++-compatible data directory — design

Parent: `docs/superpowers/specs/2026-11-02-epic-1-reporting-design.md` §6, row 3. Groundwork:
`2026-11-04-epic-1-m2-join-and-hold-tablets-design.md` §7, ADR 0003. Source paths below are
relative to `.cache/doris-src` (Doris 4.1.5); the BE's storage code is under `be/src/storage/`.

## 1. Intent

People run doris-rs as one binary from the command line, and grow from one node to many with the
same setup. For that, a storage root must mean the same thing to doris-rs and to the C++ BE: a node
can switch binaries on its disks, in either direction, and the cluster does not notice. M2 already
lays out roots as the C++ BE does and builds tablet headers byte for byte, but keeps the headers in
a temporary store (`<root>/doris-rs-meta/`). M3 moves them into `<root>/meta`, the RocksDB database
the C++ BE uses, read and written by doris-rs's own Rust code.

Replication and fleet expansion (new replicas, moving tablets onto a new backend) also depend on
this, but are later work; see §7.

## 2. Goals

1. `<root>/meta` is a database the C++ BE's RocksDB (5.14.2, `thirdparty/vars.sh:214`) opens and
   doris-rs reads and writes, in pure Rust.
2. Everything else a C++ BE writes for tablets without data is matched (§4).
3. **The swap check holds, live, in both directions, with the FE noticing nothing** (§5).

## 3. Non-goals and anti-goals

**Non-goals (later milestones):** segment and index files; the keys that exist only once data is
loaded (`rst_`, `ppi_`, `dlb_`, `binlog_*`, `pui_`, `gc_`, `tgc_`); the CLONE protocol, snapshots,
the `/api/_tablet/_download` routes, rebalancing and decommission (§7); several Doris versions
(only the pinned RocksDB 5.14.2 and Doris 4.1.5).

**Anti-goals (a failure even if the swap check passes):**
- Any RocksDB or other C++ dependency, in the binary or in tests.
- A format that only looks like RocksDB to doris-rs: every file doris-rs writes must be one the
  C++ BE's RocksDB opens, and doris-rs must read what that RocksDB writes, including compressed,
  compacted table files.
- Any change to the FE, or a swap that the FE notices (a DROP or CLONE task, a replica marked bad,
  a changed backend or disk identity).
- Silent data loss on a crash: a header that was acknowledged (finishTask sent) survives a crash
  the way it does on the C++ BE.

## 4. What the C++ BE persists, and what doris-rs must match

For tablets with no data (findings from the source, with citations, are in §4.4).

### 4.1 `<root>/meta`

- **Column families** `default`, `doris`, `meta` (`olap_define.h:146-148`); only `meta` holds
  keys. Every existing column family must be listed when opening, so doris-rs creates and keeps
  all three.
- **Options** (`olap_meta.cpp:77-100`): default column family options, bytewise comparator, no
  bloom filter, block-based tables, crc32c; `meta` has `NewFixedPrefixTransform(4)` (recorded in
  table properties as `rocksdb.FixedPrefix.4`) and `max_write_buffer_number = 5`
  (`common/config.cpp:1426`). Writes use `sync = sync_tablet_meta` (default false,
  `config.cpp:788`); close does `SyncWAL` then `Close` without a flush (`olap_meta.cpp:101-114`).
  Compression is RocksDB 5.14's default, Snappy when built with it; the builder confirms this from
  the C++ BE's files (§6).
- **Files** a 5.14.2 database has: `CURRENT`, `MANIFEST-*` (version edits naming the column
  families), the write-ahead log `*.log` (write batches), `*.sst` block-based table files,
  `IDENTITY`, `LOCK`, `OPTIONS-*`. doris-rs reads all of them and writes what 5.14.2 opens.
- **Keys** doris-rs writes: `tabletmeta_<tablet_id>_<schema_hash>` (decimal) → `TabletMetaPB`
  (`tablet/tablet_meta_manager.h:34`, `.cpp:92`), with every visible rowset embedded in `rs_metas`
  with its schema (`tablet/tablet_meta.cpp:906-913`), as M2 already builds it. Keys doris-rs does
  not write yet (§3) are kept untouched when reading a C++ BE's database.
- **Write path** *(designer's call)*: log appends for every change, as the C++ BE; doris-rs also
  flushes to table files so the log cannot grow without bound. Because the metadata is small, a
  flush may rewrite the whole live key set into one table file; real compaction is not needed. The
  C++ BE must open the result, and doris-rs must open what the C++ BE's compactions leave.
- **Crash safety** as the C++ BE: every change is written to the log before it is acknowledged,
  so it survives a killed process; it is synced to disk only when `sync_tablet_meta` is true
  (default false), and on close. Tested with a killed process, including a log whose last record
  is torn *(designer's call: match the C++ BE, not stricter)*.
- **Lock:** the `LOCK` file is held while running, so the two binaries can never open one root at
  the same time.

### 4.2 Everything else on the root

Already matched in M2: `cluster_id`, `deploy_mode`, `data/0..1023`, the start folders,
`data/<shard>/<tablet_id>/<schema_hash>/`, `trash/<time>.<n>/`. New in M3:

- **`<root>/stream_load`** on the first root: a second RocksDB (a TTL database with column
  families `default` and `meta`, `load/stream_load/stream_load_recorder.cpp:35,55-79`). doris-rs
  creates it as the C++ BE does and keeps it openable; it stays empty until stream load exists.
- **Rowset ids** version 2: 48 hex characters embedding a random per-process `backend_uid`
  (`olap_common.h:540-549`, `rowset/unique_rowset_id_generator.cpp:27-30`).
- **Drop** as the C++ BE does it (`tablet_manager.cpp:573-583`): the header is rewritten as
  SHUTDOWN, and a later sweep moves the directory to trash and deletes the key. This replaces M2's
  immediate move, now that a C++ BE may read the root.
- **Roots with data are refused** *(user's decision)*: if any tablet on a root holds data (a
  rowset with segments, or any key that exists only with data, §3), doris-rs stops at startup with
  a message naming the root and the first such tablet, and touches nothing. Such roots are
  accepted from M4.
- **Startup checks** of `DataDir::load` (`data_dir.cpp:347` and on): a legacy `hdr_` key refuses
  the root; a header whose id or hash does not match its key, or with a zero `tablet_uid`, is
  rejected; a SHUTDOWN tablet goes to the sweep; a running tablet with no versions is an error;
  a header with a rowset lacking its schema is saved again.

### 4.3 Moving off the temporary store

doris-rs no longer refuses a root with `meta/`; it opens it. On first start of M3, headers in
`<root>/doris-rs-meta/` are written into `meta/`, and the folder is removed *(designer's call)*.

### 4.4 Sources

Research notes for this section (file and line for each fact) were gathered on 2026-11-05 from
the 4.1.5 source; the builder re-checks any fact a test depends on against the source and the
captured fixtures, which are the authority.

## 5. The swap check

A live test against the reference C++ BE in Docker. The FE must notice nothing.

- **One FE, one backend entry.** The FE-only cluster (`oracle/compose/fe-only.yml`) has one
  backend at one address. doris-rs and the C++ BE take turns as that backend: one stops, the other
  starts on the same storage root.
- **Same identity.** The backend answers at the same address and ports, and the root has the same
  absolute path in both, so the disk's `path_hash` (computed from host and path) is unchanged. For
  the C++ BE this likely means host networking for its container (OrbStack supports it) and a bind
  mount of the root at the host's path. **User's decision:** if the C++ BE cannot take doris-rs's
  address that way, the swap check alone runs doris-rs in a container beside the C++ BE so both
  share one address; doris-rs stays native everywhere else.
- **Starting state:** tables created through the FE, using the M2 scenario's shapes
  (duplicate-key, merge-on-write, range-partitioned, plus ADD PARTITION).
- **Both directions:** doris-rs → C++ BE, and C++ BE → doris-rs.
- **Passes when**, after each swap: the FE sends no DROP or CLONE task; the next tablet report
  lists the same tablets with the same versions, `tablet_uid`s and path hashes; `SHOW TABLETS` and
  `SHOW BACKENDS` match what they were before the swap, except clocks, the binary's version and
  the declared deployment facts of `clusters/rust.facts.json` (CPU cores, memory, disk capacity as
  each binary sees it) *(designer's call)*; and afterwards CREATE and DROP still work, including dropping a tablet the other binary
  created.
- **Where:** an oracle make target running a scripted swap and printing pass or fail, kept apart
  from the gate suites *(designer's call)*.

Fixtures complement the live check: `meta/` and `stream_load/` directories written by the
reference C++ BE (including after a compaction, so compressed table files appear) are committed and
read by doris-rs tests; the live check is the proof the C++ BE accepts what doris-rs writes.

## 6. Left to the builder

The crate split (a RocksDB-format crate on its own is natural), the flush policy, how much of the
table-file format to write (uncompressed blocks are fine if 5.14.2 opens them), how the swap check
gets the C++ BE onto the backend's address, the order of work. Confirm from captured files the
compression codec and table format version the C++ BE writes.

## 7. Later (recorded, not designed here)

- **Replication** needs the FE-driven CLONE protocol: the CLONE task as destination;
  `BackendService.make_snapshot` and `release_snapshot`, the snapshot layout and its
  `FileHeader<TabletMetaPB>` `.hdr` file, and `/api/_tablet/_download` and `_batch_download` as
  source; rowset id rewriting. A copied data directory does not work with an unmodified FE: it
  drops copied tablets on a healthy tablet, and adopts them with a duplicate replica id on an
  unhealthy one (`fe/.../master/ReportHandler.java:1162-1210`).
- **Fleet expansion** needs all of that (a move is CLONE then DROP), accurate load figures in the
  reports for the balancer, and for decommission `PBackendService.get_wal_queue_size` returning 0.
- Planned as one milestone after M4 (the write and read loop), when there is data to copy.

## 8. Calls made by the designer (not by the user)

1. The temporary store is migrated into `meta/` on first start, then removed.
2. doris-rs flushes to table files, possibly rewriting the whole key set into one file, instead of
   leaving everything in the log or implementing compaction.
3. Drop changes to the C++ BE's SHUTDOWN-then-sweep.
4. The swap check is an oracle make target, not one of the gate suites.
5. The M3 branch stacks on `epic1-m2-join` while PR #25 is open.
6. Crash safety matches the C++ BE (`sync_tablet_meta`), not an fsync per change.
7. The swap check does not compare deployment facts (cores, memory, disk capacity).
