# FE metadata on disk: the image and the BDB-JE edit log

Research for #15 (part of the wayfinder map #14). Question: what is in a Doris 4.1.5 FE's
`meta_dir`, how each piece is serialized, and what reading and writing it in pure Rust
would take. The two cases are **cold cutover** (all Java FEs stop, doris-rs FEs start from
their metadata) and **rollback** (a reference FE starts from doris-rs's metadata).

Sources:

- Doris 4.1.5 at `917b20be838` ("add 4.1.5 release"), the read-only worktree
  `.cache/doris-src`. Paths below are relative to
  `fe/fe-core/src/main/java/org/apache/doris/` unless they start with `fe/`.
- Doris's Berkeley DB Java Edition fork: `apache/doris-thirdparty`, branch `bdbje`, HEAD
  `70324a2` (2026-02-09). Paths marked `je:` are relative to `src/main/java/com/sleepycat/`.
  The branch is at `18.3.18-doris-SNAPSHOT`, but Doris 4.1.5 pins
  `je.version = 18.3.14-doris-SNAPSHOT` (`fe/pom.xml:285`, artifact
  `org.apache.doris:je`, `fe/pom.xml:978-980`). See risk R6.

## TL;DR

- **Layout.** `meta_dir/` holds `process.lock`, `image/` and `bdb/`.
  - `image/` holds `image.<journalId>` snapshots plus three small Java `.properties`
    files: `VERSION` (clusterId, token), `ROLE` (role, name) and, in cloud mode only,
    `DEPLOY_MODE`.
  - `bdb/` is a replicated BDB-JE environment: `*.jdb` log files, `je.lck`, `je.info.*`.
    It holds one JE database per journal segment, named by the decimal id of the first
    journal in the segment, plus `epochDB` and JE's internal databases.
- **Serialization is two layers.** The outer layer is Java `DataOutput` framing:
  big-endian ints and longs, and `Text` strings stored as an int32 length followed by
  UTF-8 bytes. The inner layer is almost entirely **GSON JSON stored inside a `Text`**.
  - The image uses the `COR1` container: magic, JSON header, modules, footer index,
    checksum. It has a header section plus **37 modules** (one more, `cloudWarmUpJob`, in
    cloud mode), and 7 deprecated module names that are skipped when read.
  - Each journal record is the JE key (the journal id as a big-endian long with the sign
    bit flipped) and the JE value (an `int16` opcode followed by the payload).
    `OperationType` defines **238 opcodes** (51 marked `@Deprecated`). `JournalEntity`
    decodes **207 of them** into **~129 payload classes**, nearly all of them
    `Text(GSON)`.
- **Recommended cutover path: no BDB-JE code in Rust at all.**
  1. Stop the cluster and copy the master's `meta_dir`.
  2. Run the reference FE once on the copy with `enable_check_compatibility_mode=true`
     and `checkpoint_after_check_compatibility=true`. It replays every journal, writes
     `image.<maxJournalId>` (`catalog/Env.java:7658-7671`, `6606-6640`) and exits.
  3. doris-rs then needs only an **image reader** and a properties reader.

  A pure-Rust **read-only** JE log scanner is a feasible fallback, about 2-3k LOC. It
  stays small because the Doris journal is append-only, keyed by id, and needs only about
  10 of the 43 JE log entry types.
- **Recommended rollback path: image only.**
  1. doris-rs writes `image/image.N`, `VERSION` and `ROLE`, and leaves `bdb/` empty.
  2. The Java master opens a fresh JE environment and creates journal database `N+1`.
     This is a supported path: "when we start cluster with an image file copied from
     other cluster" (`journal/bdbje/BDBJEJournal.java:538-548`).
  3. Followers re-join empty through `--helper`.

  **Writing a BDB-JE environment from Rust is a non-goal** (see "Writing BDB-JE").
- **The real cost is the GSON object model, not the containers.** It covers about 388
  files with `@SerializedName` (2,072 annotated fields), 23 polymorphic `"clazz"` adapter
  families with 183 subtypes, Guava adapters, and 67 `GsonPostProcessable` hooks. That
  model is the FE catalog, so most of this work is shared with porting the FE itself.
  Model it with lossless pass-through of fields doris-rs does not understand, so that
  rollback does not drop anything.

## 1. What is in `meta_dir`

| Path | Writer | Format | Source |
|---|---|---|---|
| `meta_dir/process.lock` | `DorisFE.tryLockProcess` | empty file, OS file lock | `DorisFE.java:91`, `:589-600` |
| `meta_dir/image/` | `Env` | directory | `catalog/Env.java:362` (`IMAGE_DIR = "/image"`), `:1172-1177` |
| `image/image.<N>` | `Checkpoint` / `Env.saveImage` | COR1 image (§2); N = last journal id it contains | `persist/Storage.java:54`, `:324-325` |
| `image/image.ckpt` | checkpoint in progress | COR1, temp name | `persist/Storage.java:55` |
| `image/VERSION` | `Storage.writeClusterIdAndToken` | `java.util.Properties`: `clusterId=<int>`, `token=<str>` | `persist/Storage.java:49-50`, `:251-256` |
| `image/ROLE` | `Storage.writeFrontendRoleAndNodeName` | Properties: `role=FOLLOWER\|OBSERVER`, `name=<nodeName>` | `persist/Storage.java:51-52`, `:258-265` |
| `image/DEPLOY_MODE` | `Storage.writeClusterMode` | Properties: `deploy_mode=cloud\|local` | `persist/Storage.java:58-61`, `:267-271` |
| `image/edits.<N>` | `LocalJournal`, only when `edit_log_type=LOCAL` ("for testing only") | DataOutput op stream | `journal/local/LocalJournal.java:60-96`, `fe/fe-common/.../Config.java:265-269` |
| `meta_dir/bdb/` | `BDBEnvironment` | replicated JE env, group `PALO_JOURNAL_GROUP` | `catalog/Env.java:361`, `journal/bdbje/BDBEnvironment.java:81`, `:125` |
| `bdb/NNNNNNNN.jdb` | JE | JE log files (8 hex digits), 10,000,000 bytes by default | `je: je/log/FileManager.java:141`, `je/config/EnvironmentParams.java:540-544` |
| `bdb/je.lck`, `je.info.*`, `je.stat.csv` | JE | lock, text logs, stats | (JE runtime) |

Notes:

- `VERSION` and `ROLE` live in **`image/`**, not in `meta_dir` itself:
  `new Storage(this.imageDir)` (`catalog/Env.java:1260-1261`, `:1278-1279`, `:1303`).
- An FE starts "from empty" exactly when both `ROLE` and `VERSION` are missing
  (`catalog/Env.java:1259-1264`). If only one of them exists, startup fails
  (`:1290-1293`).
- `Storage.reload` picks the image with the largest `<N>` and skips `image.ckpt` and
  `*.part` (`persist/Storage.java:143-170`).
- `Properties.store` writes a `#<date>` comment line and uses Java properties escaping.
  A Rust implementation has to handle `\`, `:` and `=` escaping and `\uXXXX`, but values
  here are plain ASCII ids, tokens and hostnames.
- **`clusterId` and `token` must survive any cutover byte for byte.** BEs check
  `cluster_id`, and a missing token is regenerated (`catalog/Env.java:1339-1362`).

## 2. The image file (COR1)

### Container

- Written by `MetaWriter.write` (`persist/meta/MetaWriter.java:96-135`) and read by
  `MetaReader.read` (`persist/meta/MetaReader.java:71-137`).
- All multi-byte numbers are **big-endian** (`java.io.DataOutput`).
- `Text` is `int32 byteLen` followed by UTF-8 bytes (`fe/fe-common/.../io/Text.java:404-457`).

```
"COR1"                                     4B magic   (FeMetaFormat.COR1, persist/meta/FeMetaFormat.java:21)
Text {"imageVersion":"v1"}                 Jackson JSON header; must equal "v1" or load fails (persist/meta/MetaHeader.java:44-51)
-- "header" section (Env.saveHeader, catalog/Env.java:2705-2724) --
int32  meta_version                        = FeMetaVersion.VERSION_CURRENT = 140 (fe/fe-common/.../FeMetaVersion.java:104-107)
int64  replayedJournalId                   the image's journal id (= N in image.N)
int64  nextId                              idGenerator batch end id (catalog id generator)
bool   true                                deprecated isDefaultClusterCreated
-- modules, in PersistMetaModules.MODULES_IN_ORDER --
...module bytes...
-- footer (persist/meta/MetaFooter.java:30-44, :95-110) --
int64  checksum
int32  indexCount
{ Text name; int64 offset } * indexCount   offset of each module, including "header"
int64  footerLength
"COR1"                                     4B magic
```

How the reader treats modules:

- It walks the **footer index**, not its own list (`persist/meta/MetaReader.java:87-127`).
- Empty modules (same offset as the next module) and deprecated names are skipped.
- An unknown name throws, unless `ignore_unknown_metadata_module=true`
  (`fe/fe-common/.../Config.java:3118`, `MetaReader.java:113-124`).
- A module that is **absent** from the footer is simply never loaded: Java keeps its
  in-memory defaults. This matters for rollback (§5).

**Checksum.** Each module's save/load folds a few counts and ids into a running `long`
with XOR. Examples: `saveHeader` uses the version, journal id and next id
(`Env.java:2705-2724`); `saveFrontends` uses list sizes (`:2733-2751`); `saveBackends`
uses the count and each backend id (`system/SystemInfoService.java:764-776`). The reader
checks only the final value (`MetaReader.java:132-133`). The checksum is weak and does not
prove integrity, but a Rust writer must reproduce it exactly.

**Modules.** There are 37 (`persist/meta/PersistMetaModules.java:39-48`):

```
masterInfo frontends backends datasource db alterJob recycleBin globalVariable cluster broker
resources exportJob backupHandler paloAuth transactionState colocateTableIndex routineLoadJobs
loadJobV2 smallFiles plugins deleteHandler sqlBlockRule policy globalFunction workloadGroups
binlogs resourceGroups AnalysisMgrV2 AsyncJobManager workloadSchedPolicy insertOverwrite plsql
dictionaryManager indexPolicy KeyManagerStore authenticationIntegrations roleMappings
```

- Cloud mode adds `cloudWarmUpJob` (`:51`).
- These names are deprecated and skipped on read: `loadJob cooldownJob AnalysisMgr
  mtmvJobManager JobTaskManager syncJob lanceIndexJobManager` (`:54-58`).
- The name-to-method mapping is done by reflection in `persist/meta/MetaPersistMethod.java`
  (`create`, `:48`).

### Inside a module

Inside a module there is thin binary framing around GSON blobs. Examples:

- `db`: `int32 dbCount` (excluding `information_schema` and `mysql`), then for each
  database `Text(GSON(Database))`, `int32 tableCount` and `Text(GSON(Table))` per table
  (`datasource/InternalCatalog.java:4029-4049`, `catalog/Database.java:710-715`,
  `:764-768`). An `OlapTable` serializes its partitions, indexes, tablets and replicas
  inline (`catalog/OlapTable.java:2026-2028`). **This is where image size comes from**:
  large clusters produce GB-scale images.
- `backends`: `int32 n`, then `{ int64 id; Backend }` (`system/SystemInfoService.java:764-776`).
- `frontends`: `int32 n; Frontend*; int32 m; Text removedName*` (`catalog/Env.java:2733-2751`).
- `exportJob`: `int32 n; { int64 jobId; ExportJob }*`. Expired jobs are dropped on save
  and on load (`catalog/Env.java:2761-2785`, `:2390-2406`).
- `globalVariable`: `SessionVariable` is written as **json-simple** JSON, not GSON
  (`qe/SessionVariable.java:5993-6006`), followed by `GlobalVarPersistInfo`
  (`qe/VariableMgr.java:460-472`).
- `paloAuth`: role manager, user manager, user-role manager, property manager, LDAP info,
  in that order (`mysql/privilege/Auth.java:2034-2040`).
- `transactionState`: `int32 n`, then every transaction state, then the id generator
  (`transaction/GlobalTransactionMgr.java:926-932`).
- `resources` and `datasource` (the catalog manager) are each a single
  `Text(GSON(this))` (`catalog/ResourceMgr.java:238-241`,
  `datasource/CatalogMgr.java:1236-1239`).

### GSON configuration

The GSON configuration that a Rust implementation must match is in
`persist/gson/GsonUtils.java:640-690`:

- `RuntimeTypeAdapterFactory` with a **`"clazz"` discriminator** for 23 polymorphic
  families: `Type`, `Expr`, `DistributionInfo`, `Resource`, `AlterJobV2`, `Table`,
  `Partition`, `Replica`, `PartitionInfo`, `LoadJob`, and others. There are 183
  `registerSubtype` and `registerCompatibleSubtype` calls (counted with grep in that file).
- `enableComplexMapKeySerialization()`: maps with non-string keys become `[[k,v],...]`.
- `serializeSpecialFloatingPointValues()`, so `NaN` and `Infinity` can appear unquoted.
- `ToNumberPolicy.LONG_OR_DOUBLE`.
- Hidden-field exclusion.
- Custom adapters for Guava `Table`, `Multimap`, `ImmutableMap` and `ImmutableList`,
  plus `AtomicBoolean`, `PartitionKey` and `Range`.
- Pre- and post-process hooks. There are 67 `GsonPostProcessable` implementations, and
  they rebuild derived in-memory state after deserialization.

## 3. The edit log in BDB-JE

### Doris's use of JE

- **Databases.** Each segment is one JE database, named by the decimal id of its first
  journal (`journal/bdbje/BDBJEJournal.java:99-104`). The journal rolls into a new
  database after `edit_log_roll_num` entries (default 50,000, `Config.java:277`).
  `epochDB` is used for fencing (`journal/bdbje/BDBEnvironment.java:87`, `:205`).
  JE's own databases (`_jeUtilization`, `_jeRepGroupDB`, `_jeVlsnMapDb`, ...) are listed
  at `je: je/dbi/DbType.java:71-90`.
- **Key.** `TupleBinding<Long>`, which is `writeUnsignedLong(id ^ 0x8000000000000000)`:
  8 bytes, big-endian, sign bit flipped (`BDBJEJournal.java:322-327`,
  `je: bind/tuple/TupleOutput.java:222-226`).
- **Value.** `JournalEntity.write`: `int16 opCode` followed by `data.write(out)`
  (`journal/JournalEntity.java:198-201`). Reading switches on the opcode
  (`:203-1040`). **An unknown opcode throws** (`:1033-1037`).
- **Writes.** Single records are written with auto-commit `put`; batches are written in
  one JE transaction (`BDBJEJournal.java:130-160`, `:232-300`).
- **Opcodes.**
  - `persist/OperationType.java` defines **238** `short` opcodes, from `OP_LOCAL_EOF=-1`
    and `OP_SAVE_NEXTID=0` up to the cloud and snapshot ops 1000-1101. 51 of them are
    marked `@Deprecated`.
  - `JournalEntity` has 207 `case` labels that decode into about **129 distinct payload
    classes**.
  - Nearly all payloads are `GsonUtils.GSON.fromJson(Text.readString(in), X.class)`, for
    example `persist/CreateTableInfo.java:97-99`. The legacy `Writable` exceptions are
    `OP_SAVE_NEXTID` and `OP_SAVE_TRANSACTION_ID` (a bare `Text`), `BackendTabletsInfo`
    (`persist/BackendTabletsInfo.java:80-88`) and `Cluster`.
  - Replay semantics live in `EditLog.loadJournal` (`persist/EditLog.java:296`, 213
    `case` labels in a 2,580-line file).
- **Checkpoint.** It covers only *finalized* journals, meaning every database except the
  last (`master/Checkpoint.java:118`). Journals up to the image id are then deleted
  (`:312`). **At any moment, up to about 50k of the newest journals exist only in `bdb/`.**
  That is why cutover needs either the compatibility-mode trick or a JE reader.

### JE log format (Doris fork, LOG_VERSION 17)

Sources: `je: je/log/LogEntryHeader.java`, `je/log/LogEntryType.java`, `je/log/LogUtils.java`.

- **Entry header.** `uint32 checksum` (Adler32, `je/log/ChecksumValidator.java:20-34`),
  `u8 type`, `u8 flags`, `uint32 prevOffset`, `int32 itemSize`, plus an optional `8B VLSN`
  for replicated entries or when `VLSN_PRESENT` is set. The fixed part is 14 bytes
  (`LogEntryHeader.java:75-86`).
- **Byte order.** JE integers are **little-endian** (`LogUtils.java:127-136`). Many item
  fields use JE's `PackedInteger`.
- **Flags.** Provisional bits `0x80` and `0x40`, replicated, VLSN-present, and
  **`INVISIBLE` 0x10** (`LogEntryHeader.java:95-104`, `:281`). Replication rollback marks
  rolled-back entries invisible, and a reader must skip them.
- **Entry types.** There are 43 (`LogEntryType.java`). A read-only extractor of the Doris
  journal needs about 10 of them:
  - `FILE_HEADER`
  - `INS_LN`, `UPD_LN`, `DEL_LN` and their `_TX` variants (31-36)
  - `NAMELN` / `NAMELN_TX`, mapping database name to database id
  - `MAPLN`, for database deletion via `removeDatabase`
  - `TXN_COMMIT`, `TXN_ABORT`
  - `ROLLBACK_START`, `ROLLBACK_END`, `MATCHPOINT`
  - `ERASED`

  Everything else (IN, BIN, BIN_DELTA, CKPT_*, FILESUMMARYLN, TRACE, ...) can be skipped
  by `itemSize`.
- **LN body layout** depends on the version and per-entry flags: abort LSN, abort
  key/data/VLSN, expiration, prior size and file, embedded-LN bit, then `dbId`, key and
  data (`je/log/entry/LNLogEntry.java:874-960`). This is the most intricate part of the
  format, but it is bounded: about 1.3k lines of Java cover all versions.
- **Size.** The JE core (`log`, `txn`, `tree`, `dbi`) is about 79k lines of Java, and
  `je/rep` is another 87k. A full port is not on the table.

## 4. Cold cutover (Java → doris-rs)

### Option A, recommended: Java-assisted final checkpoint

No Rust JE code at all.

1. Stop every FE and every load. Copy the master's `meta_dir` to a scratch location.
   Keep the original untouched: compatibility mode runs `DbResetRepGroup` on the
   environment (`journal/bdbje/BDBEnvironment.java:105-112`).
2. Start the reference FE once on the copy with `enable_check_compatibility_mode=true`
   and `checkpoint_after_check_compatibility=true` (`Config.java:3342`, `:3346`).
   - It skips role and helper discovery (`catalog/Env.java:1186-1194`).
   - It loads the image, replays **all** journals (`replayJournal(-1)`), and calls
     `dumpImage()`. That writes `imageDir/image.<maxJournalId>`
     (`catalog/Env.java:6633-6640`, `:7658-7671`). Then it exits.
3. doris-rs ingests that image plus `VERSION` and `ROLE`. It keeps `clusterId`, `token`
   and node names, and starts its own journal at `maxJournalId + 1`.

Trade-off: this needs a JVM **once, at cutover**, when the reference FE is installed
anyway. The destination binary stays JVM-free.

### Option B: pure-Rust read-only JE scanner

This is the fallback, or for when no JVM is acceptable even at cutover.

1. Scan every `*.jdb` in file-number order and validate Adler32.
2. Build `dbName -> dbId` from `NameLN` entries. Keep only names that parse as `u64`
   (the journal databases).
3. Track transaction outcomes from commit and abort entries. Ignore invisible entries.
   Drop databases whose `MapLN` marks them deleted.
4. For each surviving LN, decode the key (unflip the sign bit) and keep the latest LSN
   per key.
5. Emit `(journalId, opcode, payload)` in id order, starting at `image.N + 1`.

The Doris access pattern removes most of the JE complexity:

- Keys are only ever inserted; a retried batch puts the same key again.
- Whole databases are dropped. Records are never deleted individually.
- There are no duplicate-key databases, no secondary indexes, and nothing needs B-tree
  traversal.

Estimate: **about 2-3k LOC of Rust and 2-4 engineer-weeks**, including a test corpus of
real `bdb/` directories produced by the reference FE in `oracle/`.

Residual risk: entries the log cleaner migrated, and their commit visibility (R7).

### Shared work

Whichever option is chosen, the work is an **image reader**. The container itself takes
days. The 37 modules' framing plus the GSON model take weeks to months, mostly shared with
the FE port. For option B, add **journal payload decoding and replay** for every opcode
that can appear in the tail, which means porting the `EditLog.loadJournal` semantics. In
option A that replay happens inside Java, and doris-rs never needs to replay a Java
journal. This is a strong argument for A.

## 5. Rollback (doris-rs → Java)

### Recommended: image-only hand-off

1. Stop doris-rs and write `image/image.N` (COR1, `meta_version=140`, `imageVersion="v1"`),
   where N is doris-rs's last journal id. Also write `VERSION` (the original `clusterId`
   and `token`) and `ROLE`.
2. Leave `bdb/` empty.
3. Start the Java master with itself as helper. `BDBJEJournal.open` finds no journal
   databases and opens `"" + (replayedJournalId + 1)` (`BDBJEJournal.java:538-548`).
   The source explicitly anticipates starting "with an image file copied from other
   cluster".
4. Followers and observers re-join through the standard helper path with an empty
   `bdb/`. Their `frontends` entries come from the image.

What doris-rs must guarantee when writing:

- Write only module names Java knows. Unknown names abort the load unless
  `ignore_unknown_metadata_module` is set.
- Omitted modules are tolerated: Java simply keeps its defaults.
- Every GSON blob must deserialize under Java's `GsonUtils`, including the `"clazz"`
  tags, the complex-map-key encoding, and fields only Java uses.
- So **doris-rs must carry unknown and unmodelled JSON fields through losslessly**.
  Parse to an order-preserving JSON value, project the fields doris-rs uses, and re-emit
  the rest.
- The checksum must match the XOR scheme.

Verification: load the image in the reference FE, require the checksum check to pass, and
diff `SHOW` output and `information_schema` before and after. This fits the repo's
`oracle/` harness.

### Writing BDB-JE: not recommended

For a reference FE to start from a doris-rs **BDB environment**, doris-rs would have to
produce a valid *replicated* JE environment. That means:

- a B-tree (IN, BIN, DbTree, MapLN, NameLN) with consistent LSNs
- checkpoints and utilization (FileSummaryLN)
- the VLSN index (`_jeVlsnMapDb`)
- the rep-group database (`_jeRepGroupDB`) with node identities
- Adler32-checked entries in exactly the layout the pinned JE version accepts

Estimate: several engineer-months, with high fragility against JE internals. The
image-only path makes this unnecessary. It is the same trick Doris supports for
metadata recovery: `metadata_failure_recovery` plus `DbResetRepGroup`
(`BDBEnvironment.java:105-112`).

## 6. What it would take (rough size)

| Piece | Needed for | Size |
|---|---|---|
| Java `Properties` read/write (`VERSION`, `ROLE`, `DEPLOY_MODE`) | both | ~150 LOC, hours |
| DataInput/Output primitives + `Text` | both | ~200 LOC |
| COR1 container: magic, JSON header, footer index, offsets, XOR checksum, module dispatch | both | ~600 LOC, days |
| GSON-compatible serde layer: `"clazz"` polymorphism (23 families, 183 subtypes), complex map keys, NaN, Guava shapes, lossless pass-through | both | weeks; the core risk |
| 37 module framings + post-process derivations (67 hooks) | both | weeks-months, shared with the FE catalog port |
| Read-only JE scanner (about 10 of 43 entry types) | cutover option B only | ~2-3k LOC, 2-4 weeks |
| Journal payload decode (~129 classes) + replay of up to 207 opcodes | cutover option B only | months, i.e. the `EditLog` port |
| JE writer (replicated env) | not recommended | months |

## 7. Hard parts and open risks

- **R1 GSON fidelity.** Field names, `"clazz"` tags, `@SerializedName` alternates,
  map-key encoding, number policy, and Guava adapters must all match Java, both for
  reading and for rollback writing. Lossless pass-through is the mitigation.
- **R2 Semantics hidden in load paths.** Image load does real work: it expires export
  jobs, re-registers frontends, and runs post-process hooks that rebuild indexes. A
  byte-level parser is not enough; doris-rs must re-derive the same state.
- **R3 Version pinning.** Java refuses an image whose `meta_version` is above its own
  140 (`catalog/Env.java:2329-2334`) and requires `imageVersion == "v1"`. A future
  reference version will move both, so doris-rs's writer is tied to 4.1.5's schema.
- **R4 Image scale.** `OlapTable` JSON includes every tablet and replica, which makes
  multi-GB images normal on large clusters. Both reading and writing need streaming JSON.
- **R5 Option A specifics are unverified empirically.** Open questions: whether
  compatibility mode needs BE or network access, whether `getMaxJournalId()` includes the
  open segment's tail, and what `DbResetRepGroup` does to the copy. Confirm in the
  `oracle/` harness before relying on it.
- **R6 JE fork version drift.** 4.1.5 pins `18.3.14-doris-SNAPSHOT`, while the public
  `bdbje` branch is at `18.3.18` (LOG_VERSION 17). Check the `FileHeader` log version of
  real `.jdb` files before trusting the LN layout.
- **R7 JE cleaner and migrated LNs (option B only).** The cleaner rewrites live LNs into
  newer files and deletes old files, possibly together with the original commit records.
  The scanner's visibility rule must be validated against environments that have actually
  been cleaned (long-running FEs), not only against fresh ones.
- **R8 Cluster identity.** `clusterId` and `token` in `VERSION`, plus FE node names in
  `ROLE` and in the `frontends` module, must be carried verbatim. Otherwise BEs and peers
  reject the new FE.
- **R9 Cloud mode** (`DEPLOY_MODE=cloud`, the `cloudWarmUpJob` module, opcodes 1000+) is
  out of scope here and would need its own pass.
- **R10 Weak checksum.** The XOR checksum covers only counts and ids, so passing it does
  not prove the content is correct. Use reference-FE round-trips as the oracle.
