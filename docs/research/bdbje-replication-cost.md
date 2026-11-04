# The cost of speaking BDB-JE replication

Research for #16 (part of map #14). Date: 2026-11-04.

**Question.** What would a doris-rs FE need to implement to join a reference
(Java) Doris 4.1.5 FE group over BDB-JE's replication protocol? How is that
protocol versioned, and how large is it? The answer should let us weigh
*mixed FE groups* (Java and Rust FEs replicating the same metadata at once)
against *cold cutover* (stop every Java FE, start Rust FEs from the metadata
they left behind).

**Short answer.** To join a mixed group, a Rust FE has to reimplement JE HA's
whole node: the port multiplexer, five distinct wire protocols
(about 70 message types), Paxos-style elections, the feeder/replica handshake,
VLSN syncup and rollback, ack-based durability, the replicated group-membership
database, and network restore, which copies JE's *physical* `.jdb` log files.
It also has to encode and decode all 238 Doris journal operation types
byte-for-byte, in both directions. The protocol is frozen (Oracle has published
no release since JE 18.3.12 in November 2018) and the source is Apache-2.0, but
there is no public specification. The design docs it cites are on an
unreachable Oracle-internal wiki, and the fork Doris uses ships no tests. Cold
cutover needs only a read-only parser for the JE log tail plus the Doris image
reader the Rust FE needs anyway. **Recommendation: cold cutover. Do not build
mixed groups.** Details and the residual risks follow.

---

## 1. What Doris actually runs

| Fact | Source |
|---|---|
| Doris 4.1.5 pins `org.apache.doris:je:18.3.14-doris-SNAPSHOT`. | `fe/pom.xml:285`, `:977-980` (Doris `917b20be838`) |
| That artifact is Apache's fork at `apache/doris-thirdparty`, branch `bdbje`. Version 18.3.14 is the tree at commit `7bf75a7475fab88711ba01ab4a549c70a1c13269` (pom bumped to 18.3.14 in `5cd4d31`; `7bf75a7` adds IPv6 parsing, the 18.3.14 entry in `CHANGELOG.md`). Later fork versions (18.3.15–18.3.18, 2025–2026) add TLS and are **not** used by 4.1.5. | <https://github.com/apache/doris-thirdparty/tree/bdbje>, `CHANGELOG.md`, `git log -- pom.xml` |
| The fork is Oracle JE **18.3.12** (`JEVersion.CURRENT_VERSION = 18.3.12`) with small patches. Diffing the fork's `com/sleepycat/je/rep` against Oracle's `je-18.3.12-sources.jar` from Maven Central gives 56 changed lines in 8 files: replica-side VLSN bookkeeping (`VLSNIndex`, `RepImpl`; commit `284830b`) and `HostPortPair` parsing with `lastIndexOf(':')` for IPv6. **Neither patch changes the wire format.** | `JEVersion.java:33`; <https://repo1.maven.org/maven2/com/sleepycat/je/18.3.12/> |
| Upstream JE is frozen. Maven Central's `com.sleepycat:je` metadata reports `latest = release = 18.3.12`, `lastUpdated 20181129`. | <https://repo1.maven.org/maven2/com/sleepycat/je/maven-metadata.xml> |
| Licence: Apache 2.0, so reading the source and porting it to Rust is allowed. | `LICENSE.txt` in the fork |

### How Doris uses JE HA

All paths below are under `fe/fe-core/src/main/java/org/apache/doris/`.

- **One group, `PALO_JOURNAL_GROUP`.** Each FE opens one `ReplicatedEnvironment` and passes its own `host:edit_log_port` and the `helper` host:port list (`journal/bdbje/BDBEnvironment.java:118-124`).
- **Followers** are JE `ELECTABLE` nodes. **Observers** are JE `SECONDARY` nodes: they don't vote, don't ack, have transient IDs and are not persisted in the group DB (`BDBEnvironment.java:132-140`; `NodeType.java:64-76`).
- **Durability** comes from Config: `master_sync_policy=SYNC`, `replica_sync_policy=SYNC`, `replica_ack_policy=SIMPLE_MAJORITY`. Other settings: `REPLICA_MAX_GROUP_COMMIT=0`, `NoConsistencyRequiredPolicy`, heartbeat/feeder timeout of 30 s, `TXN_ROLLBACK_LIMIT=100`, max clock delta of 5 s, max message size of 2 GB (`BDBEnvironment.java:125-180`; `fe-common/.../Config.java:319-418`).
- **Data model.** Each journal "database" holds up to `edit_log_roll_num` = 50 000 entries and is named by its first journal id. The key is a `TupleBinding<Long>`; the value is a serialized `JournalEntity`. An `epochDB` is used for fencing. The master **removes** old journal databases after a checkpoint (`journal/bdbje/BDBJEJournal.java:106-148, 324-357`; `BDBEnvironment.java:205, 339`).
- **Admin calls** go through `ReplicationGroupAdmin`, i.e. the remote Group service: `getGroup()`, `removeMember`, `updateAddress`. Doris also calls the local mutable config `setElectableGroupSizeOverride` (`ha/BDBHA.java:115-262`).
- **Recovery paths** use `NetworkRestore` on `InsufficientLogException` (a file copy of `.jdb` logs from a peer) and `DbResetRepGroup` for `metadata_failure_recovery` (`BDBEnvironment.java:104-115, 207-217`).
- **Role changes** arrive through `StateChangeListener`: MASTER, REPLICA (FOLLOWER or OBSERVER), UNKNOWN (`ha/BDBStateChangeListener.java`).
- **Transport** is plain TCP. The OSS `BdbjeReplicationConfigProvider` configures nothing, and the pinned JE has no TLS (`OssBdbjeReplicationConfigProvider.java`).

## 2. The protocol surface

Unless noted, all paths are under `src/main/java/com/sleepycat/je/` in the fork at `7bf75a7`.

### 2.1 Transport and multiplexing

Each node listens on **one port** (Doris's `edit_log_port`). `rep/utilint/ServiceDispatcher.java` reads a request of the form `Service:<1-byte len><ascii name>` and answers with **one byte**, the ordinal of `{OK, BUSY, FORMAT_ERROR, UNKNOWN_SERVICE, PROCEED, INVALID, AUTHENTICATE}` (lines 66-79, 137-153). Enumerator order is part of the wire format.

| Service name | Purpose | Protocol style |
|---|---|---|
| `Feeder` | Log shipping, master to replica (`rep/impl/node/FeederManager.java:198`) | binary |
| `LogFileFeeder` | Network restore: copies `.jdb` files (`rep/impl/networkRestore/FeederManager.java:95`) | binary |
| `Acceptor`, `Learner` | Elections (`rep/elections/Acceptor.java:56`, `Learner.java:74`) | text |
| `Group` | Membership admin (`rep/impl/GroupService.java:72`) | text |
| `NodeState`, `BinaryNodeState` | Node status queries | text / binary |
| `Monitor` | Monitor nodes (not used by Doris) | text |

**Binary framing.** `rep/utilint/BinaryProtocol.java:69-71` defines the header as a 2-byte op id plus a 4-byte body length. Version negotiation (`ClientVersion`/`ServerVersion`/`IncompatibleVersion`, ops 1001-1004) runs first. Integers use JE's `LogUtils` encoding: little-endian fixed-width or JE's own "packed" varint (`log/LogUtils.java:116-142`).

**Text framing.** `rep/impl/TextProtocol.java:51-57` uses `<version>|<group>|<nodeId>|<op>|<payload>` on a single line, and checks group name and membership on every message.

### 2.2 Message inventory (counted from the `MessageOp` declarations)

| Protocol | Version | Messages |
|---|---|---|
| Feeder/replica **handshake** (`rep/stream/Protocol.java`) | stream protocol v3–**9** (`BaseProtocol.java:70-122`; v9 = JE 18.3.4) | 15: ReplicaProtocolVersion, FeederProtocolVersion, DuplicateNodeReject, ReplicaJEVersions, FeederJEVersions, JEVersionsReject, NodeGroupInfo, NodeGroupInfoOK, NodeGroupInfoReject, SNTPRequest, SNTPResponse, ReAuthenticate, DBIdRequest, DBIdResponse, SecurityFailureResponse |
| Feeder/replica **stream** (`rep/stream/BaseProtocol.java`) | same | 14: Entry, StartStream, Heartbeat, HeartbeatResponse, Commit, Ack, EntryRequest, EntryNotFound, AlternateMatchpoint, RestoreRequest, RestoreResponse, ShutdownRequest, ShutdownResponse, GroupAck |
| **Elections** (`rep/elections/Protocol.java`) | text `"2.0"` | 9 + 3 base: Propose, Promise, Accept, Accepted, Reject, Result, MasterQuery, MasterQueryResponse, Shutdown (+ PE/OK/FAIL) |
| **Group admin** (`rep/impl/RepGroupProtocol.java`) | text `"4"` (`"3"` for group format v2) | 10: EnsureNode, EnsureOK, RemoveMember, DeleteMember, GroupRequest, GroupResponse, Fail, UpdateAddress, TransferMaster, TransferOK |
| **Network restore** (`rep/impl/networkRestore/Protocol.java`) | binary v2 | 10: FeederInfoReq/Resp, FileListReq/Resp, FileReq, FileStart, FileEnd, FileInfoReq/Resp, Done |
| **Node state** (`NodeStateProtocol`, `BinaryNodeStateProtocol` v1) | text / binary | 2 + 2 |
| Binary base (`BinaryProtocol`) | — | 4 |

That is **about 70 message types across 5 protocols**, each versioned separately, plus the service handshake.

### 2.3 Payloads are JE's log format

An `Entry` message carries a **JE log entry**: the 14-byte header (checksum, type, version, prev offset, size) plus the VLSN (`log/LogEntryHeader.java:75-86`) and a body in JE's log format at the negotiated "stream log version". Current log version is 17; the highest *replicable* version is 16; feeders can write down to version 9 for older replicas (`log/LogEntryType.java:238-269`). Of the 43 entry types, the replicable ones are the user LN insert/update/delete variants (transactional and not), `NameLN` (database create/rename/remove), `TXN_COMMIT` and `TXN_ABORT` (which carry the DTVLSN since log version 13), `MATCHPOINT`, the extinction-scan LN and `ERASED`. Replicated LNs leave out local fields such as abort LSN/key/data and prior size/file, so the stream format differs from the on-disk format (`log/entry/LNLogEntry.java:60-155`).

A Rust node therefore needs, at minimum, a byte-exact codec for the **replicated** form of these entries at log version 16. It needs JE's packed-integer encoding, Adler-32 entry checksums, and database-id semantics (replicated DBs carry IDs the master allocates; `DBIdRequest` is only a v8 lookup for the partition-generation table used by stream subscribers).

### 2.4 State machines a member must implement

1. **Elections** (`rep/elections/`, ~3.9k non-comment Java lines). A Paxos variant: `TimebasedProposalGenerator` proposals, plus `Acceptor`, `Learner` and `RankingProposer`. Rankings are `(DTVLSN, VLSN, nodeId)` with node priority on top. Tokens at the end of a message are dropped silently for backward compatibility (`Protocol.java:122-145`). Elections are gated by `ElectionQuorum`, the electable-group-size override and the designated primary.
2. **Handshake** (`rep/stream/{Feeder,Replica}FeederHandshake.java`, ~1.1k lines). Negotiates the protocol version and then the JE/log versions (rejected if below the group's `minJEVersion` stored in the group DB). Registers the node through `NodeGroupInfo`; for an electable node the master writes a new member record. Ends with an SNTP-style clock-skew check against `max_bdbje_clock_delta_ms`.
3. **Syncup** (`rep/stream/{Feeder,Replica}FeederSyncup.java`, ~1.3k lines). The replica walks its own log **backwards** by VLSN, asking for the feeder's entry at each VLSN, until the two compare equal: that's the matchpoint. Entries after the matchpoint are rolled back, a "hard recovery" bounded by `txn_rollback_limit`. If no matchpoint is found, the replica throws `InsufficientLogException`, which triggers network restore.
4. **Feeding and replay** (`rep/impl/node/{Feeder,Replica,Replay}.java`, ~5k lines; `rep/txn`). Streaming, heartbeats that carry master time and the txn-end VLSN, acks for `SIMPLE_MAJORITY`, DTVLSN tracking, and replay transactions with locks.
5. **Group membership** (`rep/impl/RepGroupDB.java`, `RepGroupImpl.java`, `RepNodeImpl.java`, ~3k lines). Membership lives in the replicated internal database `_jeRepGroupDB` (`dbi/DbType.java:88`), serialized with format version 3. A Rust master must **write** these records and a Rust replica must **read** them. A Rust node also needs the internal databases `_jeMetadata` and `_jeReservedFilesDb`.
6. **VLSN index** (`rep/vlsn/`, ~4.6k lines). Maps VLSN to log position, to serve `EntryRequest`s and syncup in both roles.
7. **Network restore** (`rep/impl/networkRestore/`, ~1.9k lines). Copies **raw `.jdb` files** from one node to another. A Java FE that falls behind can pick a Rust FE as its log provider, and it then expects JE-format files. A Rust FE that falls behind would be sent JE-format files and would have to start from them.

**Size.** By a rough count of non-blank, non-comment lines, `com/sleepycat/je/rep` is **~69k lines** out of **~218k** for all of JE. The parts listed above total **~46k lines**: stream 7.0k, elections 3.9k, `impl/node` 10.6k, group/text protocol ~4.7k, utilint 9.1k, network restore 1.9k, txn 1.4k, VLSN 4.6k, `log/entry` 2.7k. Some of utilint covers stats and SSL that Doris doesn't use, but in exchange this count leaves out the parts of the log manager, recovery and transactions that replay depends on. A faithful Rust port of the member side is plausibly **15–25k lines of Rust**. The larger cost is a cross-implementation fault-injection test rig (partitions, kills during elections, rollback, restore) run against real Java FEs.

### 2.5 How undocumented it is

- **No public wire specification.** Oracle's public docs cover only the Java API and how to operate it (e.g. *Getting Started with BDB JE High Availability*, <https://docs.oracle.com/cd/E17277_02/html/ReplicationGuide/index.html>). They say nothing about message encoding.
- **The design docs are internal.** The source links to Oracle-internal wikis for the handshake (`sleepycat.oracle.com/pmwiki/…ReplicaFeederHandshake`), elections ("ElectionsImplementation#FreezingVLSNs"), dynamic membership and log recovery. The host did not resolve from here (curl `000`).
- **No tests in the fork.** `apache/doris-thirdparty@bdbje` contains no test sources (0 test files). Oracle's original release tarball (`download.oracle.com/berkeley-db/je-18.3.12.tar.gz`) returned 404.
- **No prior art.** A web search turned up no non-Java implementation of the JE HA protocol and no write-up reverse-engineering it. (Berkeley DB's C `repmgr` is a different protocol.)
- **The only spec is the Java source.** Compatibility rules are spread through it, e.g. "older nodes ignore extra tokens", log-version downgrade when writing, the GROUP_ACK message that exists but is disabled. Enumerator ordinals are part of the wire.

## 3. Doris-level cost (on top of JE)

Speaking JE gets journal *bytes* across. In a mixed group each side must also **replay** what the other writes. A Rust follower must decode every `JournalEntity` a Java master writes, and a Rust master must write entities a Java follower can replay. That covers **238 `OP_*` operation types** (`persist/OperationType.java`; `journal/JournalEntity.java` 1 042 lines; `persist/EditLog.java` 2 580 lines), in Doris's Writable/GSON serialization, byte-compatible in **both** directions, kept in step with every Doris release we track. Cold cutover needs only the read direction. A mixed group also adds the image push over HTTP (`master/Checkpoint.java:226`) and the helper/role bootstrap (`catalog/Env.java:1152-1370`).

## 4. Cold cutover: what the BDB side needs

Doris images cover only *finalized* journal databases. `getFinalizedJournalId()` returns `lastDbName - 1` (`journal/bdbje/BDBJEJournal.java:706-723`), and the checkpointer replays only up to that id (`master/Checkpoint.java:109-160`). A new master rolls its journal and immediately writes `MasterInfo` into the new database (`catalog/Env.java:1707, 1790`), so the **current** journal database is never included in an image.

A Rust FE starting from a stopped Java group's metadata therefore needs:

1. The Doris image reader, which it needs anyway.
2. A **read-only, offline `.jdb` scanner**. It has to:
   - parse file and entry headers (all log versions present on disk, up to 17);
   - decode LN, `NameLN` and txn commit/abort entries in their on-disk (not replication) form;
   - keep only committed transactions;
   - take the highest-LSN version of each `(db, key)`, because the cleaner re-logs live records;
   - ignore B-tree internal nodes, checkpoints and VLSN buckets.

   This is roughly **2–4k lines of Rust**, with no networking, consensus or writing, and it can be checked against a dump from Doris's own `journal/bdbje/BDBTool.java` taken on the reference cluster.

After cutover the Rust group uses whatever journal and consensus it chooses (#14). JE is gone.

The mixed-group option only buys a cutover with no downtime. The cost of cold cutover is downtime for metadata writes. Queries can continue only if the BE path tolerates an FE outage, which is a separate question.

## 5. Verdict

| | Mixed FE group | Cold cutover |
|---|---|---|
| JE work | Full HA member: ~70 messages, 5 protocols, Paxos, syncup/rollback, acks/DTVLSN, group DB, VLSN index, network restore serving and receiving **physical `.jdb` files** (this pushes toward reimplementing JE's storage layout too) | Offline `.jdb` reader, read-only |
| Doris work | 238 op types, byte-compatible encode **and** decode | Decode only (needed anyway) |
| Spec quality | Java source only; internal wikis; no tests in the fork | Same source, but only the log-entry codecs |
| Failure mode | Subtle divergence or election bugs corrupt production metadata **while Java FEs are still serving** | Fails loudly at start-up; the Java FEs' untouched metadata is the rollback |
| Rough size | 15–25k lines of Rust for JE + bidirectional journal compat + heavy cross-implementation testing | 2–4k lines of Rust |

**Feasibility.** Mixed groups are *possible*: the protocol is frozen, Apache-licensed, and readable in about 46k lines of Java. But the risk is **high**. This is a consensus and replication protocol with no spec, no reference tests and no other implementations, and an error causes silent metadata divergence in a live cluster. Network restore ties the Rust FE to JE's physical on-disk format, not just its wire format. Every Doris upgrade reopens the bidirectional journal-compatibility surface.

**Recommendation.** Cold cutover first and only. Record mixed FE groups as **not pursued** unless a hard zero-downtime requirement appears. Even then, a cheaper middle path might be worth researching first: a Rust FE as a read-only **SECONDARY** (observer) that never becomes master. It would still need the handshake, stream, syncup and restore, but not elections, acks or the group-DB writes.

### Open questions (not answered here)

- **Rollback after cutover.** Can a Java FE boot from a Rust-written image with an empty BDB environment (e.g. via `metadata_failure_recovery`)? If it can, rollback needs no JE writer at all. This needs an experiment against the reference.
- **Cleaner versus offline reading.** Confirm the "latest LSN wins" rule for cleaner-migrated LNs and removed databases against real FE `bdb/` directories. Validate against `BDBTool` output.

## Sources

- Doris 4.1.5 source, commit `917b20be83878e98f94e3ebf7efccc80346e3e50`: `fe/pom.xml`; `fe/fe-core/src/main/java/org/apache/doris/{journal/bdbje,ha,persist,master,catalog}/…`; `fe/fe-common/.../common/Config.java`.
- BDB-JE fork used by Doris: <https://github.com/apache/doris-thirdparty/tree/7bf75a7475fab88711ba01ab4a549c70a1c13269> (branch `bdbje`; `CHANGELOG.md`; patch commit `284830b`).
- Oracle JE 18.3.12 sources: <https://repo1.maven.org/maven2/com/sleepycat/je/18.3.12/je-18.3.12-sources.jar>; release metadata <https://repo1.maven.org/maven2/com/sleepycat/je/maven-metadata.xml>.
- Oracle JE HA guide (API-level only): <https://docs.oracle.com/cd/E17277_02/html/ReplicationGuide/index.html>.
