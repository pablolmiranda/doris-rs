# Epic 1 · Milestone 2: join the cluster and hold tablets — design

Tracking issue: [#4](https://github.com/pablolmiranda/doris-rs/issues/4). Parent design:
`docs/superpowers/specs/2026-11-02-epic-1-reporting-design.md` (the epic spec); §5.3, §5.4 and
§6 there define this milestone. Reference: Doris 4.1.5. Date: 2026-11-04.

## 1. Intent

Milestone 2 is the first doris-rs engine code. A `doris-rs be` process joins a cluster run by the
Java FE as an ordinary backend: the FE heartbeats it, it reports its disks, tasks and tablets, and
it creates and drops tablets on request. It runs no queries and takes no loads yet.

Two things make this milestone more than a stub:

- **Clients see the BE.** `SHOW BACKENDS`, `SHOW TABLETS`, `SHOW DATA` and `SHOW PROC` replies
  are compared byte for byte by the strict gate, so what doris-rs reports must be what a C++ BE
  reports, apart from declared deployment facts.
- **The BE is a migration target.** The end state is no Java and no C++ at all; the Java FE is
  temporary scaffolding. People move from Doris to doris-rs one component at a time, and the
  user wants a C++ BE's disks to be usable by doris-rs in place, and the reverse for rollback.
  M2 starts that by laying out data directories exactly as the C++ BE does.

## 2. Decided with the user

1. **Destination: no Java FE.** A Rust FE is a later goal. The BE-first order of ADR 0001 stands;
   the Java FE is treated as throwaway, so nothing is invested in it beyond what the checker needs.
2. **doris-rs matches the C++ BE's features and capabilities**, including persistence: tablets
   survive a restart.
3. **The data directory uses the C++ layout,** so a node can switch between the C++ BE and
   doris-rs on the same disks, in both directions. This is a migration path in its own right,
   alongside cloning.
4. **The tablet metadata store (`<root>/meta`, a RocksDB database in the C++ BE) is implemented in
   pure Rust.** doris-rs never depends on RocksDB, in the binary or in tests.
5. **That store lands in its own milestone, right after this one** (§7), not in M2.

## 3. Goals, non-goals, anti-goals

**Goals**

- `doris-rs be --config <file>` joins the single-profile cluster as its only BE.
- Heartbeat replies, and disk, task and tablet reports, carry what a C++ BE in the same place
  would carry; `SHOW BACKENDS`, `SHOW TABLETS`, `SHOW DATA` match on the strict gate.
- `CREATE` and `DROP` agent tasks (and `UPDATE_VISIBLE_VERSION`, which comes with table creation
  in about ten gate suites) work and are finished with `finishTask`, as the C++ BE does.
- Data directories follow the C++ layout (§5.2); tablet headers are `TabletMetaPB` built the way
  the C++ BE builds them; tablets survive a restart.
- Everything else the FE sends gets a Doris error status naming the missing feature (epic §5.4).
- Done when: every gate suite whose needs (`oracle/epic1/needs.json`) are within this milestone's
  features passes on both gates, and the suites that send the BE no work pass too. At the time of
  writing that is about 220 of 1622: 168 send the BE nothing, about 48 only create and drop
  tablets, about 10 also need `UPDATE_VISIBLE_VERSION`. The plan recomputes the exact list.

**Non-goals**

- Queries, loads, transactions, publish-version, schema change, compaction, clone, snapshots.
- The RocksDB-format `meta/` store and the swap check (§7).
- A Rust FE.
- Linux networking beyond what the harness needs; production deployment.

**Anti-goals**

- A heartbeat or report that only looks right to the FE. Every field the C++ BE sends is sent
  with the C++ BE's value, or with a declared deployment fact.
- Writing anything into `<root>/meta` before it is the real format: a C++ BE started on a
  doris-rs disk must never meet a `meta/` it cannot open.
- A crash or exit on an unsupported request.
- Any C++ or RocksDB dependency.

## 4. The cluster under test

Today every compose file starts a C++ BE. M2 adds an FE-only cluster: the Java FE from the
reference image, the same FE configuration as `reference-single`, and no BE. doris-rs runs
natively on the host (never containerized, per the project principles) and is added with
`ALTER SYSTEM ADD BACKEND`. The `rust` target (`oracle/targets/rust.env`) points at this
cluster's FE.

doris-rs's deployment facts (host address as the FE sees it, data path, cores, memory) differ
from the reference's, so the cluster gets its own facts file in `oracle/clusters/`, and
`make epic-check TARGET=rust` uses it. That is the existing relaxation, not a new one.

How the FE container and the host reach each other (the address doris-rs registers, the FE's
address doris-rs calls back to for `report` and `finishTask`) is the builder's choice; it must
work on macOS with OrbStack. Linux is a follow-up.

## 5. The doris-rs BE in M2

### 5.1 Services

The C++ BE's default ports: heartbeat 9050 (Thrift `HeartbeatService`), be_port 9060 (Thrift
`BackendService`), brpc 8060 (gRPC `PBackendService`), webserver 8040 (HTTP). All four listen in
M2, so the FE and the harness find what they expect; everything beyond the M2 surface answers
with a Doris error naming the method or route.

FE-bound calls: `FrontendService.report` (disks, tasks, tablets, on the C++ BE's intervals) and
`finishTask`.

The calls the FE and the harness make on a clock are answered as the C++ BE answers them, not with
an error, even though `oracle/background_traffic.json` keeps them out of `needs.json` and the
needs index will never list them: `get_tablet_stat` (the FE fills `SHOW TABLETS` and `SHOW DATA`
sizes, row counts and version counts from it), `publish_topic_info`, `get_dictionary_status`, and
the BE HTTP routes `/api/health` and `/api/running_pipeline_tasks/<n>`. The background
declaration lists exactly what a joined BE must answer.

### 5.2 Data directory

Each configured storage root holds, as in the C++ BE (`be/src/storage/olap_define.h`,
`data_dir.cpp`): the `cluster_id` file, written when the FE first assigns one; `data/<shard>/
<tablet_id>/<schema_hash>/` per tablet; and the `trash`, `snapshot` and other top-level folders
the C++ BE creates at start. Shard assignment and the dropped-tablet path (into `trash`) follow
the C++ BE.

Until the meta store milestone (§7), tablet headers (`TabletMetaPB`, serialized exactly as the C++
BE serializes them, under the same `tabletmeta_<tablet_id>_<schema_hash>` keys) live in a
temporary doris-rs-only store **outside** `<root>/meta`. It is deleted when the real store lands;
test clusters lose nothing, because suites recreate their data.

Until then the two binaries cannot share disks, and M2 makes that safe rather than silent:
doris-rs refuses to start on a storage root that has a `meta/` directory or tablet directories
it did not write, and says why. The reverse (a C++ BE started on an M2 root) is unsupported and
documented as such: the C++ BE would not see M2's tablets and could garbage-collect their
directories. Nobody migrates real data before the meta store milestone.

### 5.3 Fidelity sources

- The wire corpus (`oracle/wire/corpus`) and the M1 per-suite traffic corpus
  (`oracle/epic1/traffic/`) hold real heartbeat replies, reports, `CREATE`/`DROP` tasks and
  `finishTask` calls from the reference BE. They are the templates and the test fixtures: given the
  reference's `CREATE` request, doris-rs's `finishTask` and following tablet report must equal the
  reference's, after deployment facts.
- The C++ source at the pinned tag (`.cache/doris-src`, read-only) settles anything the corpus
  does not show.

## 6. Testing

- **Replay in `cargo test`:** reference requests from the traffic corpus go into doris-rs's
  handlers; replies and FE-bound calls are compared with the reference's, after facts.
- **Restart:** create tablets, restart doris-rs, and its next tablet report is unchanged.
- **The suites:** `make epic-check TARGET=rust` against the FE-only cluster, both gates. The
  suite run is the truth (epic anti-goal: `cargo test` passing while suites fail).

## 7. Next milestone (recorded here, designed separately): C++-compatible data directory

- A pure-Rust crate for the subset of the RocksDB on-disk format the BE uses: write-ahead log,
  table files, manifest, `CURRENT`, `OPTIONS`; column families `default`, `doris`, `meta` (the last
  with a fixed-prefix index); operations put, batch put, get, delete, prefix scan.
- It must read anything a C++ BE writes with the pinned RocksDB (5.14.2, `thirdparty/vars.sh`),
  including compacted, compressed table files, and write what that RocksDB opens.
- Proven without RocksDB as a dependency: `meta/` directories written by the reference C++ BE are
  committed as fixtures, and the **swap check** starts a C++ BE on disks doris-rs wrote, and
  doris-rs on disks a C++ BE wrote; the FE and `SHOW TABLETS` must see the same tablets.
- The write and read loop (formerly milestone 3) moves to milestone 4, so the first rowset
  metadata already lands in the real format.

This changes the epic spec's milestone table (§6) and adds an ADR; both are updated with this
design.

## 8. Left to the builder

Crate split (the epic spec names `doris-be` and `doris-storage`), async runtime and servers
(`tokio`, `tonic`, `thrift` are fine), the config file format, the temporary header store's form,
FE–host networking on OrbStack, and the order of work.

## 9. Calls made by the designer (not by the user)

1. M2 includes `UPDATE_VISIBLE_VERSION` because it is small and arrives with table creation.
2. The temporary header store sits outside `<root>/meta` (anti-goal: no fake RocksDB).
3. The FE-only cluster reuses the reference image's FE and configuration.
4. The M2 branch stacks on `epic1-m1-checker` while PR #13 is open.
5. Background calls (`get_tablet_stat` and the rest of `background_traffic.json`) are answered,
   not errored, following decision 2 ("match the C++ BE").
6. Before the meta store milestone, doris-rs refuses storage roots with C++ BE contents; a C++ BE
   on an M2 root is documented as unsupported rather than guarded.
