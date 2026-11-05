# Epic 1: Core BE over the upstream p0 suites — design

Tracking issue: [#4](https://github.com/pablolmiranda/doris-rs/issues/4). Reference: Doris 4.1.5
(`oracle/versions.env`). Date: 2026-11-02.

## 1. Intent

doris-rs exists to replace Apache Doris with one Rust binary that nobody, human or program, can
tell apart from the original. Its main client is an AI agent that writes its own SQL, sends it
over the MySQL protocol with an ordinary driver, and decides each next step from the reply it just
got. Every reply counts: result rows, column metadata, errors and warnings.

This epic replaces the Backend (BE) first while the Java Frontend (FE) stays in front, and it is
driven by Doris's own regression suites. The moment it is built for: the upstream `p0` suites run
against a cluster whose only BE is doris-rs, and every suite that passes on the reference passes
there, with replies identical to the reference's.

It is the first of five use-case epics (#4 core BE, #5 logs and events, #6 user behavior, #7 AI
functions and vector search, #8 lakehouse). The checker it builds is shared by all of them.

## 2. Decided with the user

These are the user's decisions and are not open to the builder. ADRs 0001 and 0002 record why
the first draft of this spec changed.

1. **AI agents are the primary client**, so replies are compared in full, not only the result
   text the suites check.
2. **MySQL protocol only.** Postgres is a future step outside all five epics.
3. **Five epics, core BE first** (GitHub issues #4–#8).
4. **The BE is replaced first.** The Java FE keeps the MySQL connection, parsing, planning, schema
   statements, `EXPLAIN` and most error messages. doris-rs executes what the FE sends.
5. **The upstream suites drive and gate the epic** (ADR 0001). Recorded agent sessions are
   deferred to their own issue; they are not part of this epic.
6. **Writing comes first** (ADR 0002). Suites create and load their own tables, so doris-rs takes
   writes from its first useful milestone, and data it writes must stay readable by the C++ BE.
7. **Single profile only:** one Java FE, doris-rs as the only BE. Clusters mixing C++ and
   doris-rs BEs, and the `reference-multi` profile, are out of this epic.
8. **Two gates:** the suite's own pass/fail, and a strict comparison of every MySQL reply the
   suite receives (§4.5), switched on per milestone.
9. **Milestones 1–3 are fixed; after that, the needs index (§4.6) orders the work.**

Project principles from `README.md` also bind: identical interfaces and behavior; the upstream
regression suites as oracle; one native, statically linked binary that this project never
containerizes (Docker appears only in `oracle/`); migrate one component at a time with rollback;
data written by the new code stays readable by the old code.

## 3. Goals, non-goals, anti-goals

**Goals**

- A checker built on the existing `oracle/` harness: the epic's suite list, a reference baseline
  that is stable across runs and clusters, a per-suite FE↔BE traffic corpus replayed in
  `cargo test`, a MySQL reply proxy with probe marks, a needs index, and ratchet scores.
- A doris-rs BE that joins a cluster run by the Java FE, creates and drops tablets, takes writes,
  executes plans, and passes every suite in `suites/epic1.txt` with identical replies.
- A readback check proving the C++ BE reads what doris-rs wrote, which is also the rollback.

**Non-goals**

- Replacing the FE, the Postgres protocol, storage-compute separated (cloud) mode.
- Clusters with C++ and doris-rs BEs serving queries together; multi-BE and multi-replica suites.
- Features owned by the other epics: text and NGram indexes and `VARIANT` (#5); aggregate-key
  tables, `BITMAP`, `HLL`, `QUANTILE_STATE`, `AGG_STATE` (#6); AI functions and vector search
  (#7); external catalogs (#8). Also `ARRAY`, `MAP`, `STRUCT`, `JSONB` and Arrow Flight SQL.
- Recorded agent sessions (deferred to their own issue).
- The p1 TPC-H and SSB suites (`tpch_sf0.1_p1` and friends), which load from Doris's S3 bucket.
  They are a possible follow-up.
- Comparing HTTP replies (stream load JSON) in the strict gate; a named follow-up (§4.5).
- Performance work beyond what keeps the suites inside their timeouts.

**Anti-goals** (failure even if the numbers look good)

- Relaxing a comparison to hide doris-rs differences. Relaxations come only from variation the
  reference itself shows; none is hand-written to make a suite pass.
- Counting FE-only wins as BE progress (§4.7 reports both numbers).
- doris-rs crashing on something it does not support (§5.4).
- doris-rs needing Docker or any other service to run.
- `cargo test` passing while suites fail. The suite run is the truth; the traffic replay only
  makes it quicker to get there.
- Data written by doris-rs that the C++ BE cannot read.

## 4. Milestone 1: the checker

Milestone 1 has no doris-rs engine code. It extends `oracle/` so that every later milestone is
graded the same way. Harness code stays standard-library Python, like the rest of `oracle/`.

### 4.1 The epic's suite list

A script computes `oracle/suites/epic1.txt` (one suite per line) from `p0-core` and commits it. A
suite is in when it passed on the reference (§4.2) and uses none of:

- the features listed under non-goals;
- BE debug points (`enableDebugPointForAllBEs` and the other debug-point helpers), which name
  spots in the C++ code that doris-rs cannot honor without copying its internals.

BE HTTP endpoints an operator uses (stream load, `/api/compaction/run`, `show_config`, ...) stay
in scope. The script also writes, next to the list, the excluded suites grouped by reason, so the
count dropped for each reason is visible.

### 4.2 A stable reference baseline

The current baseline is a single run. The epic list is run against the reference four times:
twice on each of two freshly created clusters, one of them the differently deployed cluster of
§4.4. A suite counts only if it passes all four runs; the others go to `suites/ignore.txt` with
the run ids as the reason. These same runs, with the proxy on (§4.5) and the capture sidecar on
(§4.3), produce the probe marks and the traffic corpus, so the reference is run once per purpose
only when the version changes.

The server configuration stays the one the harness already uses, matching upstream's p0 CI.

### 4.3 Per-suite traffic corpus

The capture sidecar from the wire corpus records all traffic between the FE and the reference BE
during a baseline run. An extension of `oracle/wire/bin/extract.py` splits it per suite and
writes, in order, every FE→BE message and the BE's reply: heartbeats and reports, agent tasks
(create, drop, publish-version, ...), load and tablet-writer RPCs, `exec_plan_fragment` (or
`_prepare` and `_start`), `fetch_data`, `reportExecStatus`, `cancel_plan_fragment`. Stream-load
HTTP bodies are kept too. How messages are linked to suites (time windows, query ids, FE audit
log) is the builder's choice.

Every message must decode and re-encode byte for byte with `doris-thrift` and `doris-proto`, as
the wire corpus does today.

`cargo test` feeds each suite's FE→BE messages to an in-process doris-rs BE and compares its
replies with the reference's, after the same placeholders and probe marks as §4.5 (ids, times and
addresses vary). For `fetch_data`, the rows compared are the concatenation of all batches after
decompression, not the batch boundaries, which depend on timing. This is the fast inner loop; it
never replaces the suite run.

### 4.4 Deployment facts

Some replies describe the BE node rather than the data: the address that prefixes BE errors
(`(127.0.0.1)[CODE]message`), `SHOW BACKENDS` (address, ports, CPU cores, memory, disk capacity),
and the `information_schema` tables the BE serves from its own state (`backend_configuration`,
`rowsets`, `backend_tablets`). The expected reply for doris-rs is the one a C++ BE deployed in its
place would send.

- Each cluster declares its BE's deployment facts in a file: address, the four ports, CPU cores,
  memory limit, data paths, disk capacity.
- Before comparing, every declared fact is replaced by a named placeholder (`<be-host>`,
  `<be-port>`, ...) on both sides.
- The substitution is proven on the reference: one baseline cluster is deployed with every
  declared fact different on purpose (address, ports, memory limit and, where Docker allows, the
  number of visible cores). After substitution its replies must equal the other cluster's
  exactly. If they do not, a fact is missing from the declaration; the fix is to declare it,
  never to mask it.
- doris-rs always reports its own real facts. It never impersonates the reference's node.

`parallel_pipeline_task_num` is pinned in the shared settings, because by default the FE derives
fragment parallelism from the cores the BE reports, and plans would then depend on the machine.

### 4.5 Strict reply gate

The framework's MySQL connections go through a recording proxy (`oracle/`, standard-library
Python). The suites are not changed. For each suite the proxy stores every reply's raw packets and
decodes them into a canonical form:

- OK packet: affected rows, last insert id, status flags, warning count, info.
- ERR packet: error code, SQLSTATE, message.
- Result set, text protocol and binary (prepared-statement) protocol: every field of every column
  definition (catalog, schema, table, org_table, name, org_name, character set, length, type,
  flags, decimals); every row as raw value bytes or NULL; the terminating status flags and warning
  count.

Two replies are equal when their canonical forms are equal field by field, except for the
deployment placeholders (§4.4) and the probe marks. The marks come from the four baseline runs: a
part of a reply that differs between any of them is marked as varying:

- row order varies, the multiset does not: the statement compares as a multiset;
- a column's values vary: that column is masked for that statement;
- an error message varies (for example a query id in it): the varying spans are masked;
- the whole reply varies: the statement is marked unstable and reported apart from the score.

The marks are stored with the recording, with the runs that produced them. They are the only
relaxations the comparison ever applies.

The gate is per suite: a suite that passes its `.out` check is then held to byte-equal replies in
the same milestone. HTTP replies (stream-load JSON with labels, transaction ids and timings) are
not compared yet; that is a named follow-up with its own placeholders.

### 4.6 Needs index and triage

From the traffic corpus, a script builds a needs index: per suite, the RPCs and agent-task types,
plan node and data sink types, expression and function names, and column types the reference BE
handled. doris-rs declares in code which of these it supports. The script ranks the unsupported
features by how many epic suites each one alone blocks, and by how many it blocks together with
others. The index is committed with the corpus, and each milestone's plan starts from its ranking.

### 4.7 Scores

`make check TARGET=rust SUITES=suites/epic1.txt` stays the entry point. Scores are ratchets:
`oracle/scores/` lists the suites known to pass on doris-rs, at the `.out` level and at the strict
level. The check fails if any of them fails, and says so when new ones start passing so the files
can be updated. Two numbers are reported: all epic suites, and the suites that sent at least one
fragment or load to the BE (known from the traffic corpus), so FE-only suites do not inflate
progress.

**Milestone 1 is done when** `epic1.txt` and its exclusion report are committed; the four
baseline runs ran and flaky suites are in `ignore.txt`; the deployment-fact substitution is proven;
the proxy records every suite and the marks are stored; the traffic corpus is extracted and every
message round-trips byte for byte; the needs index and its first ranking are committed; and the
reference replays against itself at 100% on both gates.

## 5. The doris-rs BE

### 5.1 Crates and binary

- `doris-exec`: decodes plans, runs operators, formats result rows.
- `doris-storage`: the tablet store. Creates tablets, writes and reads segments with
  `doris-segment`, applies delete bitmaps, serves snapshots.
- `doris-be`: the services. HeartbeatService and BackendService (Thrift servers),
  PBackendService (gRPC on the brpc port), the FrontendService client (`report`, `finishTask`,
  `reportExecStatus`, schema-scan calls, `streamLoadPut`, transactions), the HTTP server, and agent
  tasks.
- The binary: `doris-rs be --config <file>`. One file, one command.

### 5.2 Executor

doris-rs has its own vectorized executor, with in-memory column layouts modeled on the C++ BE's,
not DataFusion. The FE has already resolved every type, cast and function signature, so the BE
evaluates typed expression trees. Doris's own semantics (decimal precision rules, `NULL` handling,
function behavior) and, later, block exchange and aggregate states shared with C++ BEs point to
Doris's own layouts.

Results are formatted exactly as the C++ BE's MySQL result writer (`vmysql_result_writer`,
`MysqlRowBuffer`) formats them: numbers, decimals at their scale, dates, datetimes at their
precision, strings, NULL.

Floating-point results are compared exactly. Where the reference's result is stable, doris-rs
matches it bit for bit, which may require matching the reference's summation order; any case that
proves impractical is raised with the user, never masked.

### 5.3 Joining the cluster

doris-rs answers heartbeats with the same fields a C++ BE sends, including the reference's
version string and its own deployment facts (§4.4), and reports disks and tablets the way the C++
BE does: `SHOW BACKENDS`, `SHOW TABLETS` and `SHOW DATA` are visible to clients, so the sizes and
counts must match. The wire corpus's heartbeat and report messages are the templates.

Ports are configurable; the defaults are the C++ BE's. In the oracle setup doris-rs runs natively
on the host. The networking between the FE container and the host (addresses the FE registers and
hands out) is the builder's to settle, for macOS with OrbStack and for Linux.

`information_schema` queries run on the BE as schema scans calling back into the FE over Thrift;
doris-rs implements the scanners the suites use.

### 5.4 What doris-rs does not support yet

An agent task, RPC, plan node, function or type doris-rs does not support fails with a Doris error
status the FE accepts, and the message names the missing feature. It never panics or exits: a
crash makes every later suite incomplete and hides progress. The named feature also cross-checks
the needs index.

### 5.5 Write path and readback

The write path covers tablet creation and dropping; `INSERT` through the table sink and tablet
writers; stream load (HTTP on the BE, `streamLoadPut`, transaction begin and commit,
publish-version tasks); the segment writer; unique-key merge-on-write delete bitmaps; and, as
suites need them, compaction and schema change. Segments doris-rs writes must be readable by the
C++ BE and give identical replies; byte-identical files are not required.

Suites drop their own tables, so they cannot prove that. A fixed **readback check** does: doris-rs
loads a fixed dataset (TPC-H at scale factor 0.01 from `tpchgen-cli`, pinned; `orders` and
`customer` as merge-on-write unique-key tables with a second, deterministic update load), through
both stream load and `INSERT`. A C++ BE is added and doris-rs is decommissioned
(`ALTER SYSTEM DECOMMISSION BACKEND`), so doris-rs serves the snapshots the clone downloads. A
fixed set of queries then runs on the C++ BE and must give the same replies as on a reference
cluster loaded the same way.

This is also the rollback: moving replicas back to a C++ BE. It lands in the milestone that adds
unique-key merge-on-write, or at the latest in the one that completes the write path. Until then
there is no way to move data back; on the test cluster that costs nothing, because suites recreate
their own data.

## 6. Milestones

Each milestone is its own plan, branch and pull request, and leaves `cargo test`, the Python tests
and the reference self-check green.

| # | Milestone | Done when |
|---|---|---|
| 1 | The checker (§4) | §4.7's list holds |
| 2 | Join and hold tablets | doris-rs joins the cluster and its heartbeats and reports match; create-tablet and drop-tablet tasks work; every epic suite whose BE traffic is only those passes on both gates |
| 3 | C++-compatible data directory | tablet headers live in `<root>/meta` in the C++ BE's RocksDB format, written and read by a pure-Rust store with no RocksDB dependency; the swap check holds (a C++ BE and doris-rs each start on the other's disks and see the same tablets). Design: `2026-11-04-epic-1-m2-join-and-hold-tablets-design.md` §7, ADR 0003 |
| 4 | The write loop | `INSERT … VALUES` and stream load (CSV, JSON) into duplicate-key tables, under FE transactions and publish, as segment files the C++ BE reads; the readback check's first form holds; every epic suite needing only these passes on both gates. Design: `2026-11-05-epic-1-m4-write-loop-design.md` |
| 5 | The read loop | scans, filters, projections, `LIMIT`, sort, the materialization node and the result sink; every epic suite needing only these and M4 passes on both gates |
| 6+ | Chosen from the needs index | each starts from the ranking at the end of the previous one; its plan names the features and the suites they unlock |

The readback check (§5.5) is part of the milestone it lands in. The epic is done when every suite
in `epic1.txt` passes on both gates and the readback check holds.

## 7. Risks to watch early

- **The scope script misses a feature.** A suite that uses another epic's feature stays in the
  list and blocks; the needs index shows it, and the fix is to extend the script, not to ignore
  the suite.
- **A deployment fact nobody declared.** The differently deployed baseline cluster exposes it
  before anything is compared against doris-rs.
- **The reference varies in ways four runs do not show.** Flaky statements are reported
  separately and investigated, never masked by hand.
- **Floating-point results depend on evaluation order** (§5.2).
- **Background FE work.** Auto analyze and the audit loader write to tablets; they are harmless
  now that writing comes first, but they add traffic the corpus must link to no suite.
- **No rollback before the readback check** (§5.5).
- **Upstream CI settings** (debug points, legacy types, MTMV, binlog) pull features into scope;
  the needs index makes each one visible before it is planned.
- **No secrets in the repo.** It is public.

## 8. Left to the builder

File and function names; the canonical-form encoding; how marks are stored; how messages are
linked to suites (§4.3); container–host networking (§5.3); the order of work inside a milestone;
which crates to depend on inside the Rust workspace (well-known crates such as `tokio`, `tonic`,
`prost` and `thrift` are fine); how the readable diff looks; the query set of the readback check.

## 9. Calls made by the designer (not by the user)

1. doris-rs has its own executor, not DataFusion.
2. doris-rs-written segments must be readable and give identical replies, not be byte-identical.
3. Harness code stays standard-library Python.
4. Deployment facts are substituted on both sides and proven on a differently deployed reference
   cluster; fragment parallelism is pinned.
5. The readback dataset is TPC-H at scale factor 0.01 with a unique-key update load.
6. Fragment rows are compared as the concatenation of all batches.
