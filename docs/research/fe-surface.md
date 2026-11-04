# The FE's surface and what the suites exercise

Research for #17 (child of map #14). Date: 2026-11-04.

**Question.** How big is the Java FE at Doris 4.1.5 by area, which parts do the upstream gate
suites exercise, and what is a natural split of the FE track into epics?

**Sources.** All numbers come from the pinned source at `.cache/doris-src` (commit `917b20be838`,
"add 4.1.5 release") and the Epic 1 gate on branch `origin/epic1-m1-checker`
(`oracle/epic1/suites.txt`, 1,622 suites, and `oracle/epic1/needs.json`). Paths below are relative
to `fe/fe-core/src/main/java/org/apache/doris/` unless they start with `fe/` or `regression-test/`.
"LOC" means physical lines (`wc -l`) including comments and license headers, so treat them as
upper bounds on real code (the 16-line license header alone is ~83K of fe-core's 5,186 main files).

**Method and caveats.** Sizes come from `find … -name '*.java' | xargs cat | wc -l` per directory.
Suite usage comes from a heuristic scan: each gate suite id was resolved to its `.groovy` or
`.sql` file (all 1,622 resolved), every string literal was split on `;`, and the leading keywords
classified (`SELECT`, `CREATE TABLE`, `SHOW ALTER`, …). Feature detectors are regexes (e.g.
`create\s+catalog`, `\bexplain\s*\{`). Expect a few percent noise (e.g. `INSERT RESULT` from
strings that happen to start with "insert"); the counts are for ordering, not contracts. The same
scan was run over all 6,131 `*_p0` suite files for contrast.

## 1. Size of the FE

The FE is Maven multi-module (`fe/pom.xml`). Main-source LOC per module:

| Module | LOC |
|---|---:|
| `fe/fe-core` | 960,087 |
| `fe/be-java-extensions` (JNI scanners, UDF runtime; runs inside the BE) | 26,394 |
| `fe/fe-common` (incl. `Config.java`, 723 `@ConfField`s) | 19,978 |
| `fe/fe-authentication` | 4,985 |
| others (`fe-foundation`, `fe-benchmark`, `hive-udf`, extension SPI/loader) | ~5,500 |
| **total main** | **~1,017,000 in 5,455 files** |
| `fe/fe-core/src/test` (unit tests, not ported) | 377,413 |

Note: `fe/fe-common/.../common/Config.java` (4,260 lines, 723 `@ConfField`s) is FE config, a separate surface from
session variables and is touched by the suites only through `ADMIN SET FRONTEND CONFIG`
(30 gate suites) and `SHOW CONFIG` (16).

### 1.1 fe-core by top-level package (LOC, files)

| Package | Files | LOC | What it is |
|---|---:|---:|---|
| `nereids/` | 2,669 | 393,168 | the optimizer: parser, analyzer, rules, memo, cost, trees, commands |
| `datasource/` | 515 | 118,544 | catalogs: `InternalCatalog.java` (4,109) + external (Hive, Iceberg, Paimon, JDBC, ES, …) |
| `catalog/` | 147 | 50,974 | metadata objects (`OlapTable` 4,023, `Env` 7,693), builtin function registry |
| `common/` | 241 | 39,913 | utilities, proc nodes, errors |
| `analysis/` | 228 | 37,901 | legacy analyzer: `Expr` tree still used to emit Thrift `TExpr`, descriptors |
| `qe/` | 89 | 31,660 | session, `StmtExecutor` (2,409), `Coordinator` (3,883), `SessionVariable` (6,948) |
| `cloud/` | 70 | 26,477 | storage/compute-separated mode |
| `load/` | 79 | 19,466 | routine load (6,725), broker/S3 `loadv2` (5,650), delete, export, group commit |
| `mysql/` | 96 | 16,335 | MySQL protocol (9 K) + `privilege/` (9,095) + `authenticate/` (3,050) |
| `planner/` | 79 | 16,045 | legacy `PlanNode`s: Thrift plan output and the EXPLAIN text format |
| `statistics/` | 81 | 15,695 | ANALYZE jobs, stats cache, auto-analyze |
| `httpv2/` | 83 | 13,701 | FE HTTP API (Spring) |
| `persist/` + `journal/` + `ha/` + `master/` | 117 | 20,835 | edit log, BDB-JE, image, FE HA |
| `job/` | 55 | 12,742 | job scheduler: insert jobs (4,891), MTMV jobs (1,173), streaming |
| `plsql/` | 74 | 12,428 | stored procedures |
| `alter/` | 19 | 11,628 | schema change, rollup, partition ops |
| `clone/` | 18 | 10,639 | tablet scheduler / balancer / repair |
| `service/` | 26 | 10,103 | Thrift `FrontendServiceImpl` (5,506): BE→FE RPCs |
| `tablefunction/` | 34 | 8,808 | TVFs (`s3()`, `local()`, `numbers()`, metadata TVFs) |
| `backup/` | 15 | 7,855 | backup/restore |
| `transaction/` | 36 | 7,806 | load transactions, publish version |
| `mtmv/` | 47 | 6,723 | async materialized-view metadata and refresh |
| `fs/`, `resource/`, `task/`, `system/`, `metric/`, `binlog/`, `plugin/`, … | ~250 | ~50,000 | storage FS, resources, BE tasks, backend registry, metrics, long tail |

### 1.2 Nereids by sub-area

| Area | Where | Files | LOC | Counts |
|---|---|---:|---:|---|
| Grammar | `fe/fe-core/src/main/antlr4/.../nereids/DorisParser.g4` + `DorisLexer.g4` | 2 | 3,174 | 509 labeled alternatives; 24 statement families in `statementBase`; 95 `SHOW` forms, 29 `CREATE`, 29 `ALTER` (+29 `ALTER TABLE` clauses), 27 `DROP`, 24 `ADMIN`, 17 load, 17 stats |
| Grammar (PL/SQL) | `PLParser.g4`, `PLLexer.g4` | 2 | 1,162 | stored procedures only |
| Parser → plan | `nereids/parser/` | 17 | 12,313 | `LogicalPlanBuilder.java` alone is 10,334 |
| Analyzer | `nereids/analyzer/` + `nereids/rules/analysis/` | 77 | 18,466 | binding (`BindExpression` 2,077), type coercion, subquery analysis; RuleType CHECK 5 |
| Expression rewrite | `nereids/rules/expression/` | 86 | 14,955 | 62 expression rules (fold constant, simplify, range inference) |
| Plan rewrite (RBO) | `nereids/rules/rewrite/` + `jobs/rewrite/` + `jobs/executor/Rewriter.java` | 209 | 38,249 | **346 REWRITE RuleTypes**; `Rewriter` runs 62 named topics |
| Exploration (CBO) | `nereids/rules/exploration/` minus `mv/` | 23 | 2,559 | 36 EXPLORATION RuleTypes (join commute/assoc, agg split) |
| Implementation | `nereids/rules/implementation/` | 55 | 4,716 | 65 IMPLEMENTATION RuleTypes (logical → physical) |
| Memo + cascades jobs | `nereids/memo/`, `nereids/jobs/` (minus rewrite/joinorder) | ~35 | ~5,800 | `jobs/cascades/` (optimize group, apply rule, cost and enforce, derive stats) |
| Join reorder | `nereids/jobs/joinorder/` (DPhyp hypergraph) + `exploration/join/` | 32 | 5,325 | gated by `disable_join_reorder`, `enable_dphyp_optimizer`, `LEADING` hints |
| Cost model | `nereids/cost/` | 6 | 1,034 | `CostModel`, `CostWeight` — small, but every constant shapes plans |
| Stats derivation | `nereids/stats/` | 14 | 6,649 | `StatsCalculator` 1,954, `FilterEstimation` 1,181 |
| Properties / enforcers | `nereids/properties/` | 43 | 6,533 | distribution and order requirements, exchange placement |
| MV rewrite | `nereids/rules/exploration/mv/` | 59 | 12,154 | 50 MATERIALIZE_VIEW RuleTypes |
| Post-processing | `nereids/processor/` | 37 | 6,282 | 26 post processors (runtime filter generation, topN filter, CTE) |
| Logical/physical plan nodes | `nereids/trees/plans/{logical,physical,algebra,visitor}` | 185 | 33,185 | |
| Distributed planning | `nereids/trees/plans/distribute/` | 48 | 5,225 | fragment/instance assignment |
| Physical → Thrift | `nereids/glue/translator/` | 5 | 5,541 | `PhysicalPlanTranslator` 3,722; feeds `planner/` + `analysis/` legacy classes |
| Expressions (non-function) | `nereids/trees/expressions/` minus `functions/` | 154 | 22,321 | literals 8,385 |
| Functions | `nereids/trees/expressions/functions/` | 842 | 80,856 | scalar 596 files (52 K), agg 92, window 16, generator 30, table 28, UDF 15, FE-side folding `executable/` 6,602 |
| Commands (statements) | `nereids/trees/plans/commands/` | 503 | 71,680 | **330 `*Command` classes**: 112 Show, 36 Drop, 31 Create, 30 Alter, 20 Admin, 10 Cancel, … ; `info/` 117 files of DDL descriptors |
| Types | `nereids/types/` | 54 | 5,993 | |

`nereids/rules/RuleType.java` holds 503 rule types: 346 REWRITE, 65 IMPLEMENTATION, 50
MATERIALIZE_VIEW, 36 EXPLORATION, 5 CHECK, 1 VALIDATION (binding rules are counted under
REWRITE in that enum).

The builtin function registry (`catalog/BuiltinScalarFunctions.java` etc.) registers 499 distinct
scalar classes (570 entries with aliases), 73 aggregate, 11 window, 28 table-generating and 26
table-valued functions.

### 1.3 Session variables, FE HTTP, Thrift services

- **Session variables:** `qe/SessionVariable.java` has 483 `@VarAttr` fields (86 marked
  `fuzzy = true`, i.e. randomized by the upstream fuzzy test mode); `qe/GlobalVariable.java` adds
  32. ~470 distinct name constants.
- **FE HTTP API:** `httpv2/` has 54 controllers and 137 `@RequestMapping/@GetMapping/@PostMapping`
  annotations over ~105 distinct path templates (`/api/show_proc`, `/api/_get_ddl`,
  `/rest/v2/manager/*`, `/api/{db}/{table}/_stream_load`, `/metrics`, …).
- **BE→FE Thrift:** `service/FrontendServiceImpl.java` (5,506 LOC) serves the BE's callbacks
  (task reports, load transactions, information_schema fetches).

## 2. What the gate suites exercise

The gate (1,622 suites) is the p0 suites that reach the BE, from 13 directories: datatype_p0 457,
nereids_p0 393, query_p0 349, correctness_p0 102, load_p0 96, unique_with_mow_p0 71,
schema_change_p0 45, variant_p0 28, compaction 27, insert_p0 24, inverted_index_p0 18,
point_query_p0 10, mysql_compatibility_p0 2. 545 of them are plain `.sql` files (one query plus
`.out`); 1,077 are Groovy.

### 2.1 Statement kinds

Suites using each statement kind (gate of 1,622 / all p0 of 6,131):

| Kind | Gate | All p0 |
|---|---:|---:|
| SELECT / WITH | 1,320 | 4,727 |
| CREATE TABLE | 857 | 4,204 |
| DROP TABLE | 853 | 3,867 |
| INSERT INTO | 704 | 3,393 |
| SET (session variable) | 534 | 2,568 |
| SYNC | 177 | 651 |
| USE | 161 | 1,198 |
| ALTER TABLE | 92 | 840 |
| CREATE / DROP DATABASE | 90 / 60 | 593 / 382 |
| DELETE / UPDATE / TRUNCATE | 46 / 15 / 46 | 286 / 181 / 197 |
| SHOW ALTER / SHOW TABLETS | 41 / 36 | 254 / 226 |
| LOAD (broker/S3) | 32 | 178 |
| CREATE / DROP VIEW | 30 / 26 | 119 / 103 |
| ADMIN SET (FE config) | 30 | 140 |
| ROUTINE LOAD (create/show/stop/…) | 23 | 49 |
| EXPLAIN SHAPE PLAN | 21 | 107 |
| ANALYZE | 20 | 331 |
| CREATE / DROP MATERIALIZED VIEW | 14 / 10 | 392 / 315 |
| CREATE USER / GRANT | 14 / 11 | 157 / 149 |
| BEGIN / COMMIT | 9 / 9 | 47 / 37 |
| CREATE / DROP CATALOG, SWITCH | 0 | 512 / 514 / 325 |
| CREATE / DROP JOB | 0 | 132 / 132 |
| CREATE FUNCTION (UDF) | ~6 | 107 |

The gate touches ~160 distinct statement kinds (78 in 3+ suites); all p0 touches ~320 (188 in
3+ suites), against 330 command classes and 509 grammar alternatives.

**Tiering.** Statement coverage needed to fully run a suite (statements only; functions, types
and plan features are separate):

| Statement set | Gate suites runnable | All p0 |
|---|---:|---:|
| T1: SELECT, CREATE/DROP TABLE/DATABASE/VIEW, INSERT (incl. OVERWRITE), DELETE, UPDATE, TRUNCATE, SET/UNSET, USE, SYNC, DESC, plain EXPLAIN | 1,227 (76%) | 3,109 (51%) |
| T2: T1 + ALTER TABLE/DATABASE/SYSTEM, ~20 SHOW forms, ADMIN SET/SHOW, ANALYZE, BEGIN/COMMIT/ROLLBACK, EXPLAIN shape/analyzed | 1,454 (90%) | 4,006 (65%) |
| rest of gate: broker LOAD 32, routine load 23, MTMV 14, users/grants 14, resources 7, UDF/alias 6–11, encrypt keys 4, workload groups 2 | 168 | |
| rest of all p0, led by: catalogs 514, MTMV 392, UDFs 176, users/grants ~157, jobs 132, cloud warm-up 70 | | 2,125 |

### 2.2 Features beyond statement kinds

| Feature (regex detector) | Gate | All p0 |
|---|---:|---:|
| stream load (`streamLoad {}` → FE `/api/{db}/{table}/_stream_load` → BE redirect) | 133 | 503 |
| ALTER … ADD/DROP/MODIFY COLUMN (schema change) | 62 | 571 |
| `SET_VAR(...)` hints | 55 | 156 |
| SHOW TABLETS / SHOW PROC / ADMIN SHOW | 44 | 289 |
| HTTP calls (curl/`httpTest`; mostly BE compaction endpoints) | 44 | 270 |
| ANALYZE / column stats | 30 | 371 |
| prepared statements / point query | 30 | 157 |
| information_schema | 22 | 129 |
| explicit transactions | 18 | 62 |
| SQL cache | 14 | 72 |
| privileges (CREATE USER/ROLE, GRANT) | 14 | 161 |
| TVFs `s3()/hdfs()/local()` | 8 | 215 |
| async MTMV / MV-rewrite assertions | 6 / 3 | 210 / 258 |
| index DDL (inverted, ngram, bloom) | 5 | 458 |
| external catalogs | **0** | 514 |
| jobs, backup/restore | 0 | 135, 33 |

**Session variables.** Gate suites set 119 distinct real session variables (all p0: 278 of ~470).
Most-set: `enable_nereids_planner` 293 suites, `enable_fallback_to_original_planner` 278 (both
effectively no-ops at 4.1.5), `parallel_pipeline_task_num` 48, `enable_decimal256` 40,
`enable_insert_strict` 33, `runtime_filter_type` 33, `time_zone` 33, `disable_join_reorder` 31,
`disable_nereids_rules` 29, `runtime_filter_mode` 24, `batch_size` 24, `debug_skip_fold_constant` 23,
`enable_unique_key_partial_update` 22, `enable_strict_cast` 20, `enable_fold_constant_by_be` 19,
`ignore_shape_nodes` 16. `disable_nereids_rules` (29 suites) names individual rules, so rule
identity (the `RuleType` names) is itself an interface the suites depend on.

**FE HTTP API.** Apart from the stream-load entry point, only ~10 gate suites call FE REST paths
directly. `needs.json` shows the BE-side traffic the FE must answer: `finishTask` 851 suites,
`reportExecStatus` 707, `streamLoadPut` 160, `loadTxnBegin/Commit` ~150, `fetchSchemaTableData` 76
(information_schema), `loadTxnRollback` 22, plus a tail of ~20 other `FrontendService` calls.

**What reaches the BE** (`needs.json`, 1,456 suites with data): 441 distinct functions (of ~640
FE builtin function classes), 21 plan node kinds (OLAP scan 1,259, exchange 1,197, sort 971,
union 737, agg 373, materialization 260, hash join 218, …), 6 sinks (OLAP table sink 702), BE
tasks CREATE 841, PUBLISH_VERSION 765, ALTER 41.

### 2.3 EXPLAIN and plan-shape checks

| Kind of check | Gate | All p0 | Notes |
|---|---:|---:|---|
| any EXPLAIN check (union of below + EXPLAIN statements) | **115 (7%)** | 392+ | |
| `explain { sql …; contains "…" }` substring assertions | 73 | 321 | check both the legacy text (`PREDICATES:`, `runtime filters`, `VTOP-N`, `TOPN OPT`) and Nereids tree strings (`PhysicalQuickSort`, `isMarkJoin`, `projects`) |
| `qt_… explain shape plan` with exact `.out` compare | 19 | 81 | |
| `.out` files containing `Physical*` plan text (exact shape) | 25 | 114 | |
| outside p0: `regression-test/suites/shape_check/` | — | 1,001 suites | 998 `.out` files of exact TPC-H/TPC-DS/SSB shapes; plus `nereids_rules_p0` 81 |

Gate directories carrying the shape checks: `nereids_p0/{stats,hint,join,runtime_filter,
aggregate,explain,cte,subquery,union,eager_agg,…}`, `query_p0/{explain,topn_lazy,cache}`.

Two consequences: (1) inside the Epic 1 gate, plan shape gates only ~7% of suites, so a planner
that produces correct results unlocks ~93% of the gate; (2) the full destination (p0 +
`shape_check` + `nereids_rules_p0`) carries ~1,200 exact-shape suites, and those depend jointly on
rewrite rules, the cost model, join reorder, stats derivation, and the statistics that ANALYZE or
`ALTER TABLE … MODIFY COLUMN … SET STATS` load. The EXPLAIN *text* format of the legacy
`planner/` nodes is also an interface (substring checks), even though a single Rust binary has
no Thrift boundary to render it from.

## 3. Candidate epics (an input, not a decision)

Sizes are Java LOC to be matched (upper bounds). Gate/p0 columns count suites that need the area.
Wire protocol (MySQL) is assumed covered by the existing wire epic.

| # | Candidate epic | Java scope (≈LOC) | Gate need | All p0 need | Notes |
|---|---|---:|---:|---:|---|
| F1 | **Metadata core**: internal catalog (db/table/partition/tablet/replica), CREATE/DROP/TRUNCATE, views, edit log + image (single FE), backend registry, BE task dispatch, load transactions + publish | catalog 51 K, InternalCatalog 4 K, Env 8 K, transaction 8 K, persist/journal 17 K, system/task 9 K ≈ **95 K** (HA, clone, cloud excluded) | ~860 | ~4,200 | precondition for everything; `finishTask`/`PUBLISH_VERSION` paths |
| F2 | **Nereids front half**: grammar + `LogicalPlanBuilder`, binder/analyzer, type coercion, function signatures, FE constant folding | parser 15 K, analysis 18 K, expressions 22 K, functions 81 K (mostly signature boilerplate), types 6 K ≈ **140 K** | 1,320 | 4,727 | 441 functions reach the BE in the gate; signature/return-type rules decide result types and so byte-equal replies |
| F3 | **Physical planning + execution handoff**: implementation rules, properties/enforcers, distribute planner, translator, post-processors (runtime filters, topN filter), coordinator, EXPLAIN text | implementation 5 K, properties 7 K, distribute 5 K, translator 6 K, processor 6 K, planner+legacy Expr ~30 K of 54 K, coordinator ~6 K ≈ **65 K** | 1,320 | 4,727 | with F1+F2 this gives a correct-results planner (minimal rewrites) |
| F4 | **DML and load**: INSERT/INSERT OVERWRITE, DELETE/UPDATE/MERGE, partial update, stream load FE side, group commit, broker/S3 load, routine load (Kafka), TVFs | insert 7 K, nereids/load 6 K, load 19 K, tablefunction 9 K, job/insert 5 K ≈ **46 K** | 704 INSERT, 133 stream load, 55 other loads | 3,400+ | stream load and INSERT first; routine load needs Kafka in the harness |
| F5 | **Rewrite (RBO) parity**: 62 rewrite topics, 346 rewrite rule types, 62 expression rules | ≈ **53 K** | results: all; shape: 115 | 392 + 1,200 shape | rule names are an interface (`disable_nereids_rules`, 29 gate suites) |
| F6 | **Cost-based optimizer parity**: memo, cascades jobs, exploration rules, DPhyp join reorder, cost model, stats derivation, hints (`LEADING`) | memo/jobs 6 K, exploration 3 K, join reorder 5 K, cost 1 K, stats derivation 7 K, hint 1 K ≈ **23 K** | ~115 (shape) | ~1,200 (shape) | small in LOC, high in fidelity risk: exact float costs and tie-breaks |
| F7 | **Statistics**: ANALYZE, stats cache, column-stats DDL, auto-analyze | statistics 16 K | 30 | 371 | pairs with F6: shapes depend on loaded stats |
| F8 | **Schema change and table maintenance**: ALTER TABLE columns/partitions/properties, rollup, sync MV, index DDL, auto partition | alter 12 K + parts of catalog/commands ≈ **20 K** | 92 | ~840 | BE-side ALTER tasks already in Epic 1 scope |
| F9 | **Session and introspection surface**: session/global variables, SHOW family, ADMIN, information_schema, SHOW PROC, FE config, SQL cache, prepared statements | SessionVariable 7 K, qe ~15 K, show commands ~20 K, proc nodes ~5 K ≈ **47 K** | 534 SET, ~150 SHOW/ADMIN | 2,568 SET | 119 variables in gate, 278 in p0; can be grown per suite |
| F10 | **Privileges and authentication** | privilege 9 K, authenticate 3 K, authorizer 2 K, fe-authentication 5 K ≈ **19 K** | 14 | ~160 | |
| F11 | **Async MTMV and MV rewrite** | mtmv 7 K, job/mtmv 1 K, exploration/mv 12 K ≈ **20 K** | 6 | ~400 | depends on F5/F6 |
| F12 | **External catalogs** (Hive, Iceberg, Paimon, JDBC, ES, …) | datasource minus internal ≈ **114 K** | 0 | 514 | most suites need external services; candidate for scoping out or late |
| F13 | **Long tail**: jobs scheduler, export/outfile, backup/restore, UDFs (Java/Python), workload groups, resources/storage policy, encryption keys, PL/SQL, FE HTTP API beyond stream load, FE HA (BDB), tablet scheduler/clone, cloud mode | ≈ **150 K+** | ~40 | ~700 | split later by demand |

**Possible order.** F1 → F2 → F3 (correct results end to end: ~76% of gate suites need only T1
statements) → F4 (stream load and INSERT unblock the bulk of fixtures) → F9 grown alongside →
F5 → F7 + F6 together (shape parity) → F8 → F10 → F11 → F12/F13. F5–F7 are where "EXPLAIN must
match" bites; doing them after a results-correct pipeline lets each rule port be checked against
`explain shape plan` output of the reference instead of designed blind.

## Open points for the map

- Whether "Nereids is effectively ported" means rule-by-rule (503 rule types, rule names exposed via
  `disable_nereids_rules`) or shape-equivalent; the suites can tell only the latter, except for
  rule names.
- The legacy EXPLAIN text (`planner/` nodes) is checked by substring in 73 gate suites; a Rust port
  must render that format without the Thrift plan it was generated from.
- External catalogs (114 K LOC, 514 p0 suites, 0 in gate) need a scope decision before sizing.
