# M5 research: aggregation and information_schema scans

Sources: `.cache/doris-src` (4.1.5-release), paths relative to it. In this tree the aggregate functions
live in `be/src/exprs/aggregate/` (not `be/src/vec/aggregate_functions/`), the schema scanners in
`be/src/information_schema/`, and the operators in `be/src/exec/operator/`. Corpus:
`oracle/epic1/traffic/*.bin.gz`, decoded with a throwaway Rust tool built against
`doris-wire`/`doris-thrift`/`doris-proto` (`<scratchpad>/m5/agg`, modes `dump <suite> [idx,..]` and
`shapes [suite]`, which prints one line per fragment with the agg/schema-scan details). Counts below are
over the 1622 gate suites (`oracle/epic1/suites.txt`) unless stated otherwise.

---

## B.3 first: the reference did run `test_query_sys_tables` to the end

* `oracle/epic1/baseline.json`: `nereids_p0.system.test_query_sys_tables` is `passed` in 126 ms
  (started 1793678636765, finished …891). `replies/…jsonl.gz` holds 65 exchanges on 2 connections: the
  harness connection plus the suite's own connection. **Every statement in the suite has a reply**, from
  `SET enable_nereids_planner` through `qt_views` (exchange 64). Exchange 54 is `SET GLOBAL wait_timeout = 31000`
  (OK) and exchange 55 is `global_variables` → `wait_timeout 31000`. 126 ms is just fast: the scans are
  metadata RPCs, and the 3 tables are empty.
* **That suite is where the 31000 comes from.** Sorting all 1622 reply files by baseline `started_ms`,
  the connect query's `@@wait_timeout` (exchange 1, column 20) reads `28800` for every suite up to and
  including this one (its own connect query reads 28800). From `nereids_p0.system.test_runtime_filter`
  (started 27 ms after it finished) on, it reads `31000`. Counts: 31000 in 809, 28800 in 730, and no
  connect query in 83.
  All 809 are gate suites, and all 809 are `passed` in the baseline.
* Twin: `query_p0/system/test_query_sys_tables.groovy:217` runs the same `SET GLOBAL`. Of the 809
  suites, 244 run between the twins, 1 is the query_p0 twin itself, and 564 run after it. If only the
  query_p0 twin passed, the 564 would still be fixed. But the twin's statements before line 217 are much
  heavier (`files`, `statistics`, `table_constraints`, `schema_privileges`, `table_privileges`,
  `partitions` → `fetchSchemaTableData`, `listSchemaPrivilegeStatus`, …), and its marks say `unstable`.
  **The nereids twin is the cheap way in.**
* Masking agrees: `marks.json` masks exchange-1 column 20 in 710 suites (those whose position relative to
  the SET differed across the 5 reference runs). It masks it in none of the 809.
* On doris-rs today (run `m4-full-2`, `oracle/results/rust/m4-full-2/teamcity.log:24591…`), the suite
  stops at its **first** BE query: `qt_charsets` (line 33).

## B.1 What before line 175 reaches the BE

Each statement and what it needs (record numbers are from the suite's traffic file):

| line | statement | BE work |
|---|---|---|
| 19-29 | `SET …`, `drop database IF EXISTS` ×3 | none |
| 32-33 | `select count(*) >= 1 from character_sets` | rec 2-11: 2 fragments, `exec_plan_fragment_prepare` + `_start`, `fetch_data` ×2, `cancel` |
| 36-37 | `… from collations` | rec 12-21: same shape |
| 41-88 | CREATE DATABASE ×3, CREATE TABLE ×3 | `submit_tasks` CREATE ×3 → `finishTask` ×3 (M2) |
| 89 | `qt_columns` (no ORDER BY) | rec 37 `exec_plan_fragment` (1 fragment); FE RPCs `getDbNames` ×1, `getTableNames` ×148, `describeTables` ×105 |
| 98-104 | CREATE DATABASE IF NOT EXISTS ×3, `qt_schemata` | rec 555 prepare+start (2 fragments); `getDbNames` ×1 |
| 110-165 | CREATE … IF NOT EXISTS (FE no-ops), `qt_tables` | rec 567 prepare+start (2 fragments); `getDbNames` ×1, `listTableStatus` ×149 |
| 170-171 | `SET wait_timeout = 30000`, `qt_session_variables` | rec 875 `exec_plan_fragment` (1 fragment); `showVariables` ×1 |
| 175 | `SET GLOBAL wait_timeout = 31000` | none (FE only) |

(Line 176 `global_variables` and everything after it, including `user_privileges`/`regexp`/`views`, is
past the gate line.)

### Plan shapes (decoded)

```
charsets/collations (rec 2 / 12):
  frag0 (1 instance): SCHEMA_SCAN#0{character_sets} -> AGG#1{need_finalize=false streaming=false first_phase=true
        group=[] count[update]() ret BIGINT inter BIGINT nullable=false} -> DATA_STREAM_SINK UNPARTITIONED -> 127.0.0.1:8060
  frag1: EXCHANGE#2 -> AGG#3{need_finalize=true first_phase=false colocate=true count[merge](slot5:VARCHAR(65533))}
        [proj ge(slot6:BIGINT, 1:BIGINT):BOOLEAN] -> RESULT_SINK
columns (rec 37): SCHEMA_SCAN{columns}[conj IN_PRED(TABLE_SCHEMA:VARCHAR(64), 'test_query_sys_db_4','…_5','…_6')]
        [proj TABLE_CATALOG, COLUMN_NAME, ORDINAL_POSITION:BIGINT, DATA_TYPE, COLUMN_TYPE, COLUMN_SIZE:BIGINT] -> RESULT_SINK
schemata (rec 555) / tables (rec 567):
  frag0: SCHEMA_SCAN[conj IN_PRED(...)][proj …] -> SORT_NODE{FULL_SORT, asc, nulls_first, merge_by_exchange=true,
        use_local_merge=true} -> DATA_STREAM_SINK UNPARTITIONED
  frag1: EXCHANGE{sort_info same key, offset 0} (merging exchange) -> RESULT_SINK
session_variables (rec 875): SCHEMA_SCAN{session_variables, db="SESSION"}[conj eq(VARIABLE_NAME, 'wait_timeout')]
        [proj VARIABLE_NAME, VARIABLE_VALUE] -> RESULT_SINK
```

Expressions needed before line 175: `IN_PRED` (VARCHAR vs string literals), `eq` (VARCHAR), `ge` (BIGINT vs
INT_LITERAL), `SLOT_REF`; plan nodes SCHEMA_SCAN, AGGREGATION (count only), EXCHANGE (plain and merging),
SORT. The needs index's `regexp` and `listUserPrivilegeStatus` come from `qt_user_privileges` (line 182),
and its `views` scan from line 191: both are after line 175 and not needed for the gate. **`count`/`ge`/
AGGREGATION_NODE are needed: `qt_charsets` and `qt_collations` (lines 33, 37) are two-phase `count(*)`.**

### TSchemaScanNode (identical across the suite's scans, except table_name/db)

`tuple_id 0, table_name "<t>", db None (SESSION/GLOBAL for the variables scans, the db name for views),
table/wild/user/user_ip None, ip "127.0.0.1", port 9020, thread_id 1855 (the MySQL connection id),
current_user_ident {username "root", host "%", is_domain false}, catalog "internal",
fe_addr_list [127.0.0.1:9020], frontend_conjuncts "[]"`.

The tuple's table descriptor is `TTableType::SCHEMA_TABLE` with `schema_table.table_type` = TSchemaTableType:
CHARSETS 1, COLLATIONS 2, COLUMNS 4 (24 cols), GLOBAL_VARIABLES 11, SCHEMATA 20 (6), SESSION_VARIABLES 23 (4),
TABLES 26 (21) (`gensrc/thrift/Descriptors.thrift:149-182`). The tuple holds **all** of the table's columns as slots;
the node's projections pick the selected ones.

C++ (`exec/operator/schema_scan_operator.cpp`): `:79-146` copies the node fields into the common params,
falling back to `{ip, port}` when `fe_addr_list` is unset. `:177-207` maps each tuple slot to a scanner
column by **case-insensitive `col_name`**, errors if a primitive type differs, and builds
`required_columns` (upper-cased slot names; the tables scan sends it to the FE). The scanner is created by
type (`information_schema/schema_scanner.cpp:202-216`); SESSION_VARIABLES/VARIABLES → SESSION.
Conjuncts and projections run on the scanner's blocks like on any scan.

### Scanners and their FrontendService RPCs

All of these are plain Thrift calls to `ip:port` (`schema_helper.cpp:41-80`, `ThriftRpcHelper::rpc<FrontendServiceClient>`).
Every request sends `current_user_ident` (and not user/user_ip) when it is set.

| scanner (file) | RPCs | rows |
|---|---|---|
| charsets (`schema_charsets_scanner.cpp:39`) | none | 1 static row: `utf8mb4, utf8mb4_0900_bin, "UTF-8 Unicode", 4`; eos after 1 block |
| collations (`schema_collations_scanner.cpp:41`) | none | 1 static row: `utf8mb4_0900_bin, utf8mb4, 309, Yes, Yes, 1` |
| columns (`schema_columns_scanner.cpp:74-104, 304-393`) | `getDbNames{catalog, current_user_ident, pattern=db}` once at start; then per db `getTableNames{db, catalog=catalogs[i], current_user_ident, pattern=table}`; per batch of **≤100 tables** `describeTables{db, tables_name, catalog, current_user_ident, show_hidden_columns=false}` | one block per describeTables batch; dbs with 0 tables are skipped |
| schemata (`schema_schemata_scanner.cpp:49-160`) | `getDbNames` (pattern = `wild`) | 1 block of all dbs: catalog, name (after `:` via `extract_db_name`), `utf8mb4`, `utf8mb4_0900_bin`, SQL_PATH NULL, `NO` |
| tables (`schema_tables_scanner.cpp:71-139, 360-377`) | `getDbNames`; per db `listTableStatus{db, catalog, current_user_ident, required_columns}` | one block per db |
| session/global variables (`schema_variables_scanner.cpp:49-147`) | `showVariables{threadId=thread_id, varType=SESSION or GLOBAL from db}` | `variables[i][0..3]` → NAME, VALUE, DEFAULT, CHANGED; error if a row has <4 fields |

Decoded replies (rec numbers from the suite):

* `getDbNames` reply (rec 35): `TGetDbsResult{dbs [~148 names: every db in the cluster, in FE hash
  order], catalogs ["internal"…], db_ids, catalog_ids [0…]}`. `catalogs` is set, so TABLE_CATALOG /
  CATALOG_NAME = `internal`.
* `getTableNames` (rec 461→901): `{db "test_query_sys_db_4", catalog "internal"}` → `tables ["test_query_sys_tb_4"]`.
* `describeTables` (rec 480 → reply at dump line 131): `tables_offset [4]`, columns
  `aaa VARCHAR len 170 not-null key "DUP"`, `bbb VARCHAR len 20`, `ccc INT len 10 prec 10 scale 0 nullable`,
  `ddd SMALLINT len 5 prec 5 scale 0 nullable`, comments `""`. `tables_offset[k]` is the cumulative
  end index of table k's columns. ORDINAL_POSITION restarts at 1 at each offset (`:443-455`).
* `listTableStatus` (rec 842): `required_columns {AVG_ROW_LENGTH, INDEX_LENGTH, MAX_DATA_LENGTH,
  TABLE_CATALOG, TABLE_NAME, TABLE_SCHEMA, TABLE_TYPE}` → `TTableStatus{name, type_ "BASE TABLE",
  comment "", avg_row_length 0, index_length 0}`, all else unset. MAX_DATA_LENGTH is always NULL in
  the scanner (column 10 = null_datas), which matches the expected `\N`.
* `showVariables` (rec 874/876): `{thread_id 1855, var_type SESSION}` → rows like
  `["wait_timeout","30000","28800","1"]` (changed rows first); GLOBAL → `["wait_timeout","31000","28800","1"]`.

Expected column values, computed from describeTables (`schema_columns_scanner.cpp:109-290`):
`DATA_TYPE` from `_to_mysql_data_type_string` (VARCHAR/STRING → `varchar`, INT → `int`, SMALLINT → `smallint`, …);
`COLUMN_TYPE` from `_type_to_string` (`varchar(<len>)`, `int(11)`, `smallint(6)`, `bigint(20)`, `tinyint(4)`,
`decimalv3(p, s)`, `datetime(s)`…); `COLUMN_SIZE` = `columnLength` if set, else NULL (INT → 10, SMALLINT → 5).
The expected rows (`data/nereids_p0/system/test_query_sys_tables.out`) are:
`internal aaa 1 varchar varchar(170) 170 / bbb 2 varchar varchar(20) 20 / ccc 3 int int(11) 10 / ddd 4 smallint smallint(6) 5`, ×3.

## B.3 Risks outside the BE (before line 175)

* **The columns scan walks every db and table in the cluster.** In the reference that was ~148 dbs and
  253 RPCs in ~30 ms. On doris-rs the set of dbs depends on what earlier suites created. The result does not depend on it
  (the IN_PRED filters it), but one failing `describeTables`/`getTableNames` aborts the scan, and so the
  suite. Any FE error on some other suite's table would break it.
* `qt_columns` has no ORDER BY. The output order is getDbNames order × table order × column order. That
  is safe here only because the three test dbs give identical row blocks.
* AVG_ROW_LENGTH/INDEX_LENGTH come from the FE (`OlapTable.getTableStatusStats`, `fe/.../catalog/OlapTable.java:1894-1916`):
  `avg = rowCountForAvg>0 ? dataSize/rowCount : 0`, and `index_length` = the sum of the replicas' reported inverted-index
  sizes. So doris-rs tablet reports must report 0 inverted-index size for these tables. Empty tables
  already give 0 for both.
* `session_variables` reads `wait_timeout 30000` from the FE session: no BE state.
* No dependency on another suite's state: the suite drops and creates its own dbs.
  (`CREATE TABLE IF NOT EXISTS` at lines 120-162 is a no-op the second time.)

---

## A. Aggregation

### A.1 TAggregationNode shapes on one BE

Fields (`gensrc/thrift/PlanNodes.thrift`; gen `plan_nodes.rs:12619`): `grouping_exprs`,
`aggregate_functions: list<TExpr>` (root `AGG_EXPR` + its argument trees), `intermediate_tuple_id`,
`output_tuple_id` (equal in every corpus node), `need_finalize`, `use_streaming_preaggregation`,
`agg_sort_infos` (always present, empty), `is_first_phase`, `is_colocate`. The AGG_EXPR node carries
`agg_expr{is_merge_agg, param_types}`, `fn_{name, binary_type BUILTIN, arg_types, ret_type, has_var_args,
signature e.g. "count()", aggregate_fn{intermediate_type, symbols ""}, vectorized true}`, and `is_nullable`.

AGGREGATION_NODEs in gate suites (10374 nodes, 373 suites):

| need_finalize | streaming | first_phase | fns | nodes | meaning |
|---|---|---|---|---|---|
| true | false | false | merge | 4025 | final phase after an exchange |
| false | false | true | update | 3384 | phase 1 (scalar or non-streaming) |
| true | false | false | update | 1419 | **one-phase** (`colocate=true`: grouped on the distribution key, or after a shuffle) |
| false | **true** | true | update | 559 | streaming pre-agg (grouped phase 1) |
| true | false | false | none | 519 | DISTINCT / group-by-only, final |
| false | true | true | none | 347 | DISTINCT / group-by-only, streaming phase 1 |
| false | false | false | update/merge | 82/39 | middle phase of a 3/4-phase distinct (20 suites) |

Phase semantics in C++: the sink is a merge sink if any evaluator `is_merge`
(`aggregation_sink_operator.cpp:865-873`). The source picks one of four result paths from
(`grouping_exprs.empty()`, `need_finalize`) (`aggregation_source_operator.cpp:58-75`). With
`need_finalize=false`, the output is the serialized state (`serialize_without_key_to_column`, `:368-374`).
For a merge expr, the function is built from **`agg_expr.param_types`** (the original argument types), not
from its child slot (`exprs/vectorized_agg_fn.cpp:77-82, 112-116, 151-152`). The child slot is a
placeholder: the FE types every partial-buffer slot as `VARCHAR(65533)`
(`fe/.../nereids/trees/expressions/AggregateExpression.java:72-80`). `fn.aggregate_fn.intermediate_type` is
`VARCHAR(65533)` for sum/avg/multi_distinct_*, and equals the input type for min/max and BIGINT for count.
Neither drives the BE's state format.

**Decoded examples**

`SELECT count(*) FROM t WHERE …` (`compaction.test_base_compaction` rec 1204; 591 requests have this shape):
```
frag0 x4 instances: OLAP_SCAN#0[conj eq(slot16:TINYINT,0)][proj 1:TINYINT]
   -> AGG#1{need_finalize=false streaming=false first_phase=true colocate=false group=[]
            count[update]() args=[] ret=BIGINT inter=BIGINT is_nullable=false param_types=[]}
   -> DATA_STREAM_SINK UNPARTITIONED -> 127.0.0.1:8060
frag1 x1: EXCHANGE#2 -> AGG#3{need_finalize=true first_phase=false colocate=true
            count[merge](slot19:VARCHAR(65533)) label "partial_count(*)"} -> RESULT_SINK
```
`SELECT k, sum(v) FROM t GROUP BY k`. Two shapes:
* k is the distribution key, so **one phase** in the scan fragment (`aggregate_decimal256` rec 34):
  `OLAP_SCAN -> AGG{need_finalize=true first_phase=false colocate=true group=[slot3:INT] sum[update](CAST(slot4 AS DECIMAL256(39,6))) ret DECIMAL256(76,6) nullable=true} -> SORT -> DATA_STREAM_SINK UNPARTITIONED; frag1 EXCHANGE -> RESULT_SINK`.
* k is not the distribution key, so **two phases with a hash shuffle** (`test_insert_strict_mode_and_filter_ratio_custom` rec 1356):
  `frag0 x4: OLAP_SCAN -> AGG{need_finalize=false streaming=true first_phase=true group=[slot0:INT] sum[update](CAST(if(…) AS DOUBLE)) ret DOUBLE} -> DATA_STREAM_SINK HASH_PARTITIONED -> 4 dests (all :8060)`;
  `frag1 x4: EXCHANGE -> AGG{need_finalize=true colocate=true group=[slot2:INT] sum[merge](slot3:VARCHAR(65533))}`.

`SELECT count(DISTINCT x) FROM t` (`datatype_p0.date.test_agg_functions` rec 48): the FE rewrites it to
`multi_distinct_count`:
```
frag0 x4: OLAP_SCAN -> AGG{need_finalize=false streaming=false first_phase=true group=[]
          multi_distinct_count[update](slot0:DATEV2) ret BIGINT inter VARCHAR(65533) nullable=false}
          -> DATA_STREAM_SINK UNPARTITIONED
frag1 x1: EXCHANGE -> AGG{need_finalize=true colocate=true multi_distinct_count[merge](slot1:VARCHAR(65533))} -> RESULT_SINK
```
The alternative 4-phase distinct (`test_timestamptz_count_distinct` rec 20, 31 requests):
`AGG{group=[x] no fns, streaming} -HASH-> AGG{group=[x] final} -> AGG{count[update](x), need_finalize=false,
first_phase=false} -UNPARTITIONED-> AGG{count[merge], final}`. Grouped count-distinct mixes both
(`test_distinct_agg` rec 56: multi_distinct_count 2-phase over HASH, then sum 2-phase over HASH).

### A.2 Result types and semantics

The FE fixes `ret_type` and `is_nullable`. The BE must honour them: `verify_result_type` checks them
(`vectorized_agg_fn.cpp:266-271`), and decimal sum/avg take precision and scale from the FE plan.

| fn | input → ret (corpus) | BE accumulator / state | semantics |
|---|---|---|---|
| count() | → BIGINT, nullable=false (all 11291) | i64 (`AggregateFunctionCount`) | counts rows |
| count(x) | any (incl. ARRAY/STRUCT) → BIGINT | i64; nullable x → `AggregateFunctionCountNotNullUnary` (`aggregate_function_count.cpp:31-51`) | counts non-NULL; empty → 0 |
| sum | TINYINT/SMALLINT/INT/BIGINT → BIGINT; LARGEINT → LARGEINT; FLOAT/DOUBLE → DOUBLE; DECIMALV3(p,s) → DECIMAL128I(38,s) (DECIMAL256(76,s) if the arg is DECIMAL256 or `enable_decimal256`) | `result_type()` (`aggregate_function_sum.h:278-288`); decimals use the FE result type (`aggregate_function_sum.cpp:34-39`); state = one `sum` value (`.h:52-70`) | NULLs skipped; no rows / all NULL → NULL; wraps on overflow (plain `+=`) |
| sum0 | BIGINT → BIGINT, nullable=false (73 nodes, from eager-agg rewrites) | **alias of sum** (`aggregate_function_sum.cpp:49`) | like sum, but the result is non-null, so empty → 0 |
| min/max | T → T (state type = T) | — | NULLs skipped; empty → NULL |
| avg | integers → DOUBLE; DOUBLE → DOUBLE; DECIMALV3(p,s) → DECIMAL128I(38, max(s,4)) (or 76 with decimal256) (`fe/.../agg/Avg.java:88-110`; seen: DECIMAL64(15,2)→(38,4), DECIMAL32(9,3)→(38,4)) | `{sum, count:u64}`; sum type `avg_sum_type`: BIGINT/LARGEINT → LARGEINT, other ints → BIGINT, float → DOUBLE, decimal → DECIMAL128I (`aggregate_function_avg.h:56-126`) | int/double: `sum as f64 / count as f64`. decimal: `(sum * 10^(out_scale - in_scale)) / count` as **integer division of the scaled value, truncating toward zero** (`avg.h:96`, `core/types.h:434-436`); empty → NULL |
| multi_distinct_count | T → BIGINT, nullable=false | exact hash set (`AggregateFunctionUniqExactData`, `aggregate_function_uniq.h:61`; registered `aggregate_function_uniq.cpp:67`) | NULLs skipped; empty → 0 |

Nullability rules (C++):
* `AggFnEvaluator` builds the result type from `fn.ret_type` and `is_nullable`, defaulting to nullable
  when that is unset (`vectorized_agg_fn.cpp:75-76`).
* If any argument type is nullable, the factory uses the nullable registry
  (`aggregate_function_simple_factory.h:123-160`): a NULL-skipping wrapper.
* A scalar agg over non-nullable inputs whose output slot is nullable gives NULL when
  `input_num_rows == 0` (`aggregation_source_operator.cpp:416-440`). That is where "sum of an empty set
  is NULL" comes from; count's slot is non-nullable, so it gives 0.
* Corpus: sum is nullable=true in 1101 nodes and false in 382 (grouped, non-null input); min 1897/25,
  max 2107/42, avg 191/126.

Commonest signatures (node counts): `count()` 6257, `count(INT)` 1601, `max(INT)` 1375, `min(INT)` 1352,
`count(TINYINT)` 1053, `count(SMALLINT)` 1010, `sum(INT)→BIGINT` 417, `count(BIGINT)` 358, `sum(BIGINT)` 264,
`sum(TINYINT)` 244, `avg(DECIMAL64(15,2))→DECIMAL128I(38,4)` 122, `sum(DECIMAL128I(38,10))` 96, `sum(DOUBLE)` 86,
`sum(DECIMAL64(15,2))→(38,2)` 80, `sum0(BIGINT)` 71. Full list: `<scratchpad>/m5/agg/fnsig.txt`.

### A.3 Intermediate state: nothing crosses a wire

* Every exchange destination is the one BE (`brpc_server` 127.0.0.1:8060). The C++ sender treats a channel as local
  when the destination host is localhost and `enable_local_exchange` is on (`exec/exchange/vdata_stream_sender.h:118`,
  `.cpp:58-61`). The plans set `enable_local_exchange: true`. A local channel hands the Block over in memory.
* The needs index over the whole corpus lists **no `transmit_block`/`transmit_block_by_http`** RPC (only
  `exec_plan_fragment*`, `fetch_data`, `cancel_plan_fragment`, …). The partial states never appear in the
  capture.
* The C++ state formats, for reference only: sum/count/avg state = `ColumnFixedLengthObject` of the raw
  `Data` struct (`aggregate_function_sum.h:137-210`, `aggregate_function_count.h:157-163`), with type
  `DataTypeFixedLengthObject`. So **doris-rs may use any in-process representation** for the
  update→merge hand-off and for the HASH_PARTITIONED shuffle between its own instances. Two requirements
  remain:
  1. The merge side must build its state from `param_types`, not from the VARCHAR placeholder slot.
  2. Rows with the same group key must reach the same merge instance. Any hash function works, because
     all instances are local.

### A.4 Scope bounds (gate suites)

* 373 gate suites contain an AGGREGATION_NODE. In **286** of them every aggregate function is in the M5 set
  {count, sum, min, max, avg, multi_distinct_count, sum0}. The rest also need, by suite count: ndv 33,
  group_concat 12, any_value 6, multi_distinct_sum 5, window_funnel_v2 4, collect_set 4, stddev 3,
  avg_weighted 3, percentile_array 3, collect_list 3, others ≤2.
  * `ndv` appears mostly in statistics-collection SQL (`count, ndv, count, min, max` with a
    `'<id>--1-value'` projection: ANALYZE/auto-analyze), e.g. 1120 requests in `mtmv_with_sql_cache`.
    Some suites may need ndv only because of that background statistics SQL. Worth checking before
    adding ndv to scope.
* Grouping: scalar agg nodes in 246 suites and grouped in 206. Key counts per grouped node: 1 key 1597,
  2 keys 288, 3 keys 127, ≥4 keys 407. Only 8 nodes group by a non-slot expression: the FE projects key
  expressions into slots first.
* Streaming pre-agg in 173 suites. It can be treated as a plain phase-1 aggregation, since it only
  changes when partials are flushed. The 3/4-phase distinct middle phase appears in 20 suites.
* Aggregate arguments that are expressions, not slots: 2413 of 8888 update args, in 119 suites. They are mostly
  `CAST_EXPR` (24 suites), `if` (22), `length` (19), `multiply`, literals (`count(1)`), `hll_hash`. So the
  agg evaluator needs the general expression engine for its inputs.
* Instances: phase-1 fragments run with `local_params` = 4 or 10 instances, each with its own scan
  ranges, so fragment/instance plumbing is the other researcher's topic. For aggregation this means
  merge-by-exchange between N local instances is the common case, not the exception.
