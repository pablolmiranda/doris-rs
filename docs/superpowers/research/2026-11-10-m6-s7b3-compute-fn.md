# Batch 3 research: `expr:COMPUTE_FUNCTION_CALL` (timestamp arithmetic)

Suites: `correctness_p0.test_outer_join_with_null_value`, `nereids_p0.join.asof.test_asof_join_build_expr_edge`,
`nereids_p0.join.asof.test_asof_join_expr`.

Dump tool: `scratchpad/b3/cfdump` (binary `scratchpad/b3/cfdump/target/release/cfdump <traffic.bin.gz>`;
`ALL=1` also prints every match condition and other-join conjunct). It walks every expr field of every plan
node (conjuncts, projections, intermediate projections, join eq/other/mark/match_condition, sort, agg,
analytic, union, partition sort, runtime filters, sink partition exprs, output exprs). Completeness check: the
count of COMPUTE_FUNCTION_CALL nodes it walked equals the count of `node_type: TExprNodeType(22)` in the
`Debug` text of every fragment (1, 15, 6). Dumps: `scratchpad/b3/cf-<suite>.txt`.

## What it is

- FE: only `TimestampArithmeticExpr.toThrift` sets it (`fe/.../analysis/TimestampArithmeticExpr.java:145-148`),
  with an opcode `TIMESTAMP_<UNIT>S_{ADD,SUB}` (:150-). Nereids makes one from `TimestampArithmetic`, i.e.
  `x + INTERVAL n UNIT` / `x - INTERVAL n UNIT` (`ExpressionTranslator.java:874-885`). `fn` is a normal
  builtin: name `{years,months,weeks,days,hours,minutes,seconds,...}_{add,sub}`, args `(date, INT|BIGINT)`,
  ret the date type, `NullableMode.DEPEND_ON_ARGUMENT`. The function-call form (`DATE_ADD(x, INTERVAL 1 HOUR)`,
  `HOURS_ADD(x, 2)`) arrives as a plain FUNCTION_CALL with the same `fn` (seen in `test_asof_join_expr`).
- BE: `vexpr.cpp:543-548` builds `ARITHMETIC_EXPR`, `BINARY_PRED`, `NULL_AWARE_BINARY_PRED` and
  `COMPUTE_FUNCTION_CALL` all as `VectorizedFnCall`; FUNCTION_CALL is the same class except for the
  `if`/`ifnull`/`coalesce`/... special cases (:550-). `VectorizedFnCall` looks the function up by
  `fn.name` and never reads `_node_type` or `_opcode` (`vectorized_fn_call.cpp`; no reference). So a
  COMPUTE_FUNCTION_CALL `hours_add` is exactly a FUNCTION_CALL `hours_add`. The only BE place that names
  FUNCTION_CALL by type is scan-predicate pushdown (`scan_operator.cpp:425`); no CFC is in a scan conjunct
  here.

Opcodes seen: 28 TIMESTAMP_HOURS_ADD, 29 TIMESTAMP_HOURS_SUB, 34 TIMESTAMP_MINUTES_ADD, 42 TIMESTAMP_SECONDS_ADD.

## Every expression tree with a COMPUTE_FUNCTION_CALL (CFC), deduplicated

DT = DATETIMEV2(18,0); `?` = is_nullable; `#n` slot id. All CFC nodes are nullable.

**test_outer_join_with_null_value** (1 tree, query `qt_select_xx`, `t5.date_cd + INTERVAL 0 SECOND`):
- OLAP_SCAN node 2 `.projections`:
  `CFC<op=42>[seconds_add(DT,BIGINT) -> DT]:DT?(CAST_EXPR:DT?(SLOT_REF:DATEV2?#6), INT_LITERAL:BIGINT=0)`.
  The projected slot is then a `<=>` (EQ_FOR_NULL) hash-join key against `t1.DATE_CD` (DATETIMEV2).
  Joins in the suite: INNER ×1, LEFT_OUTER ×6, all BROADCAST.

**test_asof_join_build_expr_edge** (12 plans; every one `l.ts >= r.ts + INTERVAL 1 HOUR`):
- HASH_JOIN `.hash_join.match_condition`, ASOF_LEFT_OUTER (14) ×10 plans, ASOF_LEFT_INNER (12) ×1:
  `BINARY_PRED[ge(DT,DT)]:BOOLEAN?(SLOT_REF:DT?#probe, CFC<op=28>[hours_add(DT,INT) -> DT]:DT?(SLOT_REF:DT?#build, INT_LITERAL:INT=1))`
- cross precision (`qt_abee_cross_precision`):
  `ge(DT6,DT6)(CAST_EXPR:DATETIMEV2(18,6)?(SLOT_REF:DATETIMEV2(18,3)?#10), CFC<op=28>[hours_add(DATETIMEV2(18,6),INT) -> DATETIMEV2(18,6)]?(SLOT_REF:DATETIMEV2(18,6)?#5, INT=1))`
- LEFT_OUTER (1) `.hash_join.other_join_conjuncts` ×2 plans (the two ROW_NUMBER cross-checks, `sql` not
  `qt`, asserted in Groovy):
  `ge(DT,DT)(SLOT_REF#probe, CFC<op=28>[hours_add(DT,INT)](SLOT_REF#build, INT=1))`, plan
  `SORT ← ANALYTIC ← SORT ← PARTITION_SORT ← HASH_JOIN[LEFT_OUTER] ← OLAP_SCAN, EXCHANGE`.
- Chained ASOF (`qt_abee_chained`): two ASOF_LEFT_OUTER joins in one fragment; only the upper has the CFC.
- CTE plans (`abee_cte_expr`, `abee_self_join`, `abee_inner_cte`): both join children are EXCHANGEs fed
  by a MULTI_CAST_DATA_STREAM_SINK.

**test_asof_join_expr** (4 CFC trees; every ASOF join is ASOF_LEFT_OUTER except one ASOF_LEFT_INNER):
- `ge(SLOT#p, CFC<op=28>[hours_add(DT,INT)](SLOT#b, INT=1))` — `r.ts + INTERVAL 1 HOUR`
  (`expr_build_interval`, `expr_edge_exact`)
- `ge(CFC<op=29>[hours_sub(DT,INT)](SLOT#p, INT=1), SLOT#b)` — expression on the **probe** side
  (`expr_probe_interval`)
- `ge(SLOT#p, CFC<op=34>[minutes_add(DT,BIGINT)](CFC<op=34>[minutes_add(DT,BIGINT)](SLOT#b, BIGINT=30), BIGINT=30))`
  — nested CFC (`expr_multi_interval`)
- `gt(SLOT#p, CFC<op=28>[hours_add(DT,INT)](SLOT#b, INT=1))` (`expr_edge_strict`)
- The other match conditions in this suite are FUNCTION_CALL (already accepted): `hours_add` (=1,2,3,24),
  `days_add(…,0)`, `minutes_add(…,90)`, `minutes_add(hours_add(…,1),-30)`,
  `minutes_sub(hours_add(…,1),30)` (FE rewrote `DATE_ADD(l.ts,30m) >= DATE_ADD(r.ts,1h)` to a build-side
  expression), `seconds_add(minutes_add(hours_add(…)))`, `le/lt(… hours_sub(…,3))`.

No CFC appears in a projection, conjunct or key of the asof suites other than above.

## ASOF machinery: already in doris-rs

- TJoinOp: ASOF_LEFT_INNER_JOIN=12, ASOF_RIGHT_INNER_JOIN=13, ASOF_LEFT_OUTER_JOIN=14,
  ASOF_RIGHT_OUTER_JOIN=15 (`PlanNodes.thrift:1182-1185`); `THashJoinNode.match_condition` field 15
  (:1230). Only 12 and 14 occur in these suites.
- doris-rs runs 12 and 14 (`hash_join.rs:31-50`, `asof_join` :413-468; `pipeline.rs:362-388` evaluates
  `match_condition` child 0 on the probe chunk and child 1 on the build chunk, appended as the last key;
  `query.rs:539-544` checks it). This matches C++: probe takes `root->get_child(0)`
  (`hashjoin_probe_operator.cpp:527-539`), build `get_child(1)` prepared against the build child's row
  desc (`hashjoin_build_sink.cpp:776-791, 814-820`). Sides are arbitrary exprs, so a CFC on either side
  needs only expression support.
- `check_asof_condition` requires both side roots of one type and scale, DATEV2 or DATETIMEV2
  (`hash_join.rs:270-284`). Every tree above passes (DT/DT; DT6/DT6 in cross precision).
- `test_asof_join_explicit` and `test_asof_join_large_bucket` already pass both gates
  (`oracle/scores/epic1-out.txt:267-268`, `epic1-strict.txt:210-211`).
- needs.json is complete for these suites: against `oracle/scores/epic1-supported.json` each lacks
  exactly `expr:COMPUTE_FUNCTION_CALL`. (One more corpus suite uses CFC, `nereids_p0.join.test_many_multi_join`,
  but it also lacks `function:random` and `read:UNIQUE_KEYS`.)

## How doris-rs evaluates FUNCTION_CALL today

- `eval.rs:192-246` `check_nodes`: `N::FUNCTION_CALL` matches on the function name; date add/sub go to
  `date_call` → `datetime::supported` (`datetime.rs:501-522`: `(DATEV2|DATETIMEV2, INT|BIGINT) -> same type
  and scale`, time units only on DATETIMEV2). Any other node type, CFC included, falls to `_ => false` →
  `Unsupported { "expression COMPUTE_FUNCTION_CALL" }`.
- `eval.rs:656-662` dispatch: `N::FUNCTION_CALL if is_date_function(f)` → `datetime::eval`
  (`datetime.rs:540-590`; `Unit::parse` covers `{years..seconds}_{add,sub}`, NULL in → NULL out,
  out of range → coded -218; `add` keeps `micro`, so scale 6 is fine).
- `eval.rs:290` `check_constants` (`call` closure: FUNCTION_CALL | ARITHMETIC_EXPR | CAST_EXPR) and
  `eval.rs:147-174` `describe`/`node_name` also list FUNCTION_CALL by type.
- Every signature in the trees above is already accepted by `datetime::supported`: hours_add/hours_sub
  `(DT,INT)`, minutes_add `(DT,BIGINT)`, seconds_add `(DT,BIGINT)`, hours_add `(DATETIMEV2(6),INT)->(6)`.
  CAST DATEV2→DATETIMEV2 and DATETIMEV2(3)→(6) are existing casts (`eval.rs:515-545`, `round_datetime`).

## Expected results (`regression-test/data/...`)

- `correctness_p0/test_outer_join_with_null_value.out`: `select1` 2, `select2` 2, `select_xx` 1 (counts).
- `nereids_p0/join/asof/test_asof_join_build_expr_edge.out`: 11 `qt_` blocks, 2–10 rows each, all
  `ORDER BY l.id` on a unique id. Outer rows NULL-extended (`\N`): `abee_self_join` ids 1,5,9,10;
  `abee_agg_build` ids 3,5. `abee_cross_precision`: `1 2 110`, `2 3 120` (10:00:00.123 ≥ 09:00:00.123+1h
  exact equality picks rid 2).
- `nereids_p0/join/asof/test_asof_join_expr.out`: 22 `qt_` blocks, all `ORDER BY l.id` (unique).
  `expr_edge_exact` relies on `>=` equality (09:00+1h = 10:00 → `exact_match`); `expr_edge_strict` on `>`
  (→ `before`). `expr_null_probe`: probe NULL ts → `2 \N \N \N \N`; build NULL ts never matches.
  `expr_no_match` all NULL-extended.
- Ties: no build group has two rows with the same match value (checked against the inserted data,
  including `abee_agg_build`'s `MAX(ts)` per (grp, category) and the self join), so doris-rs's
  "an ASOF match among equal values" refusal (`hash_join.rs:452-460`) never fires.
- The ROW_NUMBER cross-checks partition by `l.id` and order by `r.ts DESC`; `r.ts` is unique per group,
  so `rn = 1` is deterministic.

## What doris-rs needs

1. Accept `N::COMPUTE_FUNCTION_CALL` where FUNCTION_CALL is accepted for the date add/sub family, and only
   there (the FE emits CFC only from `TimestampArithmeticExpr`): in `check_nodes` an arm
   `N::COMPUTE_FUNCTION_CALL => function_name(n).is_some_and(|f| Unit-family) && date_call(n)`; in the
   dispatch `N::FUNCTION_CALL | N::COMPUTE_FUNCTION_CALL if is_date_function(..)`; add CFC to
   `check_constants`'s `call` closure (C++ folds an all-constant VectorizedFnCall at open the same way);
   add it to `describe`/`node_name` so a refusal reads "function hours_add". Ignore the opcode, as C++ does.
2. Add `expr:COMPUTE_FUNCTION_CALL` to `oracle/scores/epic1-supported.json` and the extractor's list if the
   batch keeps one; extract the asof replays.
3. Tests: a CFC `hours_add` / `hours_sub` / nested `minutes_add` evaluates as its FUNCTION_CALL twin; an
   ASOF join with a CFC on the build side and on the probe side; a CFC other than the date family is
   refused; a CFC projection on a scan feeding a `<=>` key.

Size: ~15 lines of product code plus tests; no new join machinery.

Risks:
- Low. The only behavior is the existing date add/sub code; result types/scales come from `fn`.
- Not checked by running: the CTE (multi-cast) shape with two ASOF children both EXCHANGE, and two ASOF
  joins stacked in one fragment, are already-supported features but first appear in passing suites here.
