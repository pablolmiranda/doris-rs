# Doris 4.1.5 BE: Nested-loop join, ASSERT_NUM_ROWS, CASE, nullable(), assert_true()

Paths are relative to `be/src/` unless noted. NLJP = `exec/operator/nested_loop_join_probe_operator`.

## 1. NLJ column layout and semantics

**Join block layout.** `_join_block` is built from `vintermediate_tuple_id_list`: every slot of every intermediate tuple, in tuple then slot order (`exec/operator/join_probe_operator.cpp:46-52`). That gives the probe (left) slots, then the build (right) slots. For a mark join the **last** column is the mark column, a `Nullable(UInt8)` (`join_probe_operator.cpp:54-62`).
- `_num_probe_side_columns` = materialized slots of the left child; `_num_build_side_columns` = materialized slots of the right child (NLJP.cpp:407-408).
- Nullability comes from the intermediate slot types. FE marks the left intermediate slots nullable for RIGHT/FULL OUTER and the right slots nullable for LEFT/FULL OUTER (fe `PhysicalPlanTranslator.java:2173-2180`).
- When a non-nullable source column is copied into a nullable destination, BE wraps it with an all-zero null map (NLJP.cpp:342-352, 360-371).
- Padding rows use `insert_many_defaults`, which gives NULL in nullable columns (NLJP.cpp:226, 276, 295).
- Pipeline after the join: the node's `_conjuncts` filter the intermediate block in `pull` (NLJP.cpp:458), then `_build_output_block` swaps it out (`join_probe_operator.cpp:65-72`). The generic operator applies intermediate projections and projections, then `reached_limit` runs (NLJP.cpp:479).

**Ignored Thrift fields** (no references anywhere in `exec/operator`):
- `is_output_left_side_only` (deprecated; `gensrc/thrift/PlanNodes.thrift:1242-1244`).
- `mark_join_conjuncts` for NLJ. Only `join_conjuncts`, or the legacy `vjoin_conjunct`, is read (NLJP.cpp:388-397). FE sends `mark_join_conjuncts` only when other conjuncts are non-empty (fe `PhysicalPlanTranslator.java:2185-2194`), and BE ignores them anyway.
- ⇒ doris-rs should evaluate **only `join_conjuncts`**. If a mark NLJ has empty `join_conjuncts`, every probe row counts as matched whenever the build side is non-empty.

**Conjunct evaluation is two-valued.** All `join_conjuncts` are ANDed by `VExprContext::execute_conjuncts(..., accept_null=IgnoreNull, ...)` (NLJP.h:130-131; `exprs/vexpr_context.cpp:293-307`). Each conjunct maps to a filter bit (`exprs/vexpr.cpp:1110-1160`):
- `accept_null=false`: NULL counts as false.
- `accept_null=true`: NULL counts as **true**, applied per conjunct.
- `IgnoreNull` is true only for `NULL_AWARE_LEFT_ANTI_JOIN` (NLJP.cpp:101).

With no conjuncts, every pair matches (NLJP.h:149-172).

**Flags per join op** (`join_probe_operator.cpp:85-104`):

| Flag | Join ops |
|---|---|
| `_match_all_probe` | LEFT_OUTER, FULL |
| `_match_all_build` | RIGHT_OUTER, FULL |
| `_is_left_semi_anti` | LEFT_SEMI, LEFT_ANTI, NULL_AWARE_LEFT_ANTI |

- `set_probe_side_flag` = `match_all_probe || left_semi_anti`.
- `set_build_side_flag` = `match_all_build || right_semi_anti` (NLJP.cpp:438-440).
- Build visited flags exist only for those ops (`nested_loop_join_build_operator.cpp:107-110`).

**Semantics per join op**

| op | rows emitted |
|---|---|
| CROSS / INNER | Every (p,b) pair where the conjuncts are TRUE. Pairs are materialised and then filtered (NLJP.cpp:170-172, materialize=true). |
| LEFT_OUTER | Matched pairs, plus each probe row with no TRUE match, padded with build columns set to NULL (NLJP.cpp:249-279, `IsSemi=false`, picks `visited==0`). |
| RIGHT_OUTER | Matched pairs, then build rows never matched, padded with NULL probe columns (NLJP.cpp:194-248). |
| FULL_OUTER | Both of the above. |
| LEFT_SEMI (non-mark) | Pairs are filtered but **not materialised** (`materialize = !_is_left_semi_anti`, NLJP.cpp:152, block cleared at NLJP.h:104-106). Each probe row with `visited==1` is emitted **once**, with default build columns (NLJP.cpp:159, `IsSemi = (op==LEFT_SEMI)`, NLJP.cpp:255). |
| LEFT_ANTI | Same path; probe rows with `visited==0`. |
| NULL_AWARE_LEFT_ANTI | Same as LEFT_ANTI, but a conjunct result of NULL counts as a match. A probe row is emitted iff **no** build row gives TRUE or NULL. For `a NOT IN (subq)` with conjunct `a=b`: any NULL b, or a NULL a against a non-empty build, removes the row. An empty build keeps every row. There is no other special casing (`_short_circuit_for_null_in_build_side` is hash-join only). |
| mark (SEMI / ANTI / NAAJ, `is_mark=true`) | **Every** probe row is emitted once: probe columns + build defaults + mark. The mark is `IsSemi == visited`, so SEMI gives `matched` and ANTI/NAAJ give `!matched` (NLJP.cpp:280-297). The mark is inserted with a 0 null map (`core/column/column_filter_helper.cpp:31-34`), so it is **never NULL**. NLJ mark joins therefore have no three-valued logic: a NULL comparison counts as no match for SEMI/ANTI and as a match for NAAJ. |

## 2. Exact output order

Main loop (NLJP.cpp:110-144), state `(_left_block_pos, _current_build_pos)`:

```
while join_block.rows() < batch_size:            # row count BEFORE join-conjunct filtering
  while cur_build_blk == n_build_blocks or probe_pos == probe_block.rows():
      if probe_pos < rows: processed_count++     # probe row finished all build blocks
      cur_build_blk = 0; probe_pos++             # _reset_with_next_probe_row (91-95)
      if probe_pos >= rows: matched_rows_done (left eos) or need_more_input; break
  append probe row[probe_pos] repeated |B_blk| times  ++  build block B_blk rows 0..|B_blk|-1   (333-377)
  cur_build_blk++
```

- **The order is probe-major.** For each probe row P_i (probe order within the probe block), take each build block in arrival order and each row of that block in order. This produces P1×B1..Bn, then P2×B1..Bn, and so on. The build side is not the outer loop.
- `batch_size` is checked only before appending a whole build block, so a call can overshoot. A probe row can span two calls, and the pair order is unchanged. A "call" is one `generate_join_block_data`; it is triggered by `push` (once per probe child block) and by `pull` (NLJP.cpp:419-477).
- The join conjuncts then filter the block, which keeps the relative order (NLJP.h:104).

**INNER / CROSS:** filtered pairs in probe-major order. Batch boundaries do not change the order.

**LEFT OUTER / FULL** (probe-unmatched part): matched pairs come first. **At the end of each call**, the probe rows that finished in that call (`[_left_block_start_pos, +_left_side_process_count)`) and stayed unvisited are appended in probe order with NULL build columns (NLJP.cpp:153-161, 249-279).
- Unmatched rows are therefore interleaved per call, not per row.
- Common case (one probe block and |probe|·|build| < batch_size, default 4064): **all matched pairs (probe-major), then all unmatched probe rows (probe order)**.
- With several probe blocks or a larger cross product, the output is: per call, the matches of that call followed by the unmatched rows completed in that call.
- A row whose last build block filled the batch is counted at the start of the *next* call (the inner while), so its NULL row comes after that next call's matches.

**SEMI / ANTI / NAAJ / mark:** the pair block is discarded, so the output is just probe rows in probe order (per call; probe order overall).

**RIGHT OUTER / FULL** (build-unmatched part): after `_matched_rows_done` (probe eos, every probe row processed), build rows with `visited==0` are appended:
- in build-block order, then row order inside each block (NLJP.cpp:176-181, 194-248);
- in the same block as the last probe phase, after FULL's final probe-unmatched rows;
- continuing across pulls while `column_size < batch_size`, checked per build block.
- `eos` is set when every build block has been emitted (NLJP.cpp:447-451).

**Parallelism caveat:**
- Build blocks are stored in sink arrival order (`nested_loop_join_build_operator.cpp:106`).
- Local exchange is ADAPTIVE_PASSTHROUGH for inner, left and semi joins, and NOOP for RIGHT/FULL/NAAJ (NLJP.h:206-213). With more than one instance, probe rows are split across instances and the global order is nondeterministic.

## 3. Empty sides

- The sink stores only non-empty blocks (`build_operator.cpp:104-111`). With an empty build, `build_blocks.size()==0`, so the inner while immediately marks every probe row as processed and emits no pairs (NLJP.cpp:111-132). Results:
  - INNER/CROSS/SEMI: 0 rows.
  - LEFT_OUTER/FULL: every probe row, with NULL build columns.
  - ANTI and NAAJ: every probe row, including NULL keys.
  - Mark SEMI: every row with mark=false.
  - Mark ANTI/NAAJ: mark=true.
  - Mark join without the probe flag (unused here): `_append_left_data_with_null` with mark=0 (NLJP.cpp:162-164, 302-331).
- Empty probe: no pairs. RIGHT/FULL emit every build row with NULL probe columns (all build flags are 0). Every other op emits 0 rows.
- When `can_filter_all` is true (no pair in the call matched), the block is cleared and the offset stacks reset; visited flags are unchanged (NLJP.h:134-141).

## 4. ASSERT_NUM_ROWS_NODE (`exec/operator/assert_num_rows_operator.cpp`)

Streaming. `pull` runs per child block. `n` = cumulative rows returned **including this block** (`:44-45`) and `has_more = !eos` (`:47`). An unset assertion defaults to LE (`:30-34`). Each check is evaluated per block:

| assertion | passes when |
|---|---|
| EQ (0) | `n == d \|\| (has_more && n < d)` |
| NE (1) | `n != d \|\| has_more` |
| LT (2) | `n < d` |
| LE (3) | `n <= d` |
| GT (4) | `n > d \|\| has_more` |
| GE (5) | `n >= d \|\| has_more` |

Cited at `:48-68`.

`should_convert_output_to_nullable` (`:76-97`):
- Non-empty block: every column's type and column become nullable.
- Empty block with `eos && EQ && n==0 && d==1`: the block is replaced by **one row of all NULLs**, using the node's row descriptor with every type made nullable, and the assert **passes** (`assert_res=true`).
- Otherwise (e.g. EQ 1 with 0 rows and the flag false) the assert fails.

On failure (`:99-117`) BE returns `Status::Cancelled` (code CANCELLED=1):

```
AssertOperator(node id: {node_id}) Expected {EQ|NE|LT|LE|GT|GE} {desired} to be returned by expression {subquery_string}(actual rows: {n}) 
```

- There is no space before `(actual rows:`, and there is a **trailing space**.
- `{n}` is the cumulative count at the failing block. With multi-row input this is usually the rows seen so far: the first block where `n>1` for EQ 1.
- Over the wire, `to_protobuf` and `to_thrift` prefix `({be_host})[CANCELLED]` (`common/status.cpp:44-45, 59-60`).
- FE passes `error_msgs[0]` through unchanged (fe `common/Status.java:62-73`, `qe/runtime/QueryProcessor.java:159-170` throws `UserException(errMsg)`).
- Client sees: `errCode = 2, detailMessage = (127.0.0.1)[CANCELLED]AssertOperator(node id: 7) Expected EQ 1 to be returned by expression LogicalProject[31] ( distinct=false, projects=[last_updated_stamp#4] )(actual rows: 2) `. The host is the BE IP; whether the trailing space survives FE and MySQL is unverified.
- Regression tests only check the substring `Expected EQ 1 to be returned by expression` (`regression-test/suites/nereids_p0/subquery/test_subquery.groovy:69`).
- After the check, the node's `_conjuncts` filter is applied (`:118`).

## 5. CASE_EXPR (`exprs/vcase_expr.{h,cpp}`)

- **`has_case_expr` is ignored by BE.** It is never read; clones set it to false (`vcase_expr.h:71`). Children are always `[when1, then1, when2, then2, ..., else?]` (`vcase_expr.cpp:98-110`). Nereids always emits the searched form (`new CaseExpr(whenClauses, elseExpr)`, fe `ExpressionTranslator.java:475`): a simple `CASE x WHEN v` is rewritten by FE to `x = v`, which is NULL when either side is NULL and therefore not a match. If doris-rs ever receives `has_case_expr=true`, Doris would misinterpret it as well.
- With `short_circuit_evaluation=true` the node becomes `ShortCircuitCaseExpr` instead (`exprs/vexpr.cpp:600-604`).
- Branch selection (`vcase_expr.h:270-320`):
  - `then_idx[row]` = first `i+1` whose WHEN is TRUE (non-null and nonzero); 0 means ELSE or no match.
  - A NULL WHEN never matches (`:308`).
  - A const WHEN that is true fills every unset row and stops the scan (`:278-285`); a const NULL is `get_bool` false.
- Evaluation is not lazy per row. ELSE is always evaluated (`cpp:89-95`). A THEN is evaluated for **all rows** unless its WHEN is false or NULL on every row of the block (`calculate_false_number == rows`, which counts zero or null; `cpp:101-104`, `exec/common/util.hpp:274-291`). So a THEN that can raise an error (e.g. `assert_true`, overflow) may fail on rows that did not select it.
- Result type is FE's `TExprNode.type`, i.e. `data_type()`. FE has already cast the branches to it. No match and no ELSE gives `insert_default` on a nullable column, i.e. NULL (`h:189-193`).
- **Quirk: non-nullable numeric result** (int, float, decimal; `update_result_auto_simd`, `h:215-266`). The result is computed as `Σ_i (then_idx==i) * then_i[row]`, starting from 0 (`h:261-263`). For FLOAT/DOUBLE, a NaN or ±Inf in a *non-selected* branch at that row gives `0*Inf = NaN`, which poisons the result. A selected `-0.0` becomes `+0.0` (because `0.0 + -0.0 = +0.0`). Integers and decimals are exact (wrapping). Date/datetime types use a select, not a sum (`h:250-259`).
- Nullable, string or complex results copy row by row from the selected branch (`h:170-205`).
- More than 65535 branches is NotSupported (`cpp:112-114`).

## 6. `nullable(x)` and `assert_true(cond, msg)`

- **`nullable(x)`** (`exprs/function/function_nullables.cpp:36-66`): return type `make_nullable(arg)`. Values are unchanged. A non-nullable input is wrapped with an all-zero null map; a nullable input is passed through. `use_default_implementation_for_nulls=false`, so NULL stays NULL. It is a pure type marker.
- **`assert_true(cond, msg)`** (`exprs/function/function_assert_true.cpp:41-87`):
  - Returns BOOLEAN (non-nullable), always `true` for every row.
  - `msg` must be a constant, otherwise `InvalidArgument("assert_true only accept constant for 2nd argument")` at open.
  - If the column has **any NULL** in the block (whole-block check, `:72-75`), or any row is false (`:79-82`), it throws `Exception(Status::InvalidArgument(msg))`.
  - The exception string is `"[E33] " + msg` (`common/exception.cpp:52-57`; INVALID_ARGUMENT=33, no stack trace, `common/status.h:45`; thrift `Status.thrift:65`). `RETURN_IF_CATCH_EXCEPTION` turns it into `Status::Error(33, e.to_string())` (`common/exception.h:74-80`, used in `exprs/vexpr_context.cpp:76-104`).
  - Wire message: `({be_host})[INVALID_ARGUMENT][E33] {msg}`. Client sees `errCode = 2, detailMessage = (host)[INVALID_ARGUMENT][E33] correlate scalar subquery must return only 1 row` (FE's message for scalar subqueries, `regression-test/suites/nereids_p0/subquery/correlated_scalar_subquery.groovy:118`).
