# Epic 1 M6 Story 3: window functions — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** doris-rs runs `ANALYTIC_EVAL_NODE` and `PARTITION_SORT_NODE` and answers as the C++ BE
does. This makes 14 more gate suites eligible (364 → 378).

**Architecture:**
- A new pure module, `doris-exec::analytic`, computes window results over rows that are already
  sorted. It finds each partition and peer group, takes each row's frame, and evaluates each
  function over that frame.
- A new pure module, `doris-exec::partition_sort`, holds the partition top-N filter.
- `pipeline.rs` runs both nodes.
- `check_plan` checks their parameters, functions, frames and distribution at prepare.

**Tech Stack:** Rust (`doris-exec`, `doris-be`, `doris-fixtures`), Python oracle.

**Spec:** `docs/superpowers/specs/2026-11-08-epic-1-m6-query-breadth-design.md` §2, §3, §5.

## Research

Two reports went into this section, both kept outside the repo:
- the corpus decode of all 14 suites;
- the C++ reading (Doris 4.1.5: `be/src/exec/operator/analytic_{sink,source}_operator.*`,
  `partition_sort_{sink,source}_operator.*`, `be/src/exec/sort/partition_sorter.*`,
  `be/src/exprs/aggregate/aggregate_function_window*.h`).

**Eligibility.** The spec's features (the two nodes; `row_number`, `rank`, `dense_rank`,
`lead`, `lag`, `first_value`) make 12 suites eligible. `last_value` adds 2 more:
`test_always_nullable_window_function` and `rowsFirstLast`. The FE rewrites some `first_value`
frames into `last_value` (`WindowFunctionChecker`), so the two functions come together. Eligible
goes from 364 to 378.

**ANALYTIC_EVAL_NODE (C++).**
1. **Input.** The input is already sorted by partition keys, then order keys (an
   `is_analytic_sort` SORT_NODE below it). Partition ends and peer-group ends are found by
   comparing `partition_exprs` and `order_by_exprs` one column at a time. The comparison treats
   NULL as equal to NULL, compares floats by value (-0.0 = 0.0, NaN = NaN) and strings by bytes.
   `partition_by_eq`, `order_by_eq` and `range_offset_predicate` are never read.
2. **Output.** Each input row, in input order, followed by one column per function. The row's
   tuples are the child's tuples plus `output_tuple_id`, which equals `intermediate_tuple_id`.
   Then the node's conjuncts (they hold the `rn <= N` filters), projections and limit apply.
3. **Frames.** A missing bound is UNBOUNDED. Offsets are `rows_offset_value` (PRECEDING n is
   -n, FOLLOWING n is +n). The frame is `[start, end)`, clamped to the partition:

   | Window | Frame of row `cur` in partition `[ps, pe)` |
   |---|---|
   | unset, or no start and no end (ROWS or RANGE) | `[ps, pe)` |
   | RANGE, no start, end CURRENT_ROW | `[ps, end of cur's peer group)` |
   | ROWS, no start, end at offset `e` | `[ps, cur+e+1)` |
   | ROWS, start at offset `s`, end at `e` (or no end) | `[cur+s, cur+e+1)` (or `[cur+s, pe)`) |
   | RANGE with any other bounds | never sent by the FE (it needs `range_between_offset_exprs`) |

   Values are added in row order from the frame's start. That is what C++'s cumulative and
   reset-and-recompute paths both do, so float sums agree bit for bit. With no order keys, the
   peer group is the whole partition.
4. **Functions.** Return types and nullability come from the FE (`AGG_EXPR`,
   `is_merge_agg=false`). The FE forces each rank-family function's frame.
   - `row_number`: `cur - ps + 1`.
   - `rank`: 1 + the number of rows before cur's peer group in the partition.
   - `dense_rank`: 1 + the number of earlier peer groups.
   - `lead(x, off, d)`: `x[cur+off]` if `cur+off < pe`, else `d` evaluated at row cur.
   - `lag(x, off, d)`: `x[cur-off]` if `cur-off >= ps`, else `d` at row cur. In both, `off` is
     a non-negative integer literal.
   - `first_value(x)` / `last_value(x)`: the frame's first or last row's x, or NULL if the frame
     is empty. With a second argument `true` (ignore nulls), the first or last non-NULL x in the
     frame, else NULL.
   - `min`, `max`, `sum`, `count`, `avg`: the same aggregate as the AGGREGATION_NODE, over the
     frame's rows. An empty frame gives NULL, except `count`, which gives 0.
5. **Distribution.** A node with partition keys needs its input hashed on them (HASH_SHUFFLE),
   or colocated (`is_colocate`, the rows of a bucket in one instance). A node with no partition
   keys runs as one instance.

**PARTITION_SORT_NODE (C++).**
- Rows are grouped by `partition_exprs`. Key equality is a hash table's: NULL equals NULL, and
  floats are compared by their bits. Each partition is sorted by `sort_info`; ties are found
  with `compare_at`. What is kept from each partition:
  - ROW_NUMBER: the first N rows;
  - RANK: the rows whose rank is at most N;
  - DENSE_RANK: the rows whose dense rank is at most N.

  N is `partition_inner_limit`, and `has_global_limit` forces ROW_NUMBER.
- **Passthrough.** Except in TWO_PHASE_GLOBAL, C++ stops filtering after 1024 partitions with
  fewer than 1000 rows each, so its output can be a superset. The window above it re-sorts and
  re-filters, so an exact filter gives the same final answer.
- **TWO_PHASE_GLOBAL** is exact, and the FE treats its output as sorted, so no SORT_NODE may sit
  between it and the window.
- **Output order.** Partitions in first-seen order, each one sorted.

**The corpus.**
- **Analytic nodes.** There are 20 analytic nodes in 15 plans. `window` is always set;
  `buffered_tuple_id` and `range_between_offset_exprs` never are. The child is a SORT_NODE, with
  these exceptions:
  - in `dual` and both `withGroupOrderHaving`, a UNION_NODE with no partition and no order keys;
  - in `check_partitionkey` plan #9, another analytic node.
- **Frames and functions.**
  - `row_number`: ROWS, unbounded to current row.
  - `rank`: RANGE, unbounded to current row.
  - `min`/`max`: RANGE unbounded to current row; RANGE with no bounds; ROWS with no bounds;
    ROWS from current row to 3 following.
  - `lead`/`lag`: offset 1, with a NULL DOUBLE default.
- **Instances.** 4 instances in `test_min_max_window`, `check_partitionkey` and the TPC-H
  suites, 1 elsewhere. Inputs are hashed on the partition key, a subset of it, or colocated
  (`test_min_max_window`, whose node runs in the scan fragment).
- **Partition sort.** 3 PARTITION_SORT_NODEs, all TWO_PHASE_LOCAL over an OLAP scan:
  `check_partitionkey` (RANK, limits 2 and 1) and `test_partition_topn` (ROW_NUMBER, limit 1).
- **Replays.** Six of the suites replay: `test_min_max_window`, `check_partitionkey`, both
  `withGroupOrderHaving`, `dual` and `test_partition_topn`. Task 1 also tries the two
  `last_value` suites. The TPC-H window suites read tables that only a stream load from S3
  creates, so the full run covers them.
- **Order.** Every multi-row window reply without ORDER BY is graded unordered, and the ordered
  ones sort on unique keys. `test_min_max_window` [8] needs ties on the order keys to keep scan
  order: `min(state)` over ROWS from current row to 3 following.

## Global Constraints

- **Byte-exact replies.** Rows equal the C++ BE's. A feature that can't match the C++ BE's
  reply is refused with a status naming it; a wrong answer is never acceptable.
- **Refuse at prepare.** A plan holding anything doris-rs does not run is refused at prepare,
  naming the first unsupported node, sink, function or expression.
- **No regression.** The ratchets hold, and every eligible suite (378) passes both gates in the
  story's full run.
- **The swap and readback checks stay green.**
- **Formatting.** Format new files only, with `rustfmt --edition 2021 --config max_width=150`.
  Never format a test file that declares `mod common`.

## Review Focus

1. **Rows of one partition split across instances.** A node with partition keys in several
   instances whose input isn't hashed on a subset of them (or colocated) is refused. So is a
   node with no partition keys in several instances. Each is a `a_window_node_must_be_runnable`
   mutation in Task 2.
2. **Frames the FE never sends**: RANGE with an offset, or a rank-family function over a frame
   other than the one the FE forces. Refused, not guessed (Task 2 mutations).
3. **NULL and float keys.** NULL partition keys form one partition, and -0.0 and 0.0 are peers.
   In partition sort, -0.0 and 0.0 are different partitions (Task 2 and Task 3 unit tests).
4. **Empty frames**: a sliding frame past the partition's end gives NULL, and 0 for `count`
   (Task 2 unit test).
5. **Stable ties.** Rows that tie on the order keys keep input order, so `min` over a ROWS frame
   sees them in scan order (Task 2 unit test, `test_min_max_window` replay).

---

### Task 1: Features and replays

**Files:**
- Modify: `oracle/scores/epic1-supported.json` (add `plan_node:ANALYTIC_EVAL_NODE`,
  `plan_node:PARTITION_SORT_NODE`, and `function:` `row_number`, `rank`, `dense_rank`, `lead`,
  `lag`, `first_value`, `last_value`; written with `json.dumps(indent=1)`)
- Create: `crates/doris-fixtures/replays/<suite>/` for each replayable suite (via
  `extract-replays --only`), and update `replays/index.json`
- Test: `crates/doris-fixtures/tests/replays.rs`

**Interfaces:**
- Produces: replay names for Task 2 and Task 3 (const `WINDOWS` in the test file and in
  `doris-be/tests/replay.rs`).

- [ ] **Step 1: Write the failing test** `window_replays_have_analytic_and_partition_sort`.
  The `WINDOWS` replays load, every one holds an `ANALYTIC_EVAL_NODE`, and
  `check_partitionkey` and `test_partition_topn` hold a `PARTITION_SORT_NODE`.
- [ ] **Step 2: Run** `cargo test -q -p doris-fixtures --test replays`. Expected: FAIL (the
  replays are missing).
- [ ] **Step 3:** Add the features. Then run `extract-replays --only` for the six suites and the
  two `last_value` suites, keeping each suite that extracts. Copy them by basename, never with
  `cp -R dir/`.
- [ ] **Step 4: Run** the test again. Expected: PASS.
  `every_replay_matches_the_reference` in doris-be stays red until Task 3.
- [ ] **Step 5: Commit** ("M6 s3: the window replays").

### Task 2: ANALYTIC_EVAL_NODE

**Files:**
- Create: `crates/doris-exec/src/analytic.rs` (registered in `lib.rs`)
- Modify: `crates/doris-exec/src/aggregate.rs`. Export `pub struct Accumulator` (wrapping
  `State`, with `new(&AggSpec)`, `update(&[(&ColType, &Datum)])` and `finish() -> Datum`), so a
  window aggregate is the AGGREGATION_NODE's own.
- Modify: `crates/doris-exec/src/query.rs` (`check_node`, a distribution check beside
  `check_sets`), `crates/doris-exec/src/pipeline.rs`, and `crates/doris-exec/src/eval.rs` (the
  window function names)
- Test: `crates/doris-exec/tests/analytic.rs`, `crates/doris-be/tests/query.rs`,
  `crates/doris-be/tests/replay.rs`

**Interfaces:**
- Produces:
  - `pub enum Frame { Partition, RangeToCurrent, Rows { start: Option<i64>, end: Option<i64> } }`
    and `pub fn frame(w: Option<&TAnalyticWindow>) -> Result<Frame, ExecError>`. RANGE with
    bounds other than (none, CURRENT_ROW) is `Unsupported "a RANGE window with offsets"`.
  - `pub struct WindowFn { kind: WindowKind, ret: ColType, args: Vec<TExpr> }`, with
    `WindowKind` = `RowNumber | Rank | DenseRank | Lead(i64) | Lag(i64) | First { ignore_nulls } | Last { ignore_nulls } | Agg(AggSpec)`,
    and `pub fn WindowFn::from_expr(e: &TExpr) -> Result<WindowFn, ExecError>`.
  - `pub fn window_columns(part: &[Vec<Datum>], part_types: &[ColType], order: &[Vec<Datum>], order_types: &[ColType], frame: &Frame, f: &WindowFn, args: &[(ColType, Vec<Datum>)]) -> Result<Vec<Datum>, ExecError>`:
    one result per row, where `part` and `order` are each row's key values.

- [ ] **Step 1: Write the failing unit tests** in `doris-exec/tests/analytic.rs`:
  - `row_number_rank_and_dense_rank_restart_per_partition`. Partitions a, a, a, b with order
    keys 1, 1, 2, 5 give row_number 1, 2, 3, 1; rank 1, 1, 3, 1; dense_rank 1, 1, 2, 1.
  - `null_partition_keys_are_one_partition_and_zero_signs_are_peers`. Keys NULL, NULL give
    row_number 1, 2. Order keys -0.0, 0.0 give rank 1, 1.
  - `range_to_current_includes_peers`. min over RANGE, order keys 2, 1, 1 sorted as 1, 1, 2 with
    values 5, 3, 4, gives 3, 3, 3. sum gives 8, 8, 12.
  - `rows_frames_clamp_to_the_partition`. min over ROWS 0..3 following of 4, 2, 9, 1, 7 gives
    1, 1, 1, 1, 7. Over ROWS 1..2 following, the last row gives NULL, and count gives 0.
  - `lead_and_lag_use_the_default_at_the_current_row`. lead(x, 1, d) of x = 1, 2, 3 with
    d = 10, 20, 30 gives 2, 3, 30. lag gives 10, 1, 2.
  - `first_and_last_value_with_and_without_nulls`. Over x = NULL, 2, NULL, 4 with whole-partition
    frames: first is NULL, ignore-nulls first is 2, last is 4. With ROWS to current row, last
    gives NULL, 2, NULL, 4, and ignore-nulls last gives NULL, 2, 2, 4.
  - `float_sums_add_in_row_order`. sum over ROWS to current row of 0.1, 0.2, 0.3 is bitwise
    `0.1 + 0.2` then `+ 0.3`.
  - `a_whole_partition_frame_without_order_keys`. With no order keys, rank is 1 for every row
    and RANGE-to-current min is the partition's min.
- [ ] **Step 2: Run** `cargo test -q -p doris-exec --test analytic`. Expected: FAIL (no module).
- [ ] **Step 3: Implement** `analytic.rs` as the Interfaces block says. Partitions are runs of
  equal keys and peer groups are runs of equal order keys. Use `same(a, b)`: NULL = NULL, floats
  by value, everything else by `eval::compare`. Aggregates go through `Accumulator` over the
  frame's rows, in order. A frame that starts at the partition's start may be extended
  incrementally.
- [ ] **Step 4: Run** the unit tests. Expected: PASS.
- [ ] **Step 5: Write the failing replay test** `window_functions_match_the_reference`
  (doris-be `replay.rs`, the `WINDOWS` replays). Expected: FAIL with
  `Unsupported ANALYTIC_EVAL_NODE`.
- [ ] **Step 6: Implement** `Instance::analytic` in `pipeline.rs`:
  1. Run the child.
  2. Evaluate the partition, order and argument exprs over it.
  3. Append the function columns.
  4. Type them onto `output_tuple_id`'s slots. The chunk's slots are the child's slots plus the
     output tuple's.
  5. The generic conjuncts, projections and limit follow.
- [ ] **Step 7: Write the failing prepare tests**: `a_window_node_must_be_well_formed` and
  `a_window_node_must_be_runnable` in doris-be `query.rs`, mutating a replayed plan. Each must
  be refused at prepare with its message:
  - missing parameters: BadRequest "ANALYTIC_EVAL_NODE without its parameters";
  - an unknown function (`no_such_function`): Unsupported;
  - RANGE with a PRECEDING start: Unsupported "a RANGE window with offsets";
  - rank over a ROWS frame: Unsupported "rank over a ROWS window";
  - a non-literal lead offset: Unsupported;
  - several instances:
    - no partition keys: Unsupported "an ANALYTIC_EVAL_NODE without partition keys in {n} instances";
    - an input fed by a RANDOM stream: Unsupported "an ANALYTIC_EVAL_NODE fed by a RANDOM stream in {n} instances";
    - an input hashed on a column that isn't a partition key: Unsupported "an ANALYTIC_EVAL_NODE hashed on other exprs than its partition keys in {n} instances".
- [ ] **Step 8: Implement the checks.**
  - `check_node` runs `frame` and `WindowFn::from_expr`, and `check_exprs` on the keys.
  - The distribution check in `check_plan` walks down from the node to its feeding exchange:
    - SORT_NODE: map each tuple slot to its `sort_tuple_slot_exprs` slot, or by position when
      there are none;
    - ANALYTIC_EVAL_NODE: pass its input slots through unchanged.

    It then requires the feeding sinks' hash exprs to be a subset of the partition key slots.
    A colocated node (`is_colocate`) is accepted. One instance needs nothing.
- [ ] **Step 9: Run** `cargo test -q --workspace` and
  `cargo clippy -q --workspace --all-targets`. Expected: PASS, except
  `every_replay_matches_the_reference` for the partition-sort replays until Task 3.
- [ ] **Step 10: Commit** ("M6 s3: ANALYTIC_EVAL_NODE").

### Task 3: PARTITION_SORT_NODE

**Files:**
- Create: `crates/doris-exec/src/partition_sort.rs`
- Modify: `crates/doris-exec/src/pipeline.rs`, `crates/doris-exec/src/query.rs`, and
  `crates/doris-exec/src/set_op.rs` (make `key` public as `set_op::row_key`, the bitwise key)
- Test: `crates/doris-exec/tests/partition_sort.rs`, `crates/doris-be/tests/query.rs`

**Interfaces:**
- Produces: `pub fn partition_top_n(rows: Vec<Vec<Datum>>, part: &[Vec<Datum>], cmp: impl Fn(usize, usize) -> Ordering, algo: TopNAlgorithm, n: usize) -> Vec<Vec<Datum>>`.
  It returns partitions in first-seen order, each sorted (stably) by `cmp`, keeping rows per
  `algo`.

- [ ] **Step 1: Write the failing unit tests**:
  - `row_number_keeps_the_first_n_per_partition`;
  - `rank_keeps_ties_at_the_limit`: order keys 1, 1, 2 with N=1 keep both 1s;
  - `dense_rank_keeps_n_distinct_values`;
  - `partitions_come_in_first_seen_order_sorted`;
  - `zero_signs_are_different_partitions_and_nulls_one`.
- [ ] **Step 2: Run** `cargo test -q -p doris-exec --test partition_sort`. Expected: FAIL.
- [ ] **Step 3: Implement** it. Group rows by `row_key` of their partition values.
- [ ] **Step 4: Implement** `Instance::partition_sort`:
  1. Materialize the child onto the node's tuple, as the SORT_NODE does, with
     `sort_tuple_slot_exprs` or by position. Factor that step out of `sort` and share it.
  2. Evaluate the partition exprs over the child and the ordering exprs over the node's tuple.
  3. N is `partition_inner_limit`, and the algorithm is ROW_NUMBER when `has_global_limit` is
     set.
- [ ] **Step 5: Write the failing prepare test** `a_partition_sort_node_must_be_well_formed`. Each
  case is refused:
  - missing parameters: BadRequest "PARTITION_SORT_NODE without its parameters";
  - no `sort_info`: BadRequest;
  - a negative limit: BadRequest;
  - `ptopn_phase` TWO_PHASE_GLOBAL in several instances: Unsupported "a global PARTITION_SORT_NODE in {n} instances". No eligible suite has one.
- [ ] **Step 6: Implement** the checks.
- [ ] **Step 7: Run** `cargo test -q --workspace` and clippy. Expected: all PASS, including
  `every_replay_matches_the_reference` and `window_functions_match_the_reference`.
- [ ] **Step 8: Commit** ("M6 s3: PARTITION_SORT_NODE").

### Task 4: Gate, docs, As built

**Files:**
- Modify: `oracle/scores/epic1-out.txt`, `oracle/scores/epic1-strict.txt` (from the run)
- Modify: `oracle/README.md` (an M6 story 3 paragraph), this plan (As built)

- [ ] **Step 1:** Make sure no BE holds the ports. Reset the FE (`make fe-down`, remove
  `/tmp/doris-rs-oracle/storage`, `make fe-up`), start `make rust-be` with nohup, and run
  `RUN_ID=m6-s3-full-1 make epic-check TARGET=rust`. Expected: `eligible: 378; failing
  out-gate: 0; failing strict: 0`. Triage any failure with superpowers:systematic-debugging.
- [ ] **Step 2:** Stop the BE. Regenerate the ratchets from the grade, leaving out
  `test_move_column_with_cast`. Run `make -C oracle test`, `readback-check` and `swap-check`.
  Expected: all PASS.
- [ ] **Step 3:** Write the README paragraph and the As built section, with the run's numbers.
- [ ] **Step 4: Commit** ("M6 s3: gate, docs, As built").
