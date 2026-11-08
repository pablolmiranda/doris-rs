# Epic 1 M6 Story 2: INTERSECT / EXCEPT — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** doris-rs runs `INTERSECT_NODE` and `EXCEPT_NODE` and answers as the C++ BE does, which makes 15 more gate suites eligible (349 → 364).

**Architecture:**
- A new module `doris-exec::set_op` holds the pure set logic over rows.
- The runner (`pipeline.rs`) runs each child and projects it by its result expr list. It hands
  the rows to `set_op`, then types the result onto the node's tuple.
- `check_plan` checks the node's parameters and expressions.

**Tech Stack:** Rust (`doris-exec`, `doris-be`, `doris-fixtures`), Python oracle.

**Spec:** `docs/superpowers/specs/2026-11-08-epic-1-m6-query-breadth-design.md` §2, §3, §5.

## Research

**The C++ BE** (`exec/operator/set_sink_operator.cpp`, `set_probe_sink_operator.cpp`,
`set_source_operator.cpp`, `exec/common/set_utils.h`):

1. **Build.** Child 0's rows, evaluated by `result_expr_lists[0]`, go into a hash table. Equal
   rows are one entry, so the result is DISTINCT.
2. **Probe.** Each later child `i`, evaluated by `result_expr_lists[i]`, marks the entries it
   finds.
   - INTERSECT keeps the marked entries after each child.
   - EXCEPT keeps the unmarked ones.
3. **Equality.** NULL equals NULL (`build_not_ignore_null`).
   - Numbers are keyed by their raw bits. Unlike aggregation, which calls
     `replace_float_special_values`, set operators do not normalize floats. So `-0.0` and `0.0`
     are different rows.
4. **Output order is hash-table iteration order** (`PHHashMap`, `HashCRC32`).
   - A single nullable key column uses `DataWithNullKey`, whose iterator yields the NULL entry
     first.
5. **What C++ ignores.** `const_expr_lists` and `first_materialized_child_idx` are never read.
   `is_colocate` is read: `required_data_distribution()` asks for a hash (or bucket-hash)
   shuffle on each child's result exprs. C++ adds a local exchange wherever a child is not
   already distributed that way, so equal rows meet in one pipeline task. doris-rs has no local
   shuffle, so in a fragment of several instances it refuses at prepare any set node whose rows
   might not meet (Task 2's fix pass).
6. **Generic node steps.** The node's conjuncts and limit apply to the output (`get_block_impl`).

**The corpus.** There are 15 newly eligible suites. Four of them can be replayed:
- `correctness_p0.test_set_operation`
- `nereids_p0.test_filter_pushdown_set`
- `query_p0.set_operations.set_with_null`
- `query_p0.set_operations.test_pullup_predicate_null`

The other 11 read tables loaded by another suite, so the full run covers them. Their plans look
like this:
- The set node runs in 1 or 4 instances, and has 2 to 6 children.
- Each child is an `EXCHANGE_NODE` fed by a HASH_PARTITIONED stream sink.
- `first_materialized_child_idx` is 0 and `const_expr_lists` is empty. There are no conjuncts
  or projections on the node.
- Some nodes (INTERSECT and EXCEPT) carry runtime filters. They are ignored, as everywhere in
  doris-rs. A filter built from child 0 only drops probe rows that are not in the build set, and
  those rows change neither result.

**Order.** Every reference reply with more than one row and no ORDER BY is marked `unordered`
except one: `test_pullup_predicate_null` #13, `{2,6,NULL} INTERSECT {4,NULL,2}`, which gives
`NULL, 2`. So doris-rs does not emulate the hash table's order. It emits:
- the NULL row first, when rows have one column;
- the remaining rows in child 0's first-seen order.

## Global Constraints

- **Byte-exact replies.** Rows equal the C++ BE's. A feature that can't match the C++ BE's
  reply is refused with a status naming it; a wrong answer is never acceptable.
- **Refuse at prepare.** A plan holding anything doris-rs does not run is refused at prepare,
  naming the first unsupported node, sink, function or expression.
- **No regression.** The ratchets hold. Every eligible suite passes both gates in the story's
  full run (eligible: 364).
- **Checks stay green.** The swap and readback checks pass.
- **Formatting.** Never `cargo fmt` whole packages. Format new files only, with
  `rustfmt --edition 2021 --config max_width=150`. Never rustfmt test files declaring `mod common`.
- **Commits.** Commits end with the session's Co-Authored-By and Claude-Session lines.

## Review Focus

1. **Several set instances.** Equal rows from different children must reach the same instance.
   `hash_values` is type-independent, so they do even when children's column types differ in
   width or nullability. Expected: an INTERSECT split over 4 instances gives the same rows as
   one instance.
2. **NULL in keys of more than one column.** Expected: `(1,NULL)` equals `(1,NULL)` and differs
   from `(NULL,1)`. Order stays first-seen; only one-column results put NULL first.
3. **`-0.0` and NaN.** Expected: kept apart from `0.0`, as C++ keys raw bits. The router may
   still send both to the same instance; that is harmless.
4. **A malformed node.** Fewer than 2 children; a number of result expr lists different from the
   number of children; lists of different lengths. Expected: BadRequest at prepare.
5. **Conjuncts or a limit on the set node.** Expected: applied after the set operation, by the
   runner's generic node steps.

Items 2, 3 and 4 have tests in Task 2. Item 1 is covered by the replays: `set_with_null`'s set
fragments run in 4 instances. Item 5 is covered by the generic code in `Instance::node`, which
every node type shares.

---

### Task 1: The set-operation replays

**Files:**
- Modify: `oracle/scores/epic1-supported.json` (add `plan_node:EXCEPT_NODE`, `plan_node:INTERSECT_NODE`)
- Create: `crates/doris-fixtures/replays/<suite>/` for the four suites above (via `extract-replays --only`)
- Test: `crates/doris-fixtures/tests/replays.rs`

**Interfaces:**
- Produces: `replays::load(S)` for each of the four suites; the constant list
  `SET_OPS: [&str; 4]` in each test file that uses them.

- [ ] **Step 1: Write the failing test** `set_op_replays_have_intersect_and_except`:
  - across the four replays' `queries()`, some plan node is `INTERSECT_NODE` and some is
    `EXCEPT_NODE`;
  - some set node's fragment has more than one instance (`local_params.len() > 1`).
- [ ] **Step 2: Run it.** `cargo test -p doris-fixtures --test replays set_op_replays`. Expected:
  FAIL (the replays do not exist).
- [ ] **Step 3:** Add the two features to the supported list. Then run
  `cargo run --release -p doris-wire --bin extract-replays -- crates/doris-fixtures/replays --only <the four suites>`.
- [ ] **Step 4: Run it.** Expected: PASS. `cargo test -p doris-fixtures`: all pass.
- [ ] **Step 5: Commit** ("M6 s2: the set-operation replays").

### Task 2: INTERSECT and EXCEPT

**Files:**
- Create: `crates/doris-exec/src/set_op.rs` (registered in `lib.rs`)
- Modify: `crates/doris-exec/src/pipeline.rs` (the runner), `crates/doris-exec/src/query.rs` (`check_node`)
- Test: `crates/doris-exec/tests/set_op.rs` (new), `crates/doris-be/tests/query.rs`, `crates/doris-be/tests/replay.rs`

**Interfaces:**
- Consumes: Task 1's replays.
- Produces:
  - `pub enum SetKind { Intersect, Except }`
  - `pub fn set_rows(kind: SetKind, children: Vec<Vec<Vec<Datum>>>) -> Vec<Vec<Datum>>`:
    - child 0's distinct rows, in first-seen order;
    - INTERSECT keeps the rows found in every other child, EXCEPT the rows found in none;
    - rows are equal when their values are equal, with NULL = NULL and floats compared by
      their bits;
    - when rows have exactly one column, a NULL row in the result comes first.
  - `pub fn set_node(n: &TPlanNode) -> Result<(SetKind, i32, &[Vec<TExpr>]), ExecError>` in
    `query.rs`: the kind, the tuple id and the result expr lists. It returns a BadRequest when:
    - the parameters are missing;
    - `num_children < 2` ("a set node with N children");
    - there are not `num_children` lists ("an INTERSECT_NODE with N children and M result expr
      lists");
    - the lists have different lengths ("result expr lists of different lengths").

- [ ] **Step 1: Write the failing tests.**

  In `crates/doris-exec/tests/set_op.rs` (single-column rows written as values; `N` = `Datum::Null`):
  - `intersect_keeps_distinct_rows_found_in_every_child`: `[3,1,1,2]`, `[1,3,4]`, `[3,1,5]` gives
    `[3,1]`.
  - `except_keeps_rows_found_in_no_other_child`: `[3,1,1,2,5]`, `[1]`, `[5]` gives `[3,2]`.
  - `null_equals_null_and_comes_first`: INTERSECT of `[2,6,N]` and `[4,N,2]` gives `[N,2]`
    (`test_pullup_predicate_null` #13).
  - `nulls_in_wider_rows_keep_first_seen_order`: INTERSECT of `[(1,N),(N,1),(1,N)]` and
    `[(N,1),(1,N)]` gives `[(1,N),(N,1)]`. EXCEPT of `[(1,N),(N,1)]` and `[(N,1)]` gives
    `[(1,N)]`.
  - `negative_zero_is_its_own_row`: with `Double(-0.0)` and `Double(0.0)` in child 0 and
    `Double(0.0)` in child 1:
    - EXCEPT gives `[-0.0]` (check the sign bit);
    - INTERSECT gives `[0.0]`.
  - `an_empty_child_zero_gives_nothing`: INTERSECT and EXCEPT of `[]` and `[1]` give `[]`.

  In `crates/doris-be/tests/query.rs`, `a_set_node_must_be_well_formed`:
  - take the first plan of `set_with_null` that has an `INTERSECT_NODE`; as sent,
    `check_plan` accepts it;
  - each of these mutations is a BadRequest at prepare:
    - one result expr list removed (the message contains "result expr lists");
    - one list's last expr removed ("different lengths");
    - `num_children` set to 1, with one list ("children");
  - a result expr replaced by a call to an unsupported function is refused naming it.

  In `crates/doris-be/tests/replay.rs`, `set_operations_match_the_reference`: each of the four
  replays has no mismatches.

- [ ] **Step 2: Run them.** Expected: compile failure (`set_op` missing), then
  `INTERSECT_NODE` refused.
- [ ] **Step 3: Implement.**
  - `set_op.rs` as in Interfaces. The row key is a byte encoding like `aggregate::canonical`,
    but floats are encoded by their bits, without normalizing.
  - The runner: `INTERSECT_NODE | EXCEPT_NODE` runs every child, projects child `i` by list `i`
    and calls `set_rows`. The result is typed onto the tuple's slots with `typed`. The generic
    steps (conjuncts, projections, limit) follow, as for every node.
  - `check_node`: `set_node(n)?`, then `check_exprs` over every list. `const_expr_lists` is
    ignored, as C++ ignores it.
- [ ] **Step 4: Run them.** Expected: PASS. Then `cargo test --workspace --no-fail-fast` (all
  pass) and `cargo clippy --all-targets --workspace` (no warnings).
- [ ] **Step 5: Commit** ("M6 s2: INTERSECT and EXCEPT").

### Task 3: Gate, docs, As built

**Files:**
- Modify: `oracle/scores/epic1-out.txt`, `oracle/scores/epic1-strict.txt` (from the run)
- Modify: `oracle/README.md` (an M6 story 2 paragraph), this plan (As built)

- [ ] **Step 1:** Make sure no BE holds the ports. Then reset the FE (`make fe-down`, remove
  `/tmp/doris-rs-oracle/storage`, `make fe-up`), start `make rust-be` with nohup, and run
  `RUN_ID=m6-s2-full-1 make epic-check TARGET=rust`. Expected: `eligible: 364; failing
  out-gate: 0; failing strict: 0`. Triage any failure with superpowers:systematic-debugging.
- [ ] **Step 2:** Stop the BE. Regenerate the ratchets from the grade, leaving out
  `test_move_column_with_cast`. Run `make -C oracle test`, `readback-check` and `swap-check`.
  Expected: all PASS.
- [ ] **Step 3:** Write the README paragraph and the As built section, with the run's numbers.
- [ ] **Step 4: Commit** ("M6 s2: gate, docs, As built").
