# Epic 1 M6 Story 7, Batch 1: The Long Tail — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** doris-rs runs the 13 query features that unlock the most suites per feature, as the C++
BE does. That is 11 functions (`ndv`, `assert_true`, `regexp`, `to_date`, `sum0`, `stddev`,
`ntile`, `group_concat`, `eq_for_null`, `concat_ws`, `window_funnel_v2`) and two plan nodes
(`REPEAT_NODE`, `DATA_GEN_SCAN_NODE`). Eligible goes 551 → 586, all passing both gates.

**Architecture:**
- **Scalars** go in `eval.rs`'s dispatch, beside `like`, `concat` and `length`. `regexp` gets
  its own module, `regexp.rs`: a classifier, a syntax gate over `regex-syntax`'s AST, and RE2's
  error texts.
- **Aggregates** are new `State` variants in `aggregate.rs`, in the existing
  update/merge/finalize model. `ndv` keeps its HLL in a new module, `hll.rs`.
- **ntile** is a new `WindowKind` in `analytic.rs`.
- **REPEAT_NODE and DATA_GEN_SCAN_NODE** are new nodes in `pipeline.rs`, checked in `query.rs`.
  The data-gen scan is a serial source, so story 6's merged mode covers `parallel_instances`.

**Tech Stack:** Rust (`doris-exec`, `doris-be`, `doris-fixtures`, `doris-wire`). `regex-syntax`
becomes a direct dependency of `doris-exec` (it is already in the lock file through `regex`).
Python oracle.

**Spec:** `docs/superpowers/specs/2026-11-08-epic-1-m6-query-breadth-design.md` §2 (row 7:
"features are taken in batches, most suites unlocked first, until the user stops it. Its plan
names each batch"), §3.

## Batch 1

The recount (`checker/derive.py` with the supplement; supported = `scores/epic1-supported.json`):
**154** suites are blocked only by query features, and they need **222** features between them.
They were ranked greedily by suites unlocked per new feature. After the top 14, nearly every
feature unlocks one suite.

| Feature | Suites |
|---|---|
| `ndv` | 10 (5 need `assert_true` too) |
| `assert_true` | 5 |
| `REPEAT_NODE`, `DATA_GEN_SCAN_NODE`, `regexp` | 3 each |
| `to_date`, `eq_for_null`, `concat_ws`, `stddev`, `window_funnel_v2`, `sum0`, `group_concat`, `ntile` | 2 each |

These features make **35** suites eligible (551 → 586). **16 extract as replays**
(`extract-replays --only`); the scratchpad keeps the list in `b1-replayable.txt`. The other 19
are rejected for:
- a table created by a shared load suite (12);
- a stream load (5);
- no eos (3);
- more than 40 result queries (1);
- a partial update (1).

The full run checks those.

## Research

The three research docs hold the C++ semantics, with file and line citations:
- `docs/superpowers/research/2026-11-09-m6-s7-scalars.md`: regexp, to_date, eq_for_null,
  concat_ws, assert_true.
- `docs/superpowers/research/2026-11-09-m6-s7-aggregates.md`: ndv, stddev, sum0, group_concat,
  window_funnel_v2, ntile.
- `docs/superpowers/research/2026-11-09-m6-s7-nodes-sink.md`: REPEAT_NODE, DATA_GEN_SCAN_NODE,
  GROUP_COMMIT_BLOCK_SINK.

Each task below reads its section. The rules that decide a test's expected value are repeated
in that task.

## Calls (the designer's, for the user to check)

- **The group-commit sink (`sink:GROUP_COMMIT_BLOCK_SINK`, 4 suites) moves to batch 2.** It is
  load machinery, not query execution:
  - per-table queues with labels shared across inserts, timers and byte thresholds;
  - an internal `streamLoadPut` plan;
  - sync waits released by cancel;
  - the stream-load header path.

  Research sizes it at 5–8 times the other 12 features together.
- **Boost regexes are limited to a narrow gate.** With `enable_extended_regex`, a pattern RE2
  rejects falls back to Boost in C++. doris-rs accepts it only when the pattern is ASCII, its
  lookarounds are `(?=…)`, `(?!…)` or fixed-length lookbehinds, and it has no nested unbounded
  quantifier. Anything else is refused, naming "a Boost regex". The suite's `(?=a)(a+)+b` block
  expects Boost's backtracking-limit error, so it is refused, and
  `test_regexp_extended_nonconstant_pattern` may fail. If it does, that suite goes into
  `needs_supplement.json` as `regex:boost_backtrack_limit`, with the full run's evidence.
- **Hyperscan's constant path is matched with `regex` behind the gate.** `\d \w \b \B` are
  rewritten to their ASCII forms and `\s` to RE2's set. `(?i)` and `\s` on the constant path
  are refused, because Hyperscan's handling of them is unverified. A value ending in `\n` against
  a constant pattern with `$` is refused at run time.
- **`ntile` refuses a partition larger than one 4064-row block** at run time. C++ then numbers
  its buckets from a partial partition size.
- **`ndv` is exact HLL**: MurmurHash64A with seed `0xadc83b19`, an explicit set up to 160
  values, 16384 registers, and an f32 estimate. At exactly 160 distinct values, C++'s answer
  depends on how its instances split the rows. doris-rs merges instance states in instance
  order, and the gate's suites stay far below 160.

## Global Constraints

- Byte-exact replies (spec §3). A feature that cannot match C++ is refused, naming it.
- Never `cargo fmt` whole packages. Format only new files, with
  `rustfmt --edition 2021 --config max_width=150`. Never rustfmt test files that declare
  `mod common`.
- `.cache/doris-src` is read-only.
- Commits end with the session's Co-Authored-By and Claude-Session lines.

## Review Focus

1. Regexp on non-ASCII data with `\w`, `\d` or `\b`: rewritten to ASCII, so matches equal
   RE2's.
2. ndv over strings of different lengths and over NULLs: NULLs are skipped, strings are hashed
   over their raw bytes, and the count is exact below 160.
3. stddev of one row (0), and stddev with an infinity (NaN). The empty merge still runs the
   formula.
4. REPEAT_NODE over several child chunks: the output is each chunk × each repeat, in order.
5. group_concat with a NULL in its argument: the row is skipped, and an empty result is NULL.

---

### Task 1: Features and replays

**Files:** `oracle/scores/epic1-supported.json`, `crates/doris-fixtures/tests/replays.rs`,
`crates/doris-be/tests/replay.rs`, `crates/doris-fixtures/replays/` (the 16 new replays).

- [ ] **Step 1: Write the failing test** `batch1_replays_hold_their_features` in
  `doris-fixtures/tests/replays.rs`. Every replay in the new `BATCH1` list exists, and the set
  of its features includes at least one batch-1 feature.
- [ ] **Step 2: Run** `cargo test -q -p doris-fixtures --test replays`. Expected: FAIL (missing
  replays).
- [ ] **Step 3:** Add the 13 features to `epic1-supported.json` (written with
  `json.dumps(indent=1)`), then run `extract-replays --only` for the 16 suites.
- [ ] **Step 4: Run** the test. Expected: PASS. Add `batch1_matches_the_reference` to
  `doris-be/tests/replay.rs` over `BATCH1`; it stays red until Task 7.
- [ ] **Step 5: Commit** ("M6 s7: batch 1 replays").

### Task 2: to_date, eq_for_null, concat_ws, assert_true

**Files:** `eval.rs`, `strings.rs`, `cond.rs`; tests in those modules' test files.

Rules (scalars §2–§5):
- **`to_date`** takes `DATETIMEV2` to `DATEV2` as `v >> 37`, and NULL to NULL. Other signatures
  are refused.
- **`eq_for_null`** (BINARY_PRED opcode 65, and the function name) gives:
  - true when both sides are NULL;
  - false when only one side is NULL;
  - otherwise the existing `eq`.

  The result is never NULL.
- **`concat_ws(sep, args...)`** gives NULL when `sep` is NULL. NULL args are skipped, and the
  rest are joined with `sep`. An array argument is refused.
- **`assert_true(cond, 'msg')`** needs a string-literal second argument, else it is refused. Any
  row with a NULL or false condition fails the query with `Coded{INVALID_ARGUMENT, "[E33] msg"}`.
  Otherwise the result is a column of true.

- [ ] **Step 1: Write the failing tests:**
  - `to_date_takes_the_date_bits` (`2024-03-05 10:11:12` → `2024-03-05`; NULL → NULL);
  - `eq_for_null_is_two_valued` (NULL⇔NULL true, NULL⇔1 false, 1⇔1 true);
  - `concat_ws_skips_null_args_and_null_sep_is_null`;
  - `assert_true_fails_with_e33` (the message is `[E33] ANALYZE_SKIP_LONG_STRING_COLUMN`, the
    status INVALID_ARGUMENT);
  - `assert_true_needs_a_literal_message` (refused at prepare).
- [ ] **Step 2: Run** `cargo test -q -p doris-exec`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** ("M6 s7: to_date, eq_for_null, concat_ws, assert_true").

### Task 3: regexp

**Files:** create `crates/doris-exec/src/regexp.rs`; modify `eval.rs` and `clock.rs` (query
options beside the clock) and `doris-exec/Cargo.toml` (`regex-syntax`).

Rules (scalars §1, §6):
- **Classify the pattern like C++:** ALLPASS, EQUALS, STARTS, ENDS or SUBSTRING. Otherwise:
  - a constant pattern compiles at prepare (Hyperscan's path), even over no rows;
  - a per-row pattern compiles per row, NULL-value rows included, in row order (RE2's path).

  A NULL pattern counts as `""`.
- **The gate** accepts the subset the scalars doc lists and rewrites `\d \w \b \B \s` to ASCII.
  It refuses the rest, naming the construct. Exception: the RE2 errors the doc lists come back
  as `INTERNAL_ERROR` with RE2's text.
  - Without `enable_extended_regex`, the message ends with "try setting
    enable_extended_regex=true", as C++'s does.
  - With `enable_extended_regex`, the Boost gate applies (see Calls). A Boost-path pattern
    that fails to compile gives `Invalid regex expression: {p}. Error: `.
- `enable_hyperscan_fallback = false` with a constant pattern is refused.

- [ ] **Step 1: Write the failing tests** in `regexp.rs`:
  - `classifies_like_cpp` (`'^abc'` STARTS, `'abc$'` ENDS, `'abc'` SUBSTRING, `'.*'` ALLPASS);
  - `ascii_classes_on_utf8` (`'\w+'` on `'中文'` does not match; `'[中文]'` does);
  - `re2_rejects_lookahead_with_its_text` (`'(?=a)'` → `invalid perl operator: (?=` plus the
    hint);
  - `per_row_patterns_compile_in_row_order`;
  - `constant_pattern_fails_at_prepare_over_no_rows`;
  - `boost_gate_accepts_lookahead_and_refuses_nested_quantifiers`;
  - the suite's own rows from `test_regexp_chinese` and `test_like_predicate_with_concat`.
- [ ] **Step 2: Run.** Expected: FAIL.
- [ ] **Step 3: Implement.** Use `regex::bytes` for the RE2 and Hyperscan paths, and
  `fancy-regex` for the Boost gate, added if the gate needs lookaround (ruling recorded).
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** ("M6 s7: regexp").

### Task 4: sum0, stddev, group_concat

**Files:** `aggregate.rs`, `eval.rs` (`AGGREGATES`); tests in `aggregate.rs`'s tests.

Rules (aggregates §2–§4):
- **`sum0`** is `sum` with a non-nullable result: 0 over no rows. BIGINT only; other types are
  refused.
- **`stddev` (pop)** uses Welford.
  - Add: `delta = x - mean; mean += delta / n; m2 += delta * (x - mean)`, in C++'s exact order.
  - Merge goes through the formula even into an empty state.
  - Finalize: count 0 gives NULL, count 1 gives 0, and an infinite or NaN `m2` gives NaN.
- **`group_concat(x[, sep])`** takes its separator from the first accepted row, `","` by
  default.
  - A row with any NULL argument is skipped.
  - No accepted row gives NULL.
  - Ordering (`agg_sort_infos`) and DISTINCT are refused.

- [ ] **Step 1: Write the failing tests:**
  - `sum0_of_no_rows_is_zero`;
  - `stddev_of_one_row_is_zero`;
  - `stddev_of_an_infinity_is_nan`;
  - `stddev_merges_two_phases_like_one` (values from `aggregate_stddev_over_range`'s .out);
  - `group_concat_skips_nulls_and_keeps_input_order`;
  - `group_concat_of_no_rows_is_null`.
- [ ] **Step 2: Run.** Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** ("M6 s7: sum0, stddev, group_concat").

### Task 5: ndv, window_funnel_v2, ntile

**Files:** create `crates/doris-exec/src/hll.rs`; modify `aggregate.rs` and `analytic.rs`.

Rules (aggregates §1, §5, §6):
- **`ndv`** hashes each non-NULL value with MurmurHash64A (seed `0xadc83b19`): numbers over
  their native little-endian width, strings over their raw bytes. A hash of 0 is skipped.
  - The state is an explicit set up to 160 hashes, then 16384 registers. The 160-boundary rules
    for update and merge are as the doc writes them.
  - The estimate is computed in f32/f64 as in §1.
  - The result is not nullable; over no rows it is 0.
- **`window_funnel_v2(window, mode, ts, cond...)`** keeps a packed event list. Merge is a stable
  sort of the concatenation.
  - The four modes are ported line for line.
  - The window is in calendar seconds.
  - TIMESTAMPTZ is refused.
- **`ntile(n)`** gives bucket `i` (1-based) by C++'s formula in §6. A partition larger than 4064
  rows is refused at run time.

- [ ] **Step 1: Write the failing tests:**
  - `murmur64a_matches_known_vectors`;
  - `ndv_is_exact_below_160`;
  - `ndv_switches_to_registers_past_160` (a value computed by the formula over a fixed input);
  - `ndv_skips_nulls`;
  - `window_funnel_v2_modes` (the suite's .out rows);
  - `ntile_splits_like_cpp` (7 rows, `ntile(3)` → 1,1,1,2,2,3,3);
  - `ntile_refuses_a_partition_past_one_block`.
- [ ] **Step 2: Run.** Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** ("M6 s7: ndv, window_funnel_v2, ntile").

### Task 6: REPEAT_NODE and DATA_GEN_SCAN_NODE

**Files:** `pipeline.rs`, `query.rs`; tests in `crates/doris-exec/tests/run_plan.rs`.

Rules (nodes-sink §1, §2):
- **REPEAT_NODE** output is `exprs`, then one non-null BIGINT per `grouping_list` row, with
  value `grouping_list[g][r]`.
  - `slot_id_set_list[r]` lists the slots *kept* in repeat r. Every other slot of
    `all_slot_ids` is NULL.
  - Rows come out per child chunk × each repeat in order.
  - Then conjuncts, limit and projections.
  - `grouping_list` shorter than the repeat count gives C++'s error text.
- **DATA_GEN_SCAN_NODE** supports `NUMBERS` only.
  - It produces `number` 0..N-1 (or a repeated `const_value`) as one non-null BIGINT column,
    from the one scan range.
  - An instance without a range produces nothing.
  - Anything else is refused at prepare.

- [ ] **Step 1: Write the failing tests:**
  - `repeat_emits_each_chunk_per_repeat`;
  - `repeat_nulls_slots_outside_the_kept_set`;
  - `repeat_with_no_exprs_outputs_grouping_columns_only`;
  - `numbers_counts_from_zero`;
  - `numbers_with_const_value_repeats_it`;
  - `data_gen_refuses_other_functions`.
- [ ] **Step 2: Run.** Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** ("M6 s7: repeat and data-gen scan").

### Task 7: The replays pass

- [ ] **Step 1: Run** `cargo test -q -p doris-be --test replay`. Expected: FAIL only in
  `batch1_matches_the_reference`. Fix each mismatch test-first, with a unit test per root
  cause.
- [ ] **Step 2: Run** `cargo test -q --workspace --no-fail-fast`. Expected: PASS.
- [ ] **Step 3: Commit** ("M6 s7: the batch 1 replays pass").

### Task 8: Gate, docs, As built

- [ ] **Step 1: Full run** on a fresh FE with storage wiped:
  `RUN_ID=m6-s7b1-full-N make epic-check TARGET=rust`, then `make epic-grade`. Expected: 586
  eligible (fewer if supplements are added), 0 failing in either gate, no ratchet suite lost.
- [ ] **Step 2:** Regenerate the ratchets from `grade.json`, as story 6 did.
- [ ] **Step 3:** Run `make -C oracle test`, the readback and swap checks, and
  `cargo test --workspace`.
- [ ] **Step 4:** Write the README paragraph and the As built section, then commit ("M6 s7
  batch 1: gate, docs, As built").
