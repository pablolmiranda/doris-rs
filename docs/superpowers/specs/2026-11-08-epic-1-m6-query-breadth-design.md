# Epic 1 · M6: query breadth — design

Status: chosen from the needs index after M5 (epic spec §6, row 6+), 2026-11-08. The user named
the seven stories and their order. The designer made every other call, listed in §6.

## 1. Intent

After M5, 348 of the 1,622 gate suites are eligible, and all of them pass both gates. The needs
index puts the rest in two groups.

- **338 suites** are blocked only by query features: plan nodes, functions, expression kinds and
  `INSERT … SELECT`. None of them needs new storage or new types.
- **The others** need something more: unique- or aggregate-key tables, new types, ALTER and so
  on. Those are later milestones.

M6 takes the first group. Those 338 suites need 254 features between them, but a few features
cover most of the suites: 32 features unlock 187 of them.

## 2. Stories

Each story is its own branch and pull request, stacked on the one before it (story 1 on M5's
branch). Each story starts once the previous one's PR is open. Each lands with its own plan, its
features added to `scores/epic1-supported.json`, replays or unit tests for every feature, and a
green `cargo test`, clippy and `make -C oracle test`.

"Adds" counts the gate suites that become eligible when the story lands, in this order. A suite
needs all of its features, so most suites wait for the joins (story 6) whatever lands first. The
total after story 6 does not depend on the order.

| # | Story | Features | Adds | Eligible after |
|---|---|---|---|---|
| 1 | CTEs | `sink:MULTI_CAST_DATA_STREAM_SINK` | 1 | 349 |
| 2 | INTERSECT / EXCEPT | `plan_node:INTERSECT_NODE`, `plan_node:EXCEPT_NODE` | 15 | 364 |
| 3 | Window functions | `plan_node:ANALYTIC_EVAL_NODE`, `plan_node:PARTITION_SORT_NODE`; `row_number`, `rank`, `dense_rank`, `lead`, `lag`, `first_value` | 12 | 376 |
| 4 | `INSERT … SELECT` | `write:INSERT_SELECT` | 18 | 394 |
| 5 | Arithmetic and common scalar functions | `expr:ARITHMETIC_EXPR`, `expr:CASE_EXPR`; `add`, `subtract`, `multiply`, `divide`, `mod`, `like`, `if`, `round`, `concat`, `nullable`, `ifnull`, `length`, `coalesce`, `abs`, `assert_true`; `plan_node:ASSERT_NUM_ROWS_NODE` | 73 | 467 |
| 6 | Joins | `plan_node:HASH_JOIN_NODE`, `plan_node:CROSS_JOIN_NODE` | 68 | 535 |
| 7 | The long tail | the remaining ~222 query features, in ranking order | up to 151 | up to 686 |

These numbers come from the grader's own eligibility (`checker/derive.py` with the
supplement), with the features of earlier stories counted as supported. Each story's plan
recounts its number from the ranking at the time it starts.

**Story 7** is open-ended: its features are taken in batches, most suites unlocked first, until
the user stops it. Its plan names each batch.

## 3. What every story keeps

- **Byte-exact replies.** Rows equal the C++ BE's, as in M5. A feature that can't match the C++
  BE's reply is refused with a status naming it. A wrong answer is never acceptable.
- **The plan check.** A plan that holds anything doris-rs does not run is refused at prepare,
  naming the first unsupported node, sink, function or expression, as in M5.
- **No regression.** The ratchets hold, and every eligible suite passes both gates in the
  story's full run.
- **The swap and readback checks stay green.** When a story makes a readback query answerable
  (the join, in story 6), doris-rs answers it in rounds b and c.

## 4. Story 1: CTEs (the multicast sink)

The FE plans a CTE that is used more than once as a producer fragment with a
`MULTI_CAST_DATA_STREAM_SINK`. Its `TMultiCastDataStreamSink` holds a list of `TDataStreamSink`s
and a list of destination lists, one pair per consumer.

The C++ BE (`PipelineFragmentContext::_create_data_sink`, `MultiCastDataStreamSinkOperatorX`,
`MultiCastDataStreamerSourceOperatorX`) handles it in three steps:
1. it buffers each block the fragment produces;
2. each consumer `i` gets its own copy, filtered by `sinks[i].conjuncts` first and then projected
   by `sinks[i].output_exprs` onto `sinks[i].output_tuple_id`;
3. it sends that copy through an ordinary exchange sink built from `sinks[i]` and
   `destinations[i]`.

The consumers read with ordinary `EXCHANGE_NODE`s.

doris-rs does the same with M5's in-process exchanges:
- the fragment's output chunk goes to each consumer in turn, through the same code the
  `DATA_STREAM_SINK` uses;
- the plan check checks each consumer's partition and expressions;
- the sender check counts a multicast fragment's instances for each exchange that a consumer of
  it feeds.

**Done when:**
- `correctness_p0.test_subquery_grouping` is replayed and its rows match the reference's;
- a unit test sends one chunk to two consumers with different filters and projections;
- the sender check accepts a corpus multicast plan and refuses a miscounted one;
- the story's full run is clean, with the eligible count 349.

## 5. Stories 2–7

Each later story's plan does its own research (decoded corpus plans, C++ citations), the way
M5's did, and names its replays. The constraints in §3 apply to every one of them.

## 6. Calls made by the designer (not by the user)

- **One spec for M6, one plan per story.** Each plan is written when its story starts, against
  the code the previous story left. A plan written today for story 6 would be stale by then.
- **The story 3 and story 5 feature lists** are the most-needed window and scalar features
  from the ranking. Each story's plan may add the ones its replayed suites also need.
- **Story 1's replay.** `test_subquery_grouping` is the only gate suite that multicast alone
  unlocks. The other 12 CTE suites also need joins or arithmetic. Replays for them come with
  stories 5 and 6.
- **Story PRs are stacked.** When a story's PR merges into its base branch rather than main,
  the next story's PR carries that story's commits to main, as #28 did for M4.
