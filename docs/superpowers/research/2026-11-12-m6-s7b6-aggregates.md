# Batch 6 research: stddev_samp, variance, variance_samp (+ window forms), percentile_approx, cume_dist, percent_rank, multi_distinct_sum, multi_distinct_group_concat, any_value

C++ sources: `be/src/exprs/aggregate/aggregate_function_stddev.{h,cpp}`, `aggregate_function_percentile.{h,cpp}`,
`be/src/util/tdigest.h`, `aggregate_function_window.h:203-334` (percent_rank, cume_dist),
`aggregate_function_distinct.{h,cpp}` (the `multi_distinct_` combinator), `aggregate_function_uniq.{h,cpp}`
(multi_distinct_count), `aggregate_function_null.h:419-527` (NULL wrapper in windows),
`be/src/exec/operator/analytic_sink_operator.cpp` (frame executors), `nested_loop_join_probe_operator.cpp` (CROSS mark join).

Scratch (`S` = the session scratchpad, nothing in the repo):
- plan dump of every AGG/ANALYTIC node of the six suites: `S/b6/aggdump.txt`, compacted `S/b6/agg2.txt`
  (window `W1`=ROWS, `W0`=RANGE; bound `B0`=CURRENT_ROW, `B1:n`=n PRECEDING, `B2:n`=n FOLLOWING)
- reply printer `S/b6/sim/replies.py REPLIES.jsonl.gz [substr...]`; decoded replies `S/b6/sim/<suite>.txt`
- simulator `S/b6/sim/stddev.py <decoded replies>`: stddev/stddev_samp/variance/variance_samp and percentile_approx over
  every recorded window query — **all 138 recorded results reproduced bit for bit** (54 + 54 + 30);
  `S/b6/sim/stddev_fma.py` is the same with a fused `m2` update (Python 3.14 `math.fma`), and it also matches
  all 138, so this corpus cannot tell whether the BE fuses (see Risks)
- `S/b6/sim/tdthr.py`: the size below which a t-digest never merges two points (see percentile_approx)
- `S/b6/chk`: copy of the plan-check runner

## Suites

| suite | what it needs | other blockers (from the last live runs `S/s7b4-full2.log`) |
|---|---|---|
| correctness_p0.test_select_stddev_variance_window | 30 window queries: stddev, stddev_samp, variance, variance_samp, percentile_approx(k2,0.5,4096) × 6 frames, plus percentile(k2,0.8) (already ported) | live: "doris-rs does not support stddev over a window yet" |
| nereids_p0…window_functions.test_select_stddev_variance_window | the same 30 queries, twice (60) | same |
| query_p0…window_functions.test_select_stddev_variance_window | 60 as above + row_number/rank/dense_rank (already ported) | same |
| nereids_p0…window_functions.test_window_fn | percent_rank(), cume_dist() over `PARTITION BY four ORDER BY ten` | live failure is earlier: "ANALYTIC_EVAL_NODE over an AGGREGATION_NODE in 5 instances" (`rank() over (order by sum(..)+sum(..))` on a GROUP BY) — not an aggregate issue, must be fixed too. (query_p0's test_window_fn fails the same way live but has no recorded traffic.) |
| nereids_p0.multi_distinct.multi_distinct_window | multi_distinct_sum, multi_distinct_group_concat over the whole partition and over `ROWS 1 PRECEDING AND 1 PRECEDING`; **multi_distinct_count over that ROWS frame** (doris-rs's `check_frame` allows it only over the whole partition) | live: "does not support function multi_distinct_sum yet"; the last query is an FE error |
| nereids_p0.subquery.subquery_unnesting | any_value (two-phase grouped, researched in `2026-11-10-m6-s7b3-aggregates.md`) | **confirmed: still needs a CROSS_JOIN mark join** (7 `NESTED_LOOP` `CROSS_JOIN is_mark=true`, `markconj=0`; live: "CROSS_JOIN_NODE with join op CROSS_JOIN (mark)"; `query.rs:483-491` lets mark through only for semi/anti). Plus the stats-table partial update noted in the b3 doc. Both any_value queries end in `(<be-host>)[INVALID_ARGUMENT]correlate scalar subquery must return only 1 row`. |

## Common facts

- **Doubles go on the wire in shortest round-trip form with no `.0`**: `0`, `2`, `16511`, `1989`, `0.5`,
  `0.6666666666666666`, `10887.581821431579` (the `.out` files say `0.0`/`2.0`: that is the JDBC client). doris-rs's
  `row_text.rs` already does this.
- FE names: `stddev_pop`→`stddev`, `variance_pop`/`var_pop`→`variance`, `var_samp`→`variance_samp`. Recorded:
  `stddev`, `stddev_samp`, `variance`, `variance_samp`, all `(DOUBLE) -> DOUBLE`, AGG_EXPR type `DOUBLE?`,
  `params=[DOUBLE]`, argument `CAST(SMALLINT k2 AS DOUBLE)`; `percentile_approx(DOUBLE,DOUBLE,DOUBLE) -> DOUBLE?`
  with `FLOAT_LITERAL 0.5`, `FLOAT_LITERAL 4096`; `percent_rank() -> DOUBLE`, `cume_dist() -> DOUBLE` (not nullable);
  `multi_distinct_sum(INT) -> BIGINT?`, `multi_distinct_group_concat(VARCHAR(65533)) -> VARCHAR(65533)?`,
  `multi_distinct_count(INT) -> BIGINT` (not nullable). All recorded uses of these are **ANALYTIC nodes** with
  `merge=false` (no two-phase use in these suites), except any_value.
- Window evaluation in C++ (`analytic_sink_operator.cpp:50-100, 196-335`):
  - no window / no bounds → `_get_next_for_partition`: the whole partition is added once at the partition start; every
    row gets `insert_result_into` of that state.
  - RANGE UNBOUNDED PRECEDING..CURRENT ROW → `_get_next_for_unbounded_range`: per peer group, add the group's rows
    (in order) to the running state, no reset.
  - ROWS with no start → `_get_next_for_unbounded_rows`: cumulative, one row at a time, no reset (for an n FOLLOWING
    end, the first row adds `[ps, ps+n]` then each row adds one more).
  - ROWS with a start → `_get_next_for_sliding_rows`: functions without `supported_incremental_mode()` (all of this
    batch's) **reset the state and re-add the frame `[max(cur+s,ps), min(cur+e+1,pe))` in row order for every row**.
  - The state is also reset at every partition start. An empty frame gives NULL when the FE's result type is nullable
    (`use_null_result`); a not-nullable result (count, multi_distinct_count) gives the empty state's value (0).
  - doris-rs's `analytic.rs` accumulator (fresh state unless the frame start is unchanged and the end grows) adds the
    same rows in the same order for these functions, so results equal C++'s — *except* for the Distinct-combinator
    quirk below.
- **The FE reverses frames with a bounded start and an unbounded end.** `ROWS BETWEEN 3 PRECEDING AND UNBOUNDED
  FOLLOWING` arrives as `ROWS start=None end=3 FOLLOWING` with the sort below the ANALYTIC node `k6 ASC, k1 DESC`
  (`b3pdump`: `asc=[true, false]`); `CURRENT ROW AND UNBOUNDED FOLLOWING` as `ROWS start=None end=CURRENT ROW` over
  `k1 DESC`. Accumulation therefore runs in descending k1, and the doubles depend on it: the simulator in ascending
  order fails stddev_samp/variance/variance_samp for these frames, descending matches. doris-rs only has to
  accumulate in input order (it does).
- Plan shape: scan (4 instances, 5 buckets) → hash exchange on k6 → SORT(k6, k1 [DESC]) → ANALYTIC (4 instances) →
  SORT(k1) → merge. One partition per instance; k1 is unique, so no peer ties.

## stddev / stddev_samp / variance / variance_samp (`aggregate_function_stddev.h`)

One state for all four (doris-rs `State::Stddev`, tag 6, already exact for `stddev`): `mean: f64, m2: f64, count: i64`.
- add(x) (`:110-120`): `delta = x - mean; r = delta / (1 + count) as f64; mean += r; m2 += ((count as f64) * delta) * r;
  count += 1`. (doris-rs `aggregate.rs:579-587` already has exactly this.)
- merge(rhs) (`:99-108`): if rhs.count == 0 return; `delta = mean - rhs.mean; n = (count + rhs.count) as f64;
  mean = rhs.mean + (delta * count as f64) / n; m2 = (rhs.m2 + m2) + (((delta*delta) * rhs.count as f64) * count as f64) / n;
  count = n as i64`. (doris-rs `aggregate.rs:692-700` already exact.) Runs even when self is empty.
- finalize — all wrapped by `NullableAggregateFunction` (a NULL row is skipped; no accepted row → NULL):
  - `variance` (pop): `count == 1 → 0.0`, else `m2 / count as f64`; inf → NaN.
  - `stddev` (pop): `count == 1 → 0.0`, else `sqrt(m2 / count as f64)`; inf → NaN.
  - `variance_samp`: `count == 0 || count == 1 → 0.0` (**not NULL**; `insert_default`), else `m2 / (count - 1) as f64`, inf → NaN.
  - `stddev_samp`: same guard → 0.0, else `sqrt(m2 / (count - 1) as f64)`, inf → NaN.
  - So one row gives `0` for all four (recorded: `current row and current row` → all `0`); an empty frame or all-NULL
    input gives NULL.
- Serialized (C++, informational): mean, m2, count = 24 bytes LE; the NULL wrapper adds a flag byte.
- doris-rs needs: `stddev_samp`, `variance`, `variance_samp` in `AGGREGATES` (`eval.rs:26`), the `State::Stddev`
  constructor for them (`aggregate.rs:489`), the three finalizers, and removal of `stddev` from the refused list in
  `analytic.rs:154` (and allowing the new three over windows). The serialized tag can be shared (the merge is identical)
  provided the finalizer comes from the AggSpec's function name.
- Recorded samples (nereids suite, `ROWS 3 PRECEDING AND 1 FOLLOWING`, stddev_pop): `2`, `1.5`, `2.0548046676563256`,
  `15049.645234606029`…; variance_samp of 2 rows `{1989,1991}` → `2`; `{1991,32767}`-style frames give values like
  `1073697964` (integral doubles print without a decimal point).

## percentile_approx (`aggregate_function_percentile.h:52-245`, `util/tdigest.h`)

Implementation: Derrick Burns' "merging t-digest" port, **all centroid math in f32** (`Value = Weight = float`),
some intermediates in f64. Not the ClickHouse or Dunning-Java digest.

State: `init_flag: bool, target_quantile: f64 (init -1.0), compressions: f32 (init 10000), digest`.
- add (2 or 3 arguments; only DOUBLE first argument — the FE casts): on the first row `init(quantile, compression)`:
  compression is `(float)arg2` (default 10000 for the 2-argument form); **outside [2048, 10000] it becomes 10000**;
  `check_quantile`: quantile outside [0, 1] → `INVALID_ARGUMENT "quantile in func percentile should in [0, 1], but real
  data is:" + std::to_string(q)` (6 decimals). Quantile and compression are read from row 0 of their (constant) columns.
  Then `digest.add((float)x)` — **the value is rounded to f32 first**; a NaN value is silently dropped.
- result: `init_flag ? digest.quantile((float)target_quantile) : NaN`, an f32 widened to f64. **A NaN result is written
  as 0.0 (non-NULL)** (`insert_default`), so e.g. all-NaN input gives `0`; no non-NULL row → NULL via the variadic
  NULL wrapper.
- merge (second phase): if rhs not init → nothing; if self not init → **a fresh digest with self's `compressions`, which
  is still the default 10000** (not the first phase's 4096), then `digest.merge(rhs)`; `target_quantile` copied from rhs
  if self's is -1.
- reset (windows): `init_flag = false`, `target_quantile = -1`, fresh digest; the next add re-inits.

### The digest
`_min` starts at `FLT_MAX`, `_max` at `FLT_MIN` (= 1.1754944e-38, the smallest *positive* normal — so with all-negative
data `_max` stays 1.1754944e-38; only the q→1 tail branch reads it). Buffers: `_max_processed = 2*ceil(δ)`,
`_max_unprocessed = 8*ceil(δ)`.
- `add(x, 1)`: push `(x, 1)` to `unprocessed`, `unprocessed_weight += 1`; `process()` only if dirty
  (`processed.len() > 2ceil(δ)` or `unprocessed.len() > 8ceil(δ)`).
- `process()`: pdqsort unprocessed by mean; append processed; `inplace_merge` (stable) the two sorted runs;
  `W = processed_weight += unprocessed_weight`; start with the first centroid, `wSoFar = its weight`,
  `wLimit = W * integratedQ(1.0)`; for each next centroid c: if `wSoFar + c.w <= wLimit` → merge c into the last
  (`w += c.w; mean += c.w * (c.mean - mean) / w`, f32) and `wSoFar += c.w`; else
  `k1 = integratedLocation(wSoFar / W); wLimit = W * integratedQ((float)(k1 + 1.0)); wSoFar += c.w`; push c.
  Then `_min = min(_min, first.mean)`, `_max = max(_max, last.mean)`, `updateCumulative()`.
  `integratedQ(k) = (float)((sin(min(k, δ) as f64 * π / δ - π/2) + 1) / 2)`;
  `integratedLocation(q) = (float)(δ * (asin(2.0*q - 1.0) + π/2) / π)` (f64 inside, libm sin/asin).
- `updateCumulative`: `c[i] = prev + (float)(w_i / 2.0)`, `prev += w_i`; then push `prev` (n+1 entries).
- `merge(other)` = `add([other])`: `mergeProcessed` (k-way merge of the processed lists through a `priority_queue`
  keyed on mean — tie order is the standard library's heap order), `mergeUnprocessed` (append other's unprocessed list),
  `processIfNecessary`, `updateCumulative`.
- `quantile(q)`: `process()` if anything is unprocessed or dirty (so **reading a result mutates the digest** — in a
  cumulative window C++ processes after every row), then with n centroids:
  - n == 0 → NaN; n == 1 → `mean(0)`;
  - `index = q * W` (f32); if `(index as f64) <= w0 / 2.0` → `(float)(_min + 2.0 * index / w0 * (mean0 - _min))` (f64);
  - `i = lower_bound(c, index)`; if `1 <= i <= n-1`: `z1 = index - c[i-1]; z2 = c[i] - index` (f32),
    `weightedAverage(mean(i-1), z2, mean(i), z1)`;
  - else (index in the last half-centroid): `z1 = (float)(index - W - w_last/2.0)`, `z2 = (float)(w_last/2 - z1)`,
    `weightedAverage(mean(n-1), z1, _max, z2)` (upstream's formula, negative z1 and all).
  - `weightedAverage(x1,w1,x2,w2)` = if `x1 <= x2` sorted(x1,w1,x2,w2) else sorted(x2,w2,x1,w1);
    `sorted: x = (x1*w1 + x2*w2) / (w1 + w2)` (f32), return `max(x1, min(x, x2))` (C++ std::min/max argument order).

### When it is exactly reproducible, and the simple form doris-rs can use
Every added point has weight 1. A merge of two points needs `wSoFar + 1 <= W·integratedQ(k1+1)`; the right side exceeds
`wSoFar` by only O(W/δ), so small inputs never merge. `S/b6/sim/tdthr.py` (a port of `process()`) finds **the first n at which any two points
merge: 2608 for δ=2048, 5214 for δ=4096, 12726 for δ=10000** (≈ 4δ/π). Below that, and as long as no buffer goes
dirty (needs > 2δ processed or > 8δ unprocessed — larger still), every centroid is one input value, whatever the row
order, tablet split, phase split, merge order, or how often a window read forced `process()`. The result is then:

```
v = sorted f32 values (NaN dropped); n = v.len(); W = n as f32
n == 0 -> NaN (-> 0.0 if any non-NULL row was seen, else NULL); n == 1 -> v[0]
index = (q as f32) * W                                   // f32
if index as f64 <= 0.5        -> v[0]   (the _min formula with _min == v[0])
c[i] = i + 0.5 (i < n), c[n] = n; i = first i with c[i] >= index
if 1 <= i <= n-1 -> weightedAverage(v[i-1], c[i]-index, v[i], index-c[i-1])     // f32
else             -> weightedAverage(v[n-1], z1, max(v[n-1], FLT_MIN), z2), z1 = (index as f64 - n - 0.5) as f32, z2 = 0.5 - z1
result as f64
```
For q = 0.5: odd n → the middle value; even n → `(a*0.5 + b*0.5)/1.0` in f32. The simulator uses exactly this and
reproduces all 18 recorded percentile_approx windows (values `1989`, `1990`, `1123`, `1987`, … — f32-exact). Note the
second phase uses δ = 10000 (fresh state), so for two-phase use the binding limit is the first phase's δ.

**Recommendation:** implement the state as a `Vec<f32>` plus (quantile, compression, init); compute the formula
above; return `Unsupported` when the total count reaches `floor(4·δ/π) - 1` for the smallest δ involved (or, simpler and
safe, when n ≥ 2048). A full port (beyond that size) is possible in principle — everything is deterministic — but
needs: the exact process() points (per-add dirty checks, every window read, every merge), glibc's `sin`/`asin`
(Rust's `f64::sin` differs from glibc in rare last bits; see `2026-11-11-m6-s7b5-math.md` for the glibc oracle), the
libc++/libstdc++ `priority_queue` tie order in `mergeProcessed`, and the FMA question below. Not needed by any suite.

Serialized (C++, informational): `init_flag` u8; if set, `target_quantile` f64, `compressions` f32, then the digest as a
length-prefixed string (u32 total, f32 δ/min/max, u64 max_processed/max_unprocessed, f32 weights, u32-counted
processed `(f32 mean, f32 weight)`, unprocessed, cumulative f32s). doris-rs may use its own tag (raw f32 list).

## percent_rank / cume_dist (`aggregate_function_window.h:203-334`)

Both are in `PARTITION_FUNCTION_SET` (whole partition buffered before output). The FE sends
`RANGE UNBOUNDED PRECEDING..CURRENT ROW` (`W0 end=B0`); the executor calls the function once per **peer group** with
frame = the peer group `[gs, ge)` (no reset inside a partition).
- percent_rank: `rank = 1 + (gs - ps)`, `n = pe - ps`; result `if n <= 1 {0.0} else ((rank - 1) as f64 * 1.0) / (n - 1) as f64`
  = `(gs - ps) as f64 / (n - 1) as f64`. Not nullable.
- cume_dist: `(ge - ps) as f64 * 1.0 / n as f64` = `(ge - ps) as f64 / n as f64`. Not nullable.
- Recorded (`PARTITION BY four ORDER BY ten`, `unique2 < 10`): percent_rank `0 0 1 | 0 0 0.6666666666666666 1 | 0 | 0 1`;
  cume_dist `0.6666666666666666 0.6666666666666666 1 | 0.5 0.5 0.75 1 | 1 | 0.5 1`.
- doris-rs: new `WindowKind`s; `groups` in `window_columns` already has `(gs, ge, idx)`. `check_frame` should require
  `RangeToCurrent` (what the FE forces); with no ORDER BY every row is one peer group (→ percent_rank 0, cume_dist 1).

## multi_distinct_sum / multi_distinct_group_concat (`aggregate_function_distinct.{h,cpp}`)

`multi_distinct_X` = the Distinct combinator around the plain `X` (nested `sum`, `group_concat`; created with
`result_is_nullable=false`). `multi_distinct_count` is a separate function (`AggregateFunctionUniq`, a set; result = its
size; NotNullable: NULL rows skipped, empty → 0; insert does not clear).
- State: a hash set of the argument (numeric `flat_hash_set<T>` for TINYINT…LARGEINT, else `flat_hash_set<StringRef>`
  of the bytes; several arguments → serialized tuples) **plus the nested function's state**.
- add: insert into the set. merge: set union. Serialized (C++): var-uint count then values.
- `insert_result_into`: feed the set's elements, **in hash-set iteration order**, into the nested function
  (`add_batch_single_place`), write the nested result, then **clear the set but not the nested state**.
- NULLs: nullable argument → the unary NULL wrapper (NULL rows skipped, nothing accepted → NULL). A non-nullable
  argument with the FE's nullable result (`multi_distinct_sum(INT#6 not null) -> BIGINT?`) → no wrapper; in a window
  an empty frame gives NULL through `use_null_result`.
- Results recorded: whole partition `sum(distinct 2,2,2,2) = 2`, `group_concat(distinct 'a',…) = a`;
  `ROWS 1 PRECEDING AND 1 PRECEDING` over `(1,10,'a'),(2,20,'b')`: row 1 → `\N \N 0` (sum, multi_distinct_sum, count),
  row 2 → `10 10 1`; group_concat row 1 `\N \N`, row 2 `a a`.
- **C++ quirk (window, not exercised):** because only the set is cleared on output, in frames C++ does not reset
  (ROWS with no start, RANGE to current row with an ORDER BY) each output adds the *newly added rows'* distinct values
  to the nested state again: result = Σ over steps of distinct(values added in that step) — a value that reappears in a
  later step is summed / concatenated again. Whole-partition and sliding frames are unaffected (one feed per state, or a
  reset per row). doris-rs's accumulator with a true set gives the mathematically distinct answer, so it would differ
  from C++ there: refuse multi_distinct_sum/group_concat over cumulative frames (or port the quirk).
- **Cannot match in general:** `multi_distinct_group_concat` with ≥ 2 distinct values in one feed emits them in phmap
  `flat_hash_set<StringRef, StringRefHash>` iteration order (depends on the hash, capacity growth and slot layout) —
  not reproducible without porting phmap + `StringRefHash`. Every recorded case has one distinct value. Likewise a
  DOUBLE `multi_distinct_sum` would add in hash order (integer sums are order-free). Refuse ≥ 2 distinct strings (or
  DOUBLE arguments) rather than guess.
- multi_distinct_count over a ROWS frame (needed by `qt_sum_empty_frame`): count of distinct non-NULL values in the
  frame; empty → `0`. `analytic.rs:224` must allow sliding frames (state reset per row as for any aggregate).

## any_value

Unchanged from `2026-11-10-m6-s7b3-aggregates.md` §any_value: alias of `any`; unary NULL wrapper; keeps the first
value added; merge keeps self's value if set, else rhs's. Two-phase grouped beside `count()` (first phase streaming).
Both recorded uses end in the assert error, so first-seen order is unobservable here.

## CROSS_JOIN mark join (blocker of subquery_unnesting; join research, summarized)

`nested_loop_join_probe_operator.cpp`: for `CROSS_JOIN` with `is_mark_join`, every probe×build pair is emitted and the
mark column is filled with 1 (`_update_additional_flags`, `:79-88`; there are no conjuncts here, `markconj=0`); when the
build side is empty, each probe row is emitted once with default build columns and mark 0
(`_append_left_data_with_null`, `:302-331`). Verify against the 7 recorded nodes before relying on this summary.

## Risks / cannot match

1. **FMA.** The reference BE is aarch64 (b5 math doc), built with clang ≥ 16 whose default `-ffp-contract=on` may fuse
   `m2 += (count*delta)*r` into `fma(count*delta, r, m2)` and tdigest's `x1*w1 + x2*w2`. Every recorded value in this
   batch is reproduced both with and without fusing (`stddev.py` vs `stddev_fma.py`; tdigest inputs are small integers,
   so its products are exact). Keep the unfused form (consistent with the existing `stddev`, which passes its suites);
   if a later suite disagrees in the last bit, try `mul_add` there.
2. multi_distinct_group_concat order (hash-set order) and DOUBLE multi_distinct_sum: not matchable beyond one distinct
   value; refuse.
3. percentile_approx beyond ~4δ/π values (≥ 2608 at the minimum δ): needs the full digest port (glibc sin/asin,
   heap tie order); refuse.
4. multi_distinct_sum/group_concat over cumulative windows: C++'s re-count quirk; refuse or port.
5. test_window_fn also needs the ANALYTIC-over-AGGREGATION-in-5-instances fix; subquery_unnesting the CROSS mark join
   and the stats-table partial update — none are aggregate work.
