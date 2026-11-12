# Doris 4.1.5 BE: ndv, stddev, sum0, group_concat, window_funnel_v2, ntile

Paths are relative to `be/src/` unless noted. AGG = `exprs/aggregate/`. Plans were decoded from
`oracle/epic1/traffic` with a throwaway decoder (TPipelineFragmentParamsList, as `doris-wire`'s
`needs.rs` does). "x?" means `is_nullable = true` on the node.

## 0. Rules shared by all six

**Which function object runs** (`exprs/vectorized_agg_fn.cpp:239`, `AGG/aggregate_function_simple_factory.h:123-160`, `AGG/helpers.h:103-165`):
- `result_is_nullable` is the AGG_EXPR node's `is_nullable`.
- Aliases are resolved first: `ndv` → `approx_count_distinct`, `sum0` → `sum`, `stddev_pop`/`std` → `stddev`, `window_funnel` → `window_funnel_v2`. The last one is done in the FE (`fe/.../catalog/BuiltinAggregateFunctions.java:197`), so plans always say `window_funnel_v2`.
- If **any argument type is nullable**, the function is wrapped in the null combinator:
  - Unary: a NULL row is skipped.
  - Variadic (`aggregate_function_null.h:574-597`): a row is skipped if **any** argument is NULL.
  - With `result_is_nullable`, a state that never accepted a row gives NULL. Otherwise it gives the nested default.
- With no nullable argument there is no wrapper. A scalar (no GROUP BY) aggregate whose node is nullable but whose function is not gets NULL over empty input (`exec/operator/aggregation_source_operator.cpp:430-441`). doris-rs already does this.

**Serialized state.** The second phase reads the first phase's `VARCHAR(65533)` slot. doris-rs runs every fragment itself, so the state encoding is **not observable**. Only the final value and the merge semantics have to match. The C++ layouts are listed below for reference only.

**Phases seen in the corpus.**

| Function | Phases |
|---|---|
| ndv, stddev, window_funnel_v2, sum0 | Two-phase: first `need_finalize=false`, `is_first_phase=true`; then `merge=true`, finalize |
| sum0 in eager_agg | Also `use_streaming_preaggregation=true` in the first phase |
| group_concat | One phase only |
| ntile | ANALYTIC_EVAL_NODE |

- No DISTINCT (`multi_distinct_*`) variant of any of these appears.
- `agg_sort_infos` is always one empty `TSortInfo` per function, which means no ORDER BY inside the aggregate.

## 1. ndv (= approx_count_distinct): HyperLogLog

### What the FE sends

| Suite | Signatures |
|---|---|
| correctness_p0.test_multi_distinct | ndv(INT), (BIGINT), (VARCHAR(10)), (VARCHAR(64)) |
| explain.test_outerjoin_isnull_estimation, stats.partitionRowCount | ndv(INT) |
| join.asof.test_asof_join_explicit | ndv(INT), (DATETIMEV2(18,0)) |
| stats.partition_key_minmax | ndv(INT), (VARCHAR(20)) |
| query_p0 mark_join | ndv(BIGINT), VARCHAR(150/300/500/1080) |
| rqg1333347798 | ndv(INT), (BIGINT) |
| show.test_show_analyze_task_status_cmd | ndv(INT), (VARCHAR(255)) |
| show.test_show_column_stats_command | ndv(INT), (DOUBLE), (STRING) |
| stats.col_stats.hot_value_str_to_date | **no ndv in any captured plan.** Its needs are only eq / scan / union. It sets stats by `ALTER ... SET STATS`. Re-check the needs-index entry. |

- Every ndv comes from the FE's statistics job, `FULL_ANALYZE_TEMPLATE` (`fe/.../statistics/BaseAnalysisTask.java:91-107`): `COUNT(1), NDV(col), COUNT(col), MIN, MAX, SUM(LENGTH(col))` over `(SELECT col[, length assert])`, scalar (no GROUP BY).
- The plans are two-phase. Phase 1 is `ndv(T)->BIGINT`, not nullable, argument `SLOT_REF:T?`. Phase 2 is `merge=true` over a `VARCHAR(65533)` slot, finalized.
- The result is not nullable (FE `Ndv extends NotNullableAggregateFunction`, `resultForEmptyInput` = 0).

### Semantics (`AGG/aggregate_function_approx_count_distinct.{h,cpp}`)

- Accepted types (`.cpp:31-38`): bool, all ints, largeint, float, double, decimalv2/32/64/128I/256, varchar, datev2, datetimev2, ipv4/6, timestamptz. CHAR, STRING and JSONB are read as VARCHAR (`AGG/helpers.h:375-377`).
- NULL rows are skipped by the null wrapper. Empty input gives **0**.
- Hash per row (`.h:98-112`): `murmur_hash64A(bytes, len, seed = 0xadc83b19)` (`util/hash_util.hpp:292-388`; MurmurHash64A, m = `0xc6a4a7935bd1e995`, r = 47, `h = seed ^ (len*m)`, little-endian 8-byte blocks, standard tail).
  - Fixed-width types hash their **in-memory column value, `sizeof(value)` bytes LE**:

    | Type | Bytes hashed |
    |---|---|
    | INT | 4 |
    | BIGINT | 8 |
    | LARGEINT | 16 |
    | BOOLEAN | 1 |
    | FLOAT | 4, raw bits |
    | DOUBLE | 8, raw bits (so 0.0 ≠ -0.0, and NaN payloads differ) |
    | DATEV2 | u32, packed `day:5\|month:4\|year:23` |
    | DATETIMEV2 | u64, packed `us:20\|s:6\|min:6\|h:5\|d:5\|mon:4\|year:18` (`core/value/vdatetime_value.h:256-263`). This is doris-rs's `Datum::Int` for these types (`doris-segment/src/values.rs:55`). |
    | DECIMAL32/64/128I/256 | the native integer of that width |
    | DECIMALV2 | i128 |

  - Strings hash their bytes.
  - A hash equal to 0 is ignored (`.h:51`).
- `HyperLogLog` (`core/value/hll.h`, `core/value/hll.cpp`): precision 14, so 16384 one-byte registers (`hll.h:32-36`).

  **States.** EMPTY → EXPLICIT (a set of up to 160 hashes) → FULL (registers). SPARSE exists only as a serialization form.

  **update** (`hll.cpp:56-75`):
  - EMPTY: insert, becoming EXPLICIT.
  - EXPLICIT with `size < 160`: insert. Duplicates do not grow the set.
  - EXPLICIT with **`size == 160`: any further row, even a duplicate, converts to FULL** and then updates the registers.

  **Register update** (`hll.h:262-271`):
  - `idx = h % 16384`.
  - `h >>= 14; h |= 1<<50`.
  - `rank = ctz(h) + 1`.
  - `reg[idx] = max(reg[idx], rank)`.

  **merge** (`hll.cpp:77-140`):
  - EXPLICIT ∪ EXPLICIT: union the sets, and convert to FULL only if the union is `> 160`.
  - EXPLICIT with FULL: convert, then take the register-wise max.
  - The register state is a pure function of the hash set, independent of order.

  **estimate** (`hll.cpp:315-369`):
  - EMPTY gives 0.
  - EXPLICIT gives the **set size** (exact).
  - FULL, **in f32**:
    1. `alpha = 0.7213f / (1 + 1.079f/16384)`, all f32.
    2. `hm = Σ_{i=0..16383} powf(2, -reg[i])`, summed in f32 in index order.
    3. `hm = 1/hm`.
    4. `est = (double)(alpha * 16384 * 16384 * hm)`. The multiplications are f32, left to right; the result is widened to f64.
  - Then, in f64:
    - If `est <= 40960 && zeros != 0`: `est = 16384 * ln(16384/zeros)` (linear counting).
    - Else if `est < 72000`: `bias = 5.9119e-18·e⁴ − 1.4253e-12·e³ + 1.2940e-7·e² − 5.2921e-3·e + 83.3216`, then `est -= est*bias/100`.
  - Return `(int64)(est + 0.5)`.
- Serialized state (`.h:44-56`; `hll.cpp:155-212`), for reference only. It is a length-prefixed binary string:
  - type byte 0 for EMPTY;
  - `1, n:u8, n×u64 LE` for EXPLICIT;
  - `2, n:u32 LE, n×(idx:u16 LE, val:u8)` for SPARSE, used when ≤ 4096 non-zero registers;
  - `3 + 16384 bytes` for FULL.
  - Nullable arguments add the null-wrapper flag.

### Reproducibility

Let D be the number of distinct non-zero hashes in a group.

- **D < 160:** the C++ BE always answers exactly D. No instance can reach 160, and EXPLICIT merges stay explicit.
- **D > 160:** every path ends in FULL with identical registers, so the estimate is deterministic. doris-rs reproduces it bit for bit by implementing the f32/f64 steps above in the same order. powf of an integer power of two is exact. The `ln` is the only libm call, and a 1-ulp difference could only matter at an exact `.5` boundary, which is negligible.
- **D == 160 (the only hazard):** the answer is 160 (explicit) or the linear-counting estimate (≈160–161).
  - Which one depends on whether some first-phase instance received another row after its own set hit 160.
  - That depends on how C++ split rows across pipeline tasks.
  - doris-rs should still implement the `size == 160` rule per state, which makes it right whenever its instance split matches.

All suites in this batch analyze tables of ≤ ~50 rows, so they always take the exact path. The suites check plans or `SHOW` output that is fed by these counts (`stats=`, row counts), so getting ndv exact matters.

## 2. stddev (= stddev_pop)

### What the FE sends
- FE `Stddev`: one signature, `DOUBLE -> DOUBLE`; nullable (`fe/.../agg/Stddev.java`). `stddev_pop` and `std` reach the BE as `stddev`. `stddev_samp` is a separate FE class and BE function.
- aggregate_stddev_over_range: two-phase scalar `stddev(DOUBLE)`, with `parallel_pipeline_task_num` 1, 48 and 8. The expected output is **`NaN`** three times.
- having_between: one-phase `stddev(DOUBLE)` with a 6-key GROUP BY over `cast(varchar) + cast(int)`. The expected output is `\N` because the cast gives NULL.

### Semantics (`AGG/aggregate_function_stddev.h`, `.cpp:30-104`)
- The input is always converted to double. The state is `{mean: f64, m2: f64, count: i64}`, using Welford.
- **add** (`.h:110-120`), in exactly this order:
  ```
  delta = x - mean
  r = delta / (double)(1 + count)
  mean += r
  m2 += ((double)count * delta) * r
  count += 1
  ```
- **merge** (`.h:99-108`). It returns early if `rhs.count == 0`. **Otherwise it always runs the formula, even into an empty left side**, so do not shortcut to a copy:
  ```
  delta = mean - rhs.mean
  n = (double)(count + rhs.count)
  mean = rhs.mean + (delta * (double)count) / n
  m2 = (rhs.m2 + m2) + (((delta*delta) * (double)rhs.count) * (double)count) / n
  count = (int64)n
  ```
  Into an empty left side with a huge `rhs.mean`, `(delta²)·0` is `inf·0 = NaN`. This is one way the over-range test reaches NaN.
- **Finalize, pop** (`.h:86-92`):
  - `count == 1` gives `0.0`. This holds even if the value is ±inf.
  - Otherwise `sqrt(m2 / count)`, with **inf mapped to NaN** (`.h:68-84`). An all-NULL group never reaches here (it is NULL).
- **Finalize, samp** (`.h:147-163`): count 0 or 1 inserts the column default (0.0, or NULL under the wrapper); otherwise `sqrt(m2/(count-1))`.
- NULL rows are skipped; empty input gives NULL.
- DOUBLE rendering in doris-rs is already `row_text.rs` (`NaN`, `Infinity`, shortest round-trip).

### Risk
- With finite data, the last bits depend on the order of updates and merges: how rows are split across instances and the order partial states arrive in.
- Both suites here are order-proof (the answers are NaN and NULL).
- In general, two-phase stddev over many instances is not reproducible bit for bit. That risk is shared with `avg` and `sum` on DOUBLE.

## 3. sum0

- BE: `register_alias("sum", "sum0")` (`AGG/aggregate_function_sum.cpp:49`). It is exactly `sum`.
- The FE marks it **non-nullable** (`Sum0 extends NotNullableAggregateFunction`), so `result_is_nullable=false`. NULL rows are skipped, and empty or all-NULL input gives the nested default, **0**.
- FE signatures: DOUBLE←DOUBLE/FLOAT, DECIMALV3←DECIMALV3, LARGEINT, BIGINT←BIGINT/INT/SMALLINT/TINYINT (BOOLEAN and NULL go through TINYINT).
- The corpus (eager_agg, bilateral_eager_agg) only has **`sum0(BIGINT) -> BIGINT`**, not nullable. Argument slots are mostly not nullable, one is nullable.
  - Phases: one phase; two phases (merge); and streaming preagg (`use_streaming_preaggregation=true`, first phase). Plain `sum` sits beside it in the same nodes.
- Overflow differs from doris-rs:
  - C++ `sum += value` is `NO_SANITIZE_UNDEFINED`, so int64 **wraps** (`AGG/aggregate_function_sum.h:55-60`).
  - doris-rs `sum` returns an overflow error.
  - Same existing gap as `sum`.
- Floating point: the C++ add uses `#pragma clang fp reassociate(on)` (`sum.h:56-58`). A DOUBLE sum0/sum may be vectorized into a different addition order, so refuse or flag DOUBLE sum0 the same way as `sum`.
- doris-rs change:
  - Add `"sum0"` to `AGGREGATES` and map it to `State::Sum`.
  - `finish` already returns 0 when `ret` is not nullable (`aggregate.rs:537-540`).

## 4. group_concat

### What the FE sends
- issue_7288 (nereids_p0 and query_p0): one phase, `group_concat(VARCHAR(50)) -> VARCHAR(65533)`, nullable, argument `SLOT_REF:VARCHAR(50)?`. GROUP BY `l.k1`. The input is the nullable side of a LEFT JOIN.
- Expected output: `1 \N`. There is no match, so every value is NULL and the result is NULL.

### Semantics (`AGG/aggregate_function_group_concat.{h,cpp}`)
- FE signatures (`fe/.../agg/GroupConcat.java`):
  - `(VARCHAR)` and `(VARCHAR, VARCHAR sep)`, both returning VARCHAR. Other argument types are cast to VARCHAR by the FE.
  - `+ ORDER BY` variants add trailing order arguments plus a non-empty `agg_sort_infos` entry.
  - The result is nullable if any non-order argument is nullable, or under alwaysNullable.
- **The default separator is `","`, with no space** (`.cpp:28`).
- State: `{data, separator, inited}`. On the first accepted row it stores that row's separator; later rows reuse **the stored separator**, so the per-row separator only counts from the first row (`.h:48-65`).
- Rows are concatenated in the order they are added. **merge** appends `sep + rhs.data` (`.h:67-86`).
- There is no ordering guarantee without ORDER BY: the order is arrival order, then partial-merge order.
- NULL handling (variadic wrapper): a row with a NULL value, or a NULL separator, is skipped. All rows NULL gives **NULL**. With no nullable argument, an empty group gives `""`, and a scalar over empty input gives NULL.
- ORDER BY: `transform_to_sort_agg_function` (`exprs/vectorized_agg_fn.cpp:255-257`, `AGG/aggregate_function_sort.h:96-110`) buffers rows and calls `sort_block`, which is not guaranteed stable on ties, before feeding the nested function.
- DISTINCT: rewritten by the FE to `multi_distinct_group_concat`, through the generic `multi_distinct_` combinator (`AGG/aggregate_function_distinct.cpp:85-103`).

## 5. window_funnel_v2

### What the FE sends
FE `WindowFunnelV2` (`fe/.../agg/WindowFunnelV2.java`):
- Arguments are `(BIGINT window, STRING mode, DATETIMEV2|TIMESTAMPTZ ts, BOOLEAN c1..cN)`, with N ≤ 127.
- DATEV2 `ts` is retyped to DATETIMEV2. Returns INT, nullable.

The corpus contains:
- window and mode are always literals: `INT_LITERAL:BIGINT`, and `STRING_LITERAL` with mode `default`, `increase`, `deduplication` or `fixed`. Mode parameter types are VARCHAR(5/7/8/13) or STRING.
- `ts` is DATETIMEV2(18,0) or DATETIMEV2(18,3), nullable.
- Conditions are nullable BOOLEAN expressions (eq/ne/gt/lt/ge on INT, VARCHAR and DATEV2 columns, or bare BOOLEAN slots).
- Two-phase scalar, and one phase with GROUP BY `BIGINT?`.
- Window values: 1, 5, 30, 100, 300, 3600, 10800, 20000, 86400, 864000, 1736123071, 2502512601, 3600000000.

### State and accumulation (`AGG/aggregate_function_window_funnel_v2.h`)
- The state is a list of `(ts: u64 packed DATETIMEV2, idx: u8)`, plus `sorted`, `window`, `mode` and `event_count` (`:81-126`).
- **add** (`:136-169`):
  - window and mode are taken from **this row**: the last row wins. Mode parsing is case-sensitive; an unknown mode is INVALID (`aggregate_function_window_funnel.h:59-71`).
  - Conditions are scanned **from the last to the first**. Each true condition appends `(ts, i+1)`; every event after the first in a row gets bit 0x80 (the "continuation", same-row flag).
  - A row matching nothing stores nothing.
- NULL rows: the variadic wrapper skips a row if **any** argument is NULL, including the timestamp or any one condition.
- **merge** (`:178-203`): append the other list, then stable-sort and in-place merge by `ts` only.
  - Equivalent model: the final list is the **stable sort by ts of the concatenation of all partial lists in merge order**, each in row-insertion order.
  - Equal timestamps keep insertion order. That order decides the result when events of different rows share a ts.
- **finalize** (`:275-297`, `:668-674`): stable sort, then:
  - `event_count==0` or an empty list gives **0** (this comes before the window check).
  - `window < 0` → error "the sliding time window must be a positive integer, but got: …".
  - mode INVALID → error "Invalid window_funnel mode".

**Window test** (`:252-257`): `cur <= base + window seconds`, compared as packed u64.
- `base + window` is `DateV2Value::date_add_interval<SECOND>`: calendar arithmetic that keeps microseconds.
- If the result is outside year 0001–9999, the add fails and leaves `end = base`, so the window becomes 0 (`core/value/vdatetime_value.cpp:2524-2564`).

### Modes
All levels are 0-based internally; the answer is level+1 or 0. "Same row" means that every event in `(prev, cur]` has the continuation flag (`:606-618`).
- **DEFAULT** (`:310-338`): one pass.
  - An event 0 sets `L0 = {first = last = ts, idx}`, overwriting any earlier chain.
  - Event k > 0 advances if `L[k-1]` is set, is **not from the same row**, and `ts <= L[k-1].first + window`. Then `L[k] = {L[k-1].first, ts, i}`. Return N as soon as k = N-1.
  - At the end, return the highest set level + 1 (or 0).
- **INCREASE** (`:357-408`): for each event 0 as a start, scan forward, skipping other event-0s. Advance under the DEFAULT rule plus the strict condition `L[k-1].last < ts`.
  - Answer: the maximum of `curr_level+1` over starts.
  - Pruning: stop once `remaining ≤ best`. An early return gives N.
- **DEDUPLICATION** (`:422-494`): for each event 0 as a start, look only for level `curr+1`. Skip same-row events.
  - Window failure **breaks** the chain.
  - It also breaks if any event of level < target sits in the gap rows between the last advance's row and the current row (`_next_row_start(last+1)` up to `_row_start(i)`).
  - Answer: the maximum of `curr+1`, with the same pruning.
- **FIXED** (`:513-565`): for each c1 event as a start (raw index 1), walk row by row (the first event of each row is that row's highest condition).
  - Stop on window failure, or when `highest < expected`.
  - Also stop when the row does not contain `expected` (a level jump).
  - Rows that match nothing are invisible. This is the documented 4.1 change from v1.
  - Answer: `max_level + 1`, where max_level starts at -1, so 0 when there is no c1.

### Risk
- The result depends on the tie order of equal-ts events from **different rows**, which is input order. doris-rs matches if it adds rows in storage/scan order and merges with the same stable rule.
- In this suite, scalar queries over tables distributed by `HASH(ts)` or `HASH(xwho)` put equal timestamps (or one user) in one tablet, so the C++ order is deterministic and doris-rs's DUP-key tie order (later row first, `doris-segment/src/sort.rs`) is the needed one.
- Refuse TIMESTAMPTZ `ts` unless doris-rs has that type.

## 6. ntile

### What the FE sends
- FE `Ntile` (`fe/.../window/Ntile.java`): `TINYINT|SMALLINT|INT|BIGINT -> BIGINT`, and `LARGEINT -> LARGEINT`. Always not nullable.
- The FE rejects a non-literal or ≤ 0 bucket count with the errors the suite expects. These come from the FE; the BE sees nothing.
- Corpus: `ntile(TINYINT literal 3|5) -> BIGINT`, frame **ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW** (`window_start` absent, end CURRENT_ROW).
  - nereids: partition by k1 / none / k3, order by k2.
  - query_p0: extra order keys (`k2,k3` and so on).

### Semantics (`AGG/aggregate_function_window.h:336-376`, registered `AGG/aggregate_function_window.cpp:90`)
- The state is `{bucket_index, rows}`. `reset` (new partition) sets `rows=0`.
- Each row calls `add_range_single_place(ps, pe, …)` once, through `_get_next_for_unbounded_rows` (`exec/operator/analytic_sink_operator.cpp:231-259`).
- Per row:
  ```
  row_index = rows++
  n = bucket literal                 (columns[0]->get_int(0))
  size = pe - ps
  small = size / n
  big = size % n
  first_small = big * (small + 1)
  bucket = row_index >= first_small
             ? big + 1 + (row_index - first_small) / small
             : row_index / (small + 1) + 1
  ```
  This is standard NTILE: the first `size % n` buckets have one extra row. If `n > size`, row i gets bucket i+1.
- Ties among equal ORDER BY keys follow the sorted input order. In the nereids suite, k2 ties resolve in **reverse load order**. That is the DUP-key memtable order (`load/memtable/memtable.cpp:323-332`), which doris-rs already reproduces, so its stable analytic sort over scan order gives the expected `.out`.

### Hazard: partial partition size
- This frame runs in streaming mode, and `pe` is only `_partition_by_pose.end`: **all rows received so far** while the partition has not ended (`analytic_sink_operator.cpp:534-575`).
- If one partition spans more than one input block (batch_size, 4096 by default), the C++ BE assigns buckets to the earlier rows using a **partial** partition size.
- The suite's partitions are 9 rows, so they are safe.

## 7. Recommendation for doris-rs

**Build (exact):**
1. **ndv / approx_count_distinct.**
   - MurmurHash64A over the native little-endian width of each type, and over raw bytes for strings. Skip hash 0.
   - An EXPLICIT set with the exact `size==160` update rule and the `>160` merge rule, then 16384 registers.
   - The f32/f64 estimate exactly as in §1.
   - Not nullable; 0 over empty input. Unit-test against known C++ outputs: the corpus `fetch_data` replies to the analyze SELECTs can serve as an oracle.
2. **stddev (and cheaply stddev_samp / variance).**
   - Welford add and merge with the exact operation order, and **merge through the formula even into an empty state**.
   - pop: count==1 gives 0; inf maps to NaN.
3. **sum0.** An alias of `State::Sum`, with a non-nullable result of 0.
4. **group_concat.**
   - One or two arguments; first-row separator; default `","`; skip a row with any NULL argument; NULL if no row was accepted.
   - Refuse a non-empty `agg_sort_infos` ordering (ORDER BY) and `multi_distinct_group_concat`. Neither is needed here.
5. **window_funnel_v2.**
   - The packed DATETIMEV2 event list with continuation flags and conditions added in reverse.
   - Merge = stable sort over the concatenation.
   - The four modes ported line for line, the window via calendar seconds (failing past 9999 → window 0), and the C++ error texts.
   - Refuse TIMESTAMPTZ.
6. **ntile.** The formula above.
   - Refuse (with a runtime status naming it) when a partition spans more than one batch-size block of the analytic input, because C++ then uses partial partition sizes.
   - Alternatively, emulate the block boundaries, but that is fragile.

**Wrong-answer risks, highest first:**
- **ndv:** at exactly 160 distinct values, the C++ answer depends on its instance split. Elsewhere it is deterministic once the f32 arithmetic is reproduced.
- **ntile:** large partitions (as above).
- **Floating-point stddev/sum over finite data with several instances:** last-bit differences from merge order, and sum's `fp reassociate`.
- **window_funnel_v2 / group_concat:** results depend on tie and arrival order when equal timestamps (or rows of one group) arrive from different instances.
- **sum0/sum BIGINT:** C++ wraps on overflow; doris-rs errors.

None of the target suites hits these risks.

Outside this batch: the stats queries also need CONCAT, SUBSTRING, CAST(... AS STRING), NOW() and the length assert (`assert_true`) around the aggregate.
