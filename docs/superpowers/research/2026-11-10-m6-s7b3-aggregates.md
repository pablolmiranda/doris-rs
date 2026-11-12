# Batch 3 research: aggregates (percentile, percentile_array, element_at, retention, linear_histogram, exponential_moving_average, any_value, group_bit_and/or/xor)

C++ sources (`be/src/exprs/aggregate/` unless noted): `aggregate_function_percentile.{h,cpp}`,
`../../util/percentile_util.h` (`check_quantile`, `Counts`, `PercentileLevels`), `aggregate_function_retention.{h,cpp}`,
`aggregate_function_linear_histogram.{h,cpp}`, `aggregate_function_ema.{h,cpp}`, `aggregate_function_min_max.{h,cpp}`
(any_value), `aggregate_function_bit.{h,cpp}`, `helpers.h` (NULL-wrapper choice), `aggregate_function_simple_factory.h`
(name rewrite), `exprs/vectorized_agg_fn.cpp:233-247` (attr), `exprs/function/array/function_array_element.h` (element_at).

Expected results: `oracle/epic1/replies/<suite>.jsonl.gz` (cells hex-encoded; the `.out` files are stale, e.g.
percentile's `.out` says `101.0`, the reference sends `101`). Scratch (none in the repo), `S=scratchpad`:
- agg-node dumps (`dump.sh`): `S/b3/agg/{pct,pcta,nocast,ret,lh,ema,sub,bit}.txt`
- whole-fragment dump tool `S/b3/agg/pdump` (binary `S/target/release/b3pdump FILE fn1,fn2`: every node, conjuncts,
  projections, sort asc/nulls-first, NLJ params, query option `new_version_percentile` + batch size): `S/b3/agg/nocast_plan.txt`
- partial-update finder `S/target/release/b3partial FILE`, join-op census `S/target/release/nlj FILE` (src `S/b3/agg/partial`)
- reply printer `S/b3/agg/tools/rep.py REPLIES.jsonl.gz [substr...]`
- float sims: `S/b3/agg/sim/pct.py` (percentile, every recorded value reproduced), `sim/ema.py` (EMA, all reproduced;
  shuffle test), `sim/c/fma.c`.

## Common facts

- The reference build is x86-64 `-msse4.2 -mavx2`, no `-mfma` (`be/CMakeLists.txt:399-407`): no fused multiply-add;
  plain left-to-right f64 ops reproduce every recorded value.
- doris-rs partial states are its own encoding (as for moments): new `State` variants; C++ layouts below are informational.
- NULL wrappers (`helpers.h:104-135`): `NullableAggregateFunction` → a row with any NULL argument is skipped; no accepted
  row → NULL. `NotNullableAggregateFunction` (percentile_array, linear_histogram) → NULL rows skipped too, but the result
  is never NULL: an empty state finalizes to its own empty value (`[]`, the empty histogram JSON). doris-rs already has
  this split (`Moments { accepted }`).
- Partial-state slots: `VARCHAR(65533)?` for the nullable family, `VARCHAR(65533)` (not null) for percentile_array and
  linear_histogram.
- None of these use `agg_sort_infos`; no DISTINCT shapes. Window use: percentile (nocast) and EMA (one query each).
- Hidden type needs: `ColType::supported_array` (`value.rs:95`) admits only int/string elements. percentile_array needs
  `ARRAY<DOUBLE>` slots (element nullable), retention `ARRAY<BOOLEAN>`. Array text (`row_text.rs:112`) already prints
  doubles as top-level doubles (`[1e+20, 1e+20]`, `[61, 66.30000000000001, 93.33]`) and bools as `1`/`0` — matches.
- `AggSpec::from_expr` takes `ret` from `from_type_desc`, which handles `ARRAY<scalar>`: fine for both array returns.
- `analytic.rs:155-169` refuses newer aggregates over a window by name; percentile and EMA must be allowed there
  (frame `Rows{start:None,end:Some(0)}` is already supported; the accumulator is fed in row order, which is what C++ does).

## percentile / percentile_array (IMPORTANT: the v2 classes run)

Every fragment in the three suites carries `query_options.new_version_percentile = true` (b3pdump `nvp=Some(true)`), so
`AggregateFunctionSimpleFactory::get` (`aggregate_function_simple_factory.h:140-146`) rewrites `percentile` →
`percentile_v2` (`AggregateFunctionPercentileV2`, `aggregate_function_percentile.h:753`) and `percentile_array` →
`percentile_array_v2` (`:826`), state `PercentileExactState` (`:434-619`), not the `Counts` state (`PercentileState`,
`:325`). Both give the same numbers here, but port v2.

FE signatures (all recorded):
- `percentile(T, DOUBLE) -> DOUBLE?`, T ∈ TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE (no cast; arg nullable),
  level a `FLOAT_LITERAL:DOUBLE` (0.5; window: 1, 0.9999, 0, 0.0001). Two-phase scalar (merge from `VARCHAR(65533)?`).
- `percentile_array(T, ARRAY<DOUBLE>) -> ARRAY<DOUBLE>` (not null; element nullable); levels an `ARRAY_LITERAL` of
  `FLOAT_LITERAL:DOUBLE` ([0.5,0.8] / [0.5,0.55,0.805]). Two-phase scalar, and one-phase `group by id` (nocast, T up to
  LARGEINT and DOUBLE).
- nocast `group by id` query: the FE rewrites the three `percentile(level, q)` into a second, identical
  `percentile_array(level, [0.5,0.55,0.805])` and projects `element_at(arr, 1|2|3)` (`FUNCTION_CALL
  element_at(ARRAY<DOUBLE>, INT) -> DOUBLE?`, `INT_LITERAL` index) on the agg node's projections. So `element_at` is only
  ever applied to a percentile_array result there.
- nocast window: `ANALYTIC` with `percentile(TINYINT, DOUBLE)` ×4 over `nullable(ARG0)`, window ROWS UNBOUNDED PRECEDING
  → CURRENT ROW, ORDER BY ARG0 (the child sort is `asc=false nulls_first=false`: the FE reversed the SQL's
  `CURRENT ROW AND UNBOUNDED FOLLOWING`), no partition, behind a CROSS_JOIN (non-mark) and a group-by-only agg.

Algorithm (`PercentileExactState`, bit-exact):
- add: first row sets levels (`check_quantile` each: `q < 0 || q > 1` → INVALID_ARGUMENT
  `"quantile in func percentile should in [0, 1], but real data is:" + std::to_string(q)` (`%f`, e.g. `1.500000`);
  a NULL level → `"quantiles in func percentile_array should not have null"`), then appends the raw value
  (`:439-457`, `percentile_util.h:37-43`). Values kept in their own type (i8…i128, f32, f64).
- merge: levels from rhs if unset; append rhs values (`:491-504`). Order-free: results are pure order statistics.
- single level (`_get_result`, `:595-614`) / array (`insert_result_into`, `:520-567`, levels visited in ascending
  order via a permutation, results written back at their own index):
  `n = len; if n == 1 → v[0] as f64; u = (n-1) as f64 * q; i = u as usize (trunc);`
  `if i + 1 >= n → max as f64; else s = sorted; s[i] as f64 + (u - i as f64) * (s[i+1] as f64 - s[i] as f64)`.
  (`nth_element` + `min_element` of the rest = sorted[i], sorted[i+1].) Conversions: f32→f64 exact; i128→f64
  round-to-nearest-even (Rust `as` matches).
- empty: percentile → `get()` 0.0 under the NULL wrapper → NULL; percentile_array → `[]` (non-NULL).
- Verified (`sim/pct.py`): `[101, 101.6]`, `[6.28000020980835, 8.164000129699707]` (f32 3.14/6.28/9.42),
  `[2.718281828, 2.9722683229999998]`, `[1e+20, 1e+20]`, group id=2 `61 | 66.30000000000001 | 93.33`, double table
  `[19.950000000000003, 24.725, 101.9175]`, and all 28 window cells (e.g. `126.93099999999995`, `-127.9238`).
- Window: C++ re-finalizes after each added row; sorting a copy per row in doris-rs's `finish(&self)` is fine.

## element_at (`function_array_element.h:293-330`, `_execute_number`)

`use_default_implementation_for_nulls = false`; result always Nullable(element). 1-based: `1 <= i <= len` → element
`i-1`; `-len <= i <= -1` → element `len+i`; `i == 0`, out of range, NULL array, or NULL element → NULL. In the suite the
index is a constant 1..3 and the array always has 3 non-NULL elements.

## retention (`aggregate_function_retention.h:52-176`)

FE: `retention(BOOLEAN ×32) -> ARRAY<BOOLEAN>?` (element nullable), args `BINARY_PRED eq(INT,INT)` over a nullable INT
(`uid = 1` … `uid = 32`), one-phase `group by uid` (1 bucket), then sort + exchange. The 33-argument query fails in the
FE ("The retention function can accept at most 32 params, but got 33") — no BE traffic. (The BE also throws at
construction, `:122-126`, `"retention function can accept at most {} params, but got {}"`, never reached.)
- `NullableAggregateFunction`, varargs → variadic NULL wrapper: a row with any NULL condition is skipped; no row → NULL.
- state `events[32]` u8; add: `events[i] = 1` if arg i true; merge: OR. Order-free.
- result: `out[0] = events[0]; out[i] = events[0] && events[i]` for i ≥ 1, length = argument count.
- Recorded: uid 1 → `[1, 0, …, 0]` (32), uid 2 → all 0 (its `uid = 2` event is masked by event 0).

## linear_histogram (`aggregate_function_linear_histogram.{h,cpp}`)

FE: `linear_histogram(BOOLEAN, DOUBLE) -> VARCHAR(65533)` and `(BOOLEAN, DOUBLE, DOUBLE)` (not null), interval/offset
`FLOAT_LITERAL:DOUBLE`. Two-phase scalar; two-phase `group by id % 2` (streaming preagg, BIGINT key). C++ accepts
BOOLEAN…DOUBLE and decimals (`.cpp:38-41`); only BOOLEAN (nullable and NOT NULL) is sent.
- `NotNullableAggregateFunction`: NULL rows skipped, empty → `{"num_buckets":0,"buckets":[]}` (recorded for all-NULL
  and empty input).
- add (`.h:204-234`): per row: `interval <= 0` → INVALID_ARGUMENT
  `"Invalid interval {}, row_num {}, interval should be larger than 0"`; with offset, `offset < 0 || offset >= interval`
  → `"Invalid offset {}, row_num {}, offset should be in [0, interval)"`. `{}` of a double is fmt shortest (`0`, `1`,
  `0.5` — doris-rs `double_text`); row_num = the row's index in its input block (always 0 here: the check fails on the
  first row). Recorded: `(<be-host>)[INVALID_ARGUMENT]Invalid interval 0, row_num 0, interval should be larger than 0`
  and `…Invalid offset 1, row_num 0, offset should be in [0, interval)`.
  Then `val = value as f64` (bool 0/1), `key = floor((val - offset) / interval)`; `key <= i32::MIN || key >= i32::MAX`
  → INVALID_ARGUMENT `"{} exceeds the bucket range limit"` (val); `buckets[key as i32] += 1` (`.h:69-84`).
- merge: skip if `rhs.interval == 0` (empty); take rhs interval/offset; add counts (`.h:87-98`).
- output (`.h:131-177`): keys sorted ascending; `num_buckets = max_key - min_key + 1` (u32; 0 if empty); every key from
  min to max is emitted, missing ones with count 0; `left = min_key as f64 * interval + offset`, then per bucket
  `lower = left; left += interval; upper = left` (accumulated, not recomputed); `acc_count` running sum.
  JSON via rapidjson `Writer` (compact, no spaces), key order `num_buckets`, `buckets`; each bucket `lower`, `upper`,
  `count`, `acc_count`. Example: `{"num_buckets":2,"buckets":[{"lower":-0.5,"upper":0.5,"count":1,"acc_count":1},{"lower":0.5,"upper":1.5,"count":2,"acc_count":3}]}`.
- Doubles in the JSON are rapidjson's `internal::dtoa` (Grisu2 + `Prettify`, maxDecimalPlaces 324), NOT the result
  `double_text`: always a fraction or exponent: `0.0`, `1.0`, `-0.5`, `-0.0` for negative zero; with `k` = decimal exponent
  of the digit string and `kk = len + k`: `0 <= k && kk <= 21` → digits, zeros, `.0` (`1e20` → `100000000000000000000.0`);
  `0 < kk <= 21` → `12.34`; `-6 < kk <= 0` → `0.001234`; else exponent `d.ddde±X` with no `+` and no padding (`1e22`,
  `1.5e-7`, `1e-7`). (Recalled from rapidjson `internal/dtoa.h`; the source is not in `.cache`. Grisu2 is not always the
  shortest repr — Ryu/Rust may differ on rare doubles; the suite only has k/2 values.) NaN/Inf: the writer refuses them
  (not reachable: interval > 0).

## exponential_moving_average (`aggregate_function_ema.h:60-171`; ClickHouse port)

FE: `exponential_moving_average(DOUBLE, DOUBLE, DOUBLE) -> DOUBLE?` = (half_decay FLOAT_LITERAL 5/1/0/2, value, time),
value/time nullable DOUBLE slots. Shapes: two-phase scalar ×6, one-phase `group by grp`, and ANALYTIC (ROWS UNBOUNDED
PRECEDING → CURRENT ROW, ORDER BY t asc nulls first, no partition).
- `NullableAggregateFunction` (variadic wrapper): row skipped if any arg NULL; no row → NULL (`empty` → NULL;
  `null` test 11.25 = rows (10,1),(20,3)).
- state `value=0, time=0, half_decay=0` (a fresh state is a real point at time 0!).
  `scale(dt, hd) = exp2(-dt / hd)`; `sum_weights(hd) = 1 / (1 - exp2(-1 / hd))`.
  add(v, t, hd): `half_decay = hd`; merge_point(v, t):
  `if time > t {value = value + v*scale(time - t)} else if time < t {value = v + value*scale(t - time); time = t} else {value = value + v}`.
  merge(rhs): `hd = if half_decay != 0 {half_decay} else {rhs.half_decay}; if hd == 0 return; half_decay = hd;` merge_point(rhs).
  get: `if half_decay == 0 {0.0} else value / sum_weights(half_decay)`.
- Quirks to keep: hd=0 → `0` (non-NULL; recorded `0`); merging into a fresh state with `rhs.time < 0` (or adding a
  negative time first) mixes in the phantom (0,0) point — no negative times in the suite. The wrapper never merges an
  empty partial (flag false).
- ORDER-SENSITIVE. `sim/ema.py`: the basic query (20 rows, hd 5) gives 5 different doubles over shuffles; only
  ascending-t order gives the recorded `92.25779635374204` (reversed → …205). The reference feeds rows in tablet storage
  order: `ema_test` is 1 bucket, DUP KEY(id), id order == t order. doris-rs must scan in stored key order and, in the
  two-phase plan, add the single tablet's rows in one state (only one instance has rows). Other recorded values:
  47.5, grp 1 → 11.25 / grp 2 → 50, 0, 2.196699141100893 (same both orders), 11.25, NULL, 15.46875 (duplicate times
  summed), window 5 / 7.5 / 8.75.
- `exp2`: C++ `std::exp2` (glibc on the reference). Rust `f64::exp2` calls the platform libm; macOS libm reproduced every
  recorded value, but glibc vs other libms is a (small) risk for non-integer exponents.

## any_value (`aggregate_function_min_max.cpp:40-41`, `AggregateFunctionAnyData`, `.h:727-743`)

`any_value` is an alias of `any`. FE: `any_value(BIGINT) -> BIGINT?`, two-phase grouped (streaming first phase) beside
`count()`: it is the FE's scalar-subquery decorrelation (`any_value(c)` + `assert_true(count <= 1 OR count IS NULL,
'correlate scalar subquery must return only 1 row')`). Semantics: unary NULL wrapper (NULL rows skipped, none → NULL);
keeps the FIRST value added (`change_first_time`, `.h:193-203`; `add_batch_single_place` takes row 0, `:782-790`);
merge keeps self if it has a value, else takes rhs. So it is instance/arrival-order dependent in general — but in this
suite both plans that use it (`same_conj_*`, `side_phys_*`) end in the assert error
`(<be-host>)[INVALID_ARGUMENT]correlate scalar subquery must return only 1 row`, and any successful use has ≤1 row per
group. Not order-sensitive here.

## group_bit_and / group_bit_or / group_bit_xor (`aggregate_function_bit.h:43-150`)

FE: `group_bit_X(T) -> T?`, T ∈ TINYINT, SMALLINT, INT, BIGINT, LARGEINT (no casts), two-phase scalar and one-phase
`group by flag` (VARCHAR(10)?). `NullableAggregateFunction`, unary: NULL rows skipped, no row → NULL.
State in T: and starts at -1 (all ones), or/xor at 0; add/merge `&=`, `|=`, `^=` in two's complement of T. Recorded:
and 4 / or 127 / xor 106; by flag a: 12/60/48, b: 5/95/90; with -44: and 4, or -33, xor -110; with a NULL row: and 4,
or 95, xor 70 (all five types equal). LARGEINT prints as an integer string.

## Suites and hidden needs

| suite | shapes | extra needs beyond the functions |
|---|---|---|
| support_type.percentile | 7 scalar two-phase, 3 buckets, 4 instances | none |
| support_type.percentile_ar | 7 scalar two-phase | `ARRAY<DOUBLE>` slots + result |
| test_aggregate_percentile_no_cast | grouped one-phase + element_at ×3; scalar two-phase (double); ANALYTIC percentile over `nullable()` after CROSS_JOIN; one FE-only `explain` | `ARRAY<DOUBLE>`; element_at; percentile allowed in `analytic.rs` |
| test_aggregate_retention_param_limit | grouped one-phase, 32 args; 33-arg error is FE-side | `ARRAY<BOOLEAN>` slots + result |
| test_linear_histogram_boolean | two-phase scalar/grouped; 2 BE errors | rapidjson double text; error text |
| exponential_moving_average | 6 scalar two-phase, grouped one-phase, ANALYTIC | EMA allowed in `analytic.rs`; stored row order |
| nereids_p0.subquery.subquery_unnesting | any_value two-phase grouped (both uses end in the assert error) | **CROSS_JOIN mark join** (7 `NESTED_LOOP` `CROSS_JOIN is_mark=true`, no join/mark conjuncts): the latest live run (`S/s7b2-full2.log`) fails on it before reaching any_value — "doris-rs does not support a CROSS_JOIN_NODE with join op CROSS_JOIN (mark) yet". Also a stats-table partial update (below) and scan-conjunct errors that carry `. tablet=<id>, backend=<be-host>` (already handled?) |
| nereids_p0…test_aggregate_bit | two-phase scalar ×9, grouped one-phase ×3 | two stats-table partial updates (below) |

Partial updates: `truncate table` makes the FE delete the table's column statistics from its internal
`column_statistics` table (table_id 1793677838739) with a partial-update OLAP_TABLE_SINK, columns
`catalog_id, db_id, col_id, tbl_id, id, idx_id, part_id, __DORIS_DELETE_SIGN__` (`b3partial`): once in subquery_unnesting
(`truncate table emi_t3`, txn 2700) and twice in test_aggregate_bit (txns 3615, 3617). `fragment::table_sink`
(`fragment.rs:166`) refuses partial updates and `extract-replays.rs:267` rejects the whole suite ("partial update").
These are FE background writes the suite never reads; the planner must decide whether doris-rs accepts (or no-ops) this
stats DELETE live, and whether the replay extractor may skip it, or these two suites stay out of reach.

Order-dependence summary: EMA (stored order, reproduced by ascending-key scan; no `cpp_instance_order` needed since one
tablet feeds it); any_value (first-seen, but unobservable here); percentile*, retention, linear_histogram, group_bit_*
are order-free.
