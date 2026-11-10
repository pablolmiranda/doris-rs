# Batch 2 research: dates + bits families

C++ root: `.cache/doris-src/be/src` (this tree uses `exprs/function/` and `core/value/`, not `vec/functions/`).
Plan signatures come from the traffic corpus (`oracle/epic1/traffic`), dumped with a scratch tool
(`b2/ws/crates/doris-wire/examples/dump_fn_exprs.rs` + `b2/condense_dates.py`; full dump in
`b2/dates_bits_exprs.txt`). Notation below: `T?` = node is_nullable, `DTV2(s)` = DATETIMEV2 scale s.

Packed layouts (doris-rs `value.rs:330-337`, C++ `vdatetime_value.h`):
DATEV2 u32 = `y<<9 | m<<5 | d`; DATETIMEV2 u64 = `y<<46 | m<<42 | d<<37 | h<<32 | mi<<26 | s<<20 | us`.

## Cross-cutting finding: constant expressions are evaluated at *open* (prepare)

`VectorizedFnCall::open` (exprs/vectorized_fn_call.cpp:320-331) and `VExpr::open` (exprs/vexpr.cpp:460-469)
call `get_const_col` (vexpr.cpp:824-845) on every subtree whose children are all constant
(`is_constant`, vexpr.cpp:819-822). So an all-literal call that throws fails the **exec_plan_fragment
response**, not fetch_data. In test_years_add all 8 erroring queries (`years_add(cast('9999-12-31' as
datev2), 1) from hits_years_add`, etc.) answer exec_plan_fragment with
`PStatus{status_code: -218, error_msgs: ["(127.0.0.1)[E-218]Operation year_add of 9999-12-31, 1 out of range"]}`
then the FE cancels; no fetch, no reportExecStatus. doris-rs's `submit` (doris-be/src/query.rs:166-230)
has no such step: it needs a prepare-time pass that evaluates every constant FUNCTION_CALL/CAST subtree
(1 row) and returns the coded error. Column-driven overflows (test_date_or_datetime_computation_negative)
come instead on fetch_data + reportExecStatus (twice) + the cancel, like existing Coded errors.
Code/name: `ExecError::Coded{code: -218, name: "E-218", ..}` (ErrorCode::OUT_OF_BOUND; printed `[E-218]`).

## Per function

### year — `year(datev2) -> SMALLINT` (not nullable in corpus)
date_time_transforms.h:81 (`TO_TIME_FUNCTION(ToYearImpl, year)`): the year field. NULL in → NULL out.
Corpus: test_probe_clean `year(ifnull(d,d))` folded by FE to `year(SLOT DATEV2)`.

### years_add / months_add / hours_sub (the add/sub family)
Signatures in corpus: `years_add(datetimev2(0), int) -> DTV2(0)[?]`, `years_add(datetimev2(6), int) -> DTV2(6)?`,
`years_add(datev2, int) -> DATEV2?`; `months_add(datev2, int) -> DATEV2` (not null);
`hours_sub(datetimev2(6), int) -> DTV2(6)` (not null). Return type = arg type
(function_date_or_datetime_computation.h:103-131); interval is INT for YEAR..HOUR, BIGINT for minute and smaller (:110-116).
- `date_time_add` (h:75-101): DAY/WEEK use `date_add_days(delta*7?)`; others `date_add_interval<unit>(TimeInterval(unit, |delta|, delta<0))`.
  Failure → `throw_out_of_bound_date_int(get_time_unit_name(unit), t, delta)` (h:89,95).
- `_sub` = add with `-delta` (h:388-401, hours_sub h:428-432) ⇒ message says **`hour_add`** and the **negated** delta:
  `days_sub('0000-01-01',1)` → `Operation day_add of 0000-01-01, -1 out of range` (seen in corpus).
- Message: `Operation {unit}_add of {date}, {delta} out of range` (datetime_errors.h:57-61; unit names :122-169:
  year_add, quarter_add, month_add, week_add, day_add, hour_add, minute_add, second_add, ...). Week delta is in weeks.
  `{date}` = `DateV2Value::to_string` scale −1 (vdatetime_value.cpp:2261-2324): `YYYY-MM-DD` for DATEV2;
  `YYYY-MM-DD hh:mm:ss` for DATETIMEV2, plus `.ffffff` (6 digits) only when microseconds ≠ 0 — regardless of the column's scale.
- YEAR (vdatetime_value.cpp:2575-2583): year ± n via `set_time_unit<YEAR>` (u32; >9999 or negative-wrapped → fail);
  Feb 29 into a non-leap year → 28. Year 0 is valid; `is_leap` treats year 0 as non-leap (util/time_lut.h:36-38).
- MONTH/QUARTER (cpp:2584-2597): `months = y*12 + m-1 ± n`; <0 → fail; y>9999 → fail; day clamped to month end (Feb 29 in leap years).
- HOUR/MIN/SEC/DAY/WEEK (cpp:2533-2573): seconds arithmetic from day 1 of the month, `calc_daynr` + `get_date_from_daynr`
  (cpp:2404-2443: daynr ≤0 or > DATE_MAX_DAYNR (9999-12-31) → fail); microseconds kept.
- NULLs: DateTimeOp skips null rows (no throw on a null row's nested value) and the result null map is the OR (h:700-735).

### days_diff / weeks_diff — `(datetimev2(6), datetimev2(6)) -> BIGINT?`
h:560-584 + h:625-639: `days_diff(a, b) = datetime_diff<DAY>(b_as_ts1, a_as_ts2)` = **a − b**.
vdatetime_value.h:1666-1669 + :1258-1271: `day = daynr(a) − daynr(b)`; if day>0 and time(a)<time(b) (µs) then day−−;
if day<0 and time(a)>time(b) then day++ (truncate toward zero). weeks = that / 7 (C++ truncation).
`calc_daynr` (h:1745-1772) must be ported exactly: 0000-01-01 → 1, year 0 not leap, `y--` when month ≤ 2 with C
truncating division (y=−1 → 0). Corpus: `days_diff(k0, '0000-01-01 13:00:00')` over 0000-01-01..08 12:00 → 0,0,1..6; weeks all 0.

### weekofyear — `weekofyear(datev2) -> TINYINT?`
date_time_transforms.h:107: `week(mysql_week_mode(3))`. mysql_week_mode (vdatetime_value.cpp:56-62): `m&=7; if !(m&1) m^=4`.
Mode 3 = Monday-first + week_year, ISO. `DateV2Value::week` (cpp:2373-2376) → `calc_week` (cpp:3056-3113; 1950-2029 LUT gives
identical values): year 0 → 0. Algorithm: weekday_first = calc_weekday(daynr(y,1,1), !monday_first) with
`calc_weekday(n, sun) = (n + 5 + sun) % 7` (util/time_lut.cpp:98-100); January days ≤ 7−wf belong to the previous
year when (first_weekday? wf≠0 : wf>3) unless !week_year → 0; days = …; week_year && days ≥ 364 may roll to week 1 of next
year; return days/7+1. Corpus: `weekofyear(CAST(int AS DATEV2)) = 52` and `weekofyear(CAST(varchar AS DATEV2)) = 52`
(casts INT 20000101→YYYYMMDD and VARCHAR→DATEV2, both already handled by doris-rs's text cast).

### yearweek — `yearweek(datev2, int) -> INT?` (mode 6 in corpus)
h:668-669: two-arg form through FunctionDateOrDateTimeComputation (null rows skipped).
`year_week(mysql_week_mode(mode))` (cpp:2379-2398): calc_week(mode|2, disable_lut) then, if week==53 && day≥29 && !(mode&4),
last-day weekday check may bump to (year+1, 1). Returns `year*100 + week`. mode 6 → mysql mode 2.
Corpus: inside `approx_count_distinct(YEARWEEK(col_date, 6))` (AGG ndv(int)) in estimate_string_as_date.

### str_to_date — `str_to_date(varchar(50), varchar(8)) -> DATEV2?` with format `'%Y-%m-%d'`
function_other_types_to_date.cpp:73-243. Return type is **the FE's** (block result type, :145-170): DATEV2 here
(FE picks DATEV2 when the format has no time specifiers, DATETIMEV2(6) otherwise). Default null handling.
Specials rewritten first (:93-107): `yyyyMMdd`, `yyyy-MM-dd`, `yyyy-MM-dd HH:mm:ss`. Parse =
`DateV2Value::from_date_format_str` (vdatetime_value.cpp:1792-2258): skips leading whitespace in value; %Y up to 4 digits (≤2 digits → +1900/2000
pivot 70), %y, %m/%c, %d/%e, %H/%k, %h/%I/%l (12h), %i, %s/%S, %f, %p, %M/%b, %j, %W/%a, %U/%u/%V/%v/%X/%x, %r, %T,
%., %@, %#, %%; literal chars must match; trailing value text ignored; missing month/day default to 1 when only Y/m/d used;
time parts into a DATEV2 result → fail. Any failure → NULL (`_execute_inner_loop`, :236-243). Corpus table is empty
(no INSERT), so only dispatch + types matter for the replay.

### bitand — `ARITHMETIC_EXPR bitand(T, T) -> T`, T ∈ {TINYINT, SMALLINT, INT, BIGINT, LARGEINT}
function_bit.cpp:142-152 (`a & b`), registered for all five (:223-228); default NULL handling. Node type is
**ARITHMETIC_EXPR** (goes through `arith.rs` `arithmetic_supported`, not the FUNCTION_CALL match).
Corpus: `bitand(int?, int?)` (test_outer_join_sort), `bitand(smallint,...)`, `bitand(tinyint,...)`, `bitand(bigint,...)` literals.

### bit_shift_left / bit_shift_right — `(bigint, tinyint) -> BIGINT[?]`
function_bit_shift.cpp:40-42, 127-151: shift count is Int8; `b < 0 || b >= 64` → **0**; otherwise logical shift on u64
(`1<<63` = −9223372036854775808; `INT64_MIN >> 63` = 1; `-1 >> 63` = 1). Default NULL handling. FE casts the value
to BIGINT (CAST TINYINT/SMALLINT/INT → BIGINT?) and the count to TINYINT (`CAST(0 - number AS TINYINT)`).

### bit_test (alias bit_test_all) — `bit_test(T, T, T...) -> TINYINT[?]`, all args same type T
function_bit_test.cpp:44-46 (returns Int8 0/1), :103-122 (2 args), :124-149 (≥3 args), :154 alias.
Arithmetic shift of the signed value (`(v >> pos) & 1`); pos < 0 or ≥ bit width (8/16/32/64/128) → 0; with several
positions the result is 1 only if every bit is set, an invalid position → 0. Any NULL arg → NULL (default handling).
LARGEINT args arrive as LARGE_INT_LITERAL.

### xor — `xor(boolean, boolean) -> BOOLEAN?`, FUNCTION_CALL
functions_logical.h:97-102 (`a ^ b`, `special_implementation_for_nulls() = false`) ⇒ `use_default_implementation_for_nulls`
(functions_logical.h:122-124) ⇒ **any NULL → NULL** (unlike and/or). functions_logical.cpp:182-206 return type, :289 registration.
`1 xor 0` is an FE error; constant xor is FE-folded (enable_fold_constant_by_be=false).

## doris-rs dispatch (crates/doris-exec/src/eval.rs)
- `check_nodes` (eval.rs:170-235) whitelists FUNCTION_CALL names (:196-208) by signature; evaluation in the big match
  (~eval.rs:520-690), e.g. `to_date` (:602-606, `v >> 37`), errors as `ExecError::Coded` (:561, :626).
- ARITHMETIC_EXPR → `arith.rs` (`arithmetic_supported`, arith.rs:58-80; ops add/subtract/multiply/divide/mod only).
- No date arithmetic or calendar code exists in eval.rs; `funnel.rs:51-77` `add_seconds` uses chrono and *keeps* the
  value on overflow (and chrono's year 0 is leap) — do not reuse for add/sub; port calc_daynr/get_date_from_daynr.
- Errors render as `(ip)[name]msg` in doris-be `error_status` (query.rs:58-66).

## Per suite (all 13 extract as replays per b2/replay_status.json)
| suite | BE uses | still missing |
|---|---|---|
| correctness_p0.test_bit_shift_lagency.test_bit_shift | 25 shift calls (literals, slot casts, `-number`) | — |
| correctness_p0.test_bit_shift_nereids | same 25 | — |
| correctness_p0.test_bit_test_function | 33 bit_test incl. 3-/5-arg, LARGEINT, negative/out-of-range pos | — |
| correctness_p0.test_outer_join_sort | `bitand(int?, int?)` (ARITHMETIC_EXPR) | — |
| correctness_p0.test_xor_pred | `xor(slot?, slot?)` | — |
| correctness_p0.test_date_diff | days_diff, weeks_diff on DTV2(6) incl. year 0 | — |
| correctness_p0.test_probe_clean | year(datev2) | — |
| correctness_p0.test_window_function_with_project | str_to_date → DATEV2 (empty table) | — |
| datatype_p0.date.test_weekofyear | weekofyear(cast int/varchar as datev2) | — |
| nereids_p0.legacy.test_clone_legacy_nullable_from_nereids | hours_sub(DTV2(6), 2) inside window_funnel_v2 | — |
| nereids_p0.stats.col_stats.estimate_string_as_date | yearweek(datev2?, 6) in ndv | — |
| query_p0.join.left_anti_with_other | months_add(datev2, 2) in join conjunct | — |
| query_p0.sql_functions.test_years_add | years_add DTV2(0)/DTV2(6)/DATEV2, nullable(...) casts | **prepare-time constant evaluation** with the E-218 error in the exec_plan_fragment reply (8 queries) |

Near misses (same templates, cheap): test_date_or_datetime_computation_negative (OK replay) needs days/weeks/hours/
minutes/seconds/months/years _add/_sub — runtime E-218 errors via fetch_data; test_case_when_date (OK) needs dayofmonth,
days_add, month; test_bit_function (OK) needs bit_count, bitnot, bitor, bitxor; operator_precedence (OK) needs bitor, bitxor.
