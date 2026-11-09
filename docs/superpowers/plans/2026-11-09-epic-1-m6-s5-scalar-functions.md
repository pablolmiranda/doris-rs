# Epic 1 M6 Story 5: Arithmetic and common scalar functions — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** doris-rs evaluates arithmetic (`add`, `subtract`, `multiply`, `divide`, `mod`, `abs`),
the conditionals `if`, `ifnull`, `coalesce`, the string functions `like`, `concat`, `length`,
`round`, and `now()`, `current_date()` and casts of text with a named time zone, as the C++ BE
does. Eligible goes 409 → 487, all passing both gates.

**Architecture:**
- The read-side evaluator (`eval.rs`, a column per expression) gains one function per
  feature. Each new function lives in a new module by family: `arith.rs`, `cond.rs`,
  `strings.rs` (with `like`), `round.rs`. `eval.rs` only dispatches by node type and function
  name, and `check_nodes` accepts exactly the signatures those modules evaluate.
- **Query constants are bound at prepare.** A new `bind.rs` builds a `QueryClock` from the
  fragment's `TQueryGlobals` and `TQueryOptions`. Its `bind` replaces `now()`, `now(n)` and
  `current_date()` with DATE_LITERALs. It also folds a CAST of a string literal to
  DATEV2/DATETIMEV2 in the query's time zone, so a named zone resolves. C++ computes these once
  per query (`fragment_mgr.cpp:525`), so a literal is exact. Every TExpr a fragment evaluates
  (query and load) passes through `bind` before `check`. The 40 evaluator call sites stay as
  they are.
- **Decimal arithmetic uses raw scaled integers**, as C++ does. A multiply product that
  overflows i128 is redone in 256 bits (`ethnum::I256`).

**Tech Stack:** Rust (`doris-exec`, `doris-be`, `doris-fixtures`, `doris-wire`). New crates:
`regex` (the `bytes` API, Unicode on) for `like`, `ethnum` for 256-bit products, and
`chrono-tz` (with `chrono`) for named time zones. Python oracle.

**Spec:** `docs/superpowers/specs/2026-11-08-epic-1-m6-query-breadth-design.md` §2 (row 5), §3,
§5, §6 ("Each story's plan may add the ones its replayed suites also need").

## Research

**Recount** (`checker/derive.py` with the supplement, supported = `scores/epic1-supported.json`
plus the story's features, suites = `epic1/suites.txt`):
- The spec's features make **73** suites eligible (409 → 482).
- The five suites story 4 deferred need `function:now` (3), `function:current_date` (1) and
  `function:cast_datetime_named_time_zone` (1). With those and their supplement entries
  removed: **487**.
- The 78 suites need, besides features already supported: `like` 37, `round` 15,
  `ARITHMETIC_EXPR` 12, `if` 7, `multiply` 6, `concat` 5, `divide` 5, `add` 5, `length` 3,
  `coalesce` 3, `now` 3, `abs` 2, `subtract` 2, `mod` 1, `ifnull` 1, `current_date` 1, and the
  named-zone cast 1.
- **No newly eligible suite needs `CASE_EXPR`, `nullable`, `assert_true` or
  `ASSERT_NUM_ROWS_NODE`.** Every suite that uses them also needs a join. They move to story 6
  (see "Calls" below).

**The signatures in the 78 suites** (decoded from `oracle/epic1/traffic` plans: function,
argument types → result type). These are the signatures `check_nodes` accepts; anything else
is refused at prepare.

| Function | Signatures |
|---|---|
| add, subtract | BIGINT,BIGINT→BIGINT; DOUBLE,DOUBLE→DOUBLE |
| multiply | BIGINT,BIGINT→BIGINT; DOUBLE,DOUBLE→DOUBLE; DECIMAL128I(27,9)×DECIMAL64(16,8)→DECIMAL128I(38,12); DECIMAL128I(22,14)×DECIMAL32(3,0)→DECIMAL128I(25,14) |
| divide | DOUBLE,DOUBLE→DOUBLE; DECIMAL128I(22,14)/DECIMAL32(3,0)→DECIMAL128I(22,14); DECIMAL32(6,5)/DECIMAL128I(20,0)→DECIMAL32(6,5); DECIMAL128I(38,10)/DECIMAL128I(38,2)→DECIMAL128I(38,8) |
| mod | BIGINT,BIGINT→BIGINT |
| abs | INT→BIGINT; BIGINT→LARGEINT |
| if | BOOLEAN,T,T→T for TINYINT, INT, BIGINT, STRING, DECIMAL32(6,5), DECIMAL64(16,2), DECIMAL64(18,3) |
| ifnull, coalesce | DOUBLE; DECIMAL64(16,8); DECIMAL128I(27,9) |
| like | VARCHAR,VARCHAR; STRING,VARCHAR; VARCHAR,VARCHAR,VARCHAR (ESCAPE) |
| concat | mixes of STRING/VARCHAR/CHAR → VARCHAR |
| length | VARCHAR→INT; CHAR→INT |
| round | DOUBLE,INT→DOUBLE; DECIMAL128I(38,3)→(38,2); (33,25)→(33,0); (38,15)→(38,0); (20,0)→(20,0) |

The plan generalizes these by rule, not by listing: integer arithmetic on any integer width,
decimal arithmetic on DECIMAL32/64/128I (never DECIMAL256), and `if`/`ifnull`/`coalesce` on
any supported scalar type. Each rule is C++'s own, cited below.

**Replays.** 32 of the 78 suites extract (`extract-replays --only`). The other 46 read
tables created by a shared load suite ("tablet not created": the `like_*` and window `sql`
suites, `likeOperatorWorks_order`), or are stream loads or fold-constant checks. The 32 cover
every signature above except `round(DOUBLE,INT)` (12 window suites) and
`coalesce(DOUBLE,DOUBLE)` (1 suite). Unit tests pin those two from C++'s code.

The 32 replays: `test_char_default_value`, `test_implicit_cast`,
`test_like_predicate_with_nullable`, `test_no_grouping_window`, `test_order_by_mod`,
`test_if_functions`, `agg_window_project`, `aggregate_groupby_constant`,
`view_authorization`, `show_variables_command`, `test_case_when_to_if`,
`fold_constant_cast`, `projectAggFuncs` (nereids and query), `test_show_columns_command`,
`test_multiply` (nereids and query), `test_round_overflow`, `test_string_function_like`,
`test_string_function_like_pushdown` (nereids and query), `count`, `test_ifnull`,
`test_like_escapes`, `test_like_literal`, `test_like_no_backslash_escapes_mode`,
`test_like_table`, `test_window_float_sliding_frame`, `test_union_decimal_agg`,
`test_view2`, `char_type_shrink_before_projec`, `virtual_slot_ref_basic`.

**C++ semantics** (Doris 4.1.5, `.cache/doris-src/be/src/`; `F/` = `exprs/function/`):
- **Integer add/subtract/multiply** wrap at the result width, with no NULL and no error
  (`F/plus.cpp:31`, `F/multiply.cpp:54-57`, `NO_SANITIZE_UNDEFINED`). The result is not
  nullable.
- **DOUBLE divide:** a zero divisor (0.0 or -0.0) gives NULL, and so x/0 and 0/0 are NULL;
  otherwise IEEE (`F/divide.cpp:348-351`). The result is always nullable.
- **mod** truncates (`%`), so the sign follows the dividend; x mod 0 is NULL
  (`F/modulo.cpp:491-502`). `INT64_MIN mod -1` fails the query with INVALID_ARGUMENT
  `Division of minimal signed number by minus one is an undefined behavior, {a} % {b}. `
  (`F/modulo.cpp:41-53`).
- **Decimal multiply:**
  - the raw product is scaled down by 10^(s_a+s_b−s_r), rounding half away from zero
    (`F/multiply.cpp:428-496`);
  - a product past i128 is redone in Int256;
  - a result past 10^p−1 fails with ARITHMETIC_OVERFLOW_ERRROR `Arithmetic overflow: {a}
    multiply {b} = {r}, result type: {type}` (`:485-494`).
  - With `check_overflow_for_decimal` false, both-column multiplies round differently
    (`:401-422`). The FE always sends it true.
- **Decimal divide:** the quotient is the raw dividend divided by the raw divisor, truncated
  toward zero, with no rescale. The FE has already cast the dividend so that s_r = s_a − s_b
  (`F/divide.cpp:433-440`). A zero divisor gives NULL, and there is no overflow check.
- **abs** widens (INT→BIGINT, BIGINT→LARGEINT) as `~a + 1` in the wide type, so
  `abs(INT64_MIN)` is 9223372036854775808 (`F/math.cpp:217-227`).
- **if:** a NULL condition takes the else branch (`exprs/vcondition_expr.cpp:369-396`).
  **ifnull/coalesce** test only for NULL, so NaN is a value (`vcondition_expr.cpp:495-547`, `:650+`).
- **like** (`F/like.cpp`):
  - Any NULL argument gives NULL. Matching is case-sensitive.
  - The 3rd argument is a one-byte constant ESCAPE; the pattern is rewritten to backslash form
    (`like.h:150-171`).
  - `convert_like_pattern` (`like.cpp:789-843`):
    - an empty pattern is `^\z`;
    - `^` is prepended unless the pattern starts with `%`;
    - `\%`, `\_`, `\\` are literal, and any other `\x` is a literal backslash;
    - `%` → `.*`, and a final `%` ends the regex with no `\z`;
    - `_` → `.` (one UTF-8 character, dot matches newline);
    - 17 bytes `[](){}-*+\|/:^.$?` are escaped;
    - `\z` is appended, and the regex is searched unanchored.
  - Constant patterns take fast paths first (`like.cpp:896-1005`, regexes `:57-61`), with
    `remove_escape_character` (`:845-861`). EQUALS and ENDS_WITH accept a raw `\`, so `a\\_`
    is EQUALS `a\_` (not a wildcard). This divergence is reproduced.
- **concat:** any NULL argument gives NULL. CHAR reads arrive unpadded already.
  **length** counts bytes (`F/function_string.cpp:191-210`).
- **round(DOUBLE,d):**
  - d=0 gives `round(x)`;
  - d>0 gives `round(x*10^d)/10^d`;
  - d<0 gives `round(x/10^-d)*10^-d`;
  - `round` is C's half away from zero, and 10^d is `int_exp10`, which saturates at
    UINT64_MAX past 19 (`F/round.h:251-320`, `:464-490`, `exec/common/int_exp.h:56-65`);
  - examples: round(2.675,2)=2.68, round(1.005,2)=1, round(-0.5,0)=-1,
    round(12345.6789,-2)=12300.
  - **round(DECIMAL,d):** the result scale is the ret_type's. The value is rounded half away
    from zero, in exact integers (`F/round.h:111-147`, `:193-226`).
- **now/current_date** (`F/function_date_or_datetime_computation.h:1014-1150`):
  - the value comes from `TQueryGlobals` once per query: `timestamp_ms` and `nano_seconds`
    when `time_zone` is set, otherwise `now_string` (`runtime/runtime_state.cpp:177-203`);
  - `now()` has microseconds 0, and `now(n)` truncates `nano_seconds` to n digits;
  - `current_date()` is the date of that instant in the query zone.
- **Cast of text with a zone** (`cast_to_datetimev2_impl.hpp`, `util/timezone_utils.cpp:158-175`):
  - the zone is a ±hh[:mm] offset or a name, looked up case-insensitively, with `Z`
    (Zulu), `CST` (Asia/Shanghai) and `UTC`/`GMT±h` forms;
  - the civil time in that zone becomes civil time in the query zone, and microseconds are
    kept;
  - an unknown zone gives NULL when the cast is not strict;
  - a DATEV2 target checks the zone but does not convert.
  - `test_timezone` sets `time_zone='+02:00'`, so '08:15:30Europe/London' (GMT in January) is
    10:15:30.

## Calls (the designer's, for the user to check)

- **CASE, `nullable`, `assert_true` and `ASSERT_NUM_ROWS_NODE` move to story 6.** No suite
  this story makes eligible uses them. In story 6 they come with the join suites that need
  them, so replays and the gate can check them. Building them now would mean code that
  nothing checks against the C++ BE.
- **A time-zone crate.** Named zones need the IANA database. `chrono-tz` embeds it. C++ uses
  cctz with the system's tzdata, so a zone whose rules changed between the two databases
  could differ. The corpus's zones (Europe/London, Asia/Shanghai, offsets) have stable rules.
- **A cast of a column of text with a zone suffix** has no query zone at evaluation time (only
  `bind` sees it). Such a value is refused with a status naming "a time zone in a cast of a
  column", not cast to NULL. No suite does this.
- **`check_overflow_for_decimal` false is refused** for decimal arithmetic at prepare, since its
  rounding differs (`F/multiply.cpp:401-422`). The FE always sends true.

## Global Constraints

- **Byte-exact replies.** Rows equal the C++ BE's. A feature that can't match the C++ BE's
  reply is refused with a status naming it; a wrong answer is never acceptable.
- **Refuse at prepare.** A plan holding anything doris-rs does not run is refused at prepare,
  naming the first unsupported node, sink, function or expression. A signature outside the
  rules above is refused as `function <name>`.
- **No regression.** The ratchets hold, and every eligible suite (487) passes both gates in the
  story's full run.
- **The swap and readback checks stay green.**
- **Formatting.** Format new files only, with `rustfmt --edition 2021 --config max_width=150`.
  Never format a test file that declares `mod common`.

## Review Focus

1. **Boundaries of integer and decimal arithmetic:**
   - BIGINT overflow wraps;
   - `INT64_MIN mod -1` errors, with no panic in Rust;
   - a decimal product needing more than 128 bits is computed exactly;
   - a decimal result past its precision errors.
   Each is a unit test in Task 2.
2. **Zero divisors.** DOUBLE x/0.0 and x/-0.0, decimal x/0 and x mod 0 are NULL, never inf or
   a panic (Task 2 unit tests).
3. **like edge patterns:**
   - an empty pattern;
   - a trailing `\`;
   - `_` on a multi-byte character;
   - a pattern holding regex metacharacters;
   - the EQUALS divergence `a\\_`;
   - a NULL value or pattern;
   - a non-constant pattern, which takes the per-row regex path with correct escape parity.
   Each is a unit test in Task 4.
4. **Rounding at .5 in binary doubles.** round(2.675,2)=2.68, round(1.005,2)=1,
   round(0.285,2)=0.28, round(-2.5,0)=-3, and d > 19 saturates (Task 5 unit tests).
5. **Time zones:**
   - a DST zone in summer (Europe/London in August gives 09:15:30 at +02:00);
   - an unknown zone gives NULL;
   - `now(3)` truncates and does not round;
   - `current_date()` near midnight takes the query zone's date, not UTC's.
   Each is a unit test in Task 6.

---

### Task 1: Features and replays

**Files:**
- Modify: `oracle/scores/epic1-supported.json` (add `expr:ARITHMETIC_EXPR` and `function:`
  `add`, `subtract`, `multiply`, `divide`, `mod`, `abs`, `if`, `ifnull`, `coalesce`, `like`,
  `concat`, `length`, `round`, `now`, `current_date`, `cast_datetime_named_time_zone`; written
  with `json.dumps(indent=1)`)
- Create: `crates/doris-fixtures/replays/<suite>/` for the 32 suites (via `extract-replays
  --only`), and update `replays/index.json`
- Test: `crates/doris-fixtures/tests/replays.rs`

**Interfaces:**
- Produces: the const `SCALARS: &[&str]` (the 32 replay names) in the fixtures test and in
  `doris-be/tests/replay.rs`.

- [ ] **Step 1: Write the failing test** `scalar_replays_hold_their_functions`. Every
  `SCALARS` replay loads, and between them their plans hold each of `add`, `subtract`,
  `multiply`, `divide`, `mod`, `abs`, `if`, `ifnull`, `coalesce`, `like`, `concat`, `length`
  and `round` (use `doris_fixtures::replays::functions`).
- [ ] **Step 2: Run** `cargo test -q -p doris-fixtures --test replays`. Expected: FAIL (the
  replays are missing).
- [ ] **Step 3:** Add the features. Run `extract-replays --only` for the 32 suites, and copy
  them by basename (never `cp -R dir/`).
- [ ] **Step 4: Run** the test again. Expected: PASS. `every_replay_matches_the_reference` in
  doris-be stays red until Task 5.
- [ ] **Step 5: Commit** ("M6 s5: the scalar replays").

### Task 2: Arithmetic

**Files:**
- Create: `crates/doris-exec/src/arith.rs`
- Modify: `crates/doris-exec/src/eval.rs` (dispatch `ARITHMETIC_EXPR` and `abs`; `check_nodes`
  accepts them), `crates/doris-exec/src/lib.rs`, `crates/doris-exec/Cargo.toml` (`ethnum`)

**Interfaces:**
- Produces:
  - `pub fn arithmetic(op: &str, a: (&ColType, &[Datum]), b: (&ColType, &[Datum]), to: &ColType) -> Result<Vec<Datum>, ExecError>`,
    where op is one of `add`, `subtract`, `multiply`, `divide`, `mod`;
  - `pub fn abs(a: (&ColType, &[Datum]), to: &ColType) -> Result<Vec<Datum>, ExecError>`;
  - `pub fn arithmetic_supported(op: &str, args: &[ColType], to: &ColType) -> bool`, which
    `check_nodes` calls.
- Errors are `ExecError::BadRequest`-style statuses carrying C++'s code and text:
  INVALID_ARGUMENT for mod, ARITHMETIC_OVERFLOW_ERRROR for a decimal overflow. Add an
  `ExecError` variant if no existing one maps to those codes.
  - The decimal-overflow `{a}`, `{b}` and `{r}` are each value's decimal text at its own
    scale. `{type}` is C++'s type name, e.g. `Decimal(38, 12)` (check `type_result.get_name()`
    in `core/data_type/data_type_decimal.*` for the exact form).

- [ ] **Step 1: Write the failing tests** in `arith.rs`. Each test's name and assertions:
  - `integer_ops_wrap`: BIGINT `i64::MAX + 1` = `i64::MIN`, and `i64::MIN * -1` = `i64::MIN`.
  - `mod_truncates_and_zero_is_null`: −7 mod 3 = −1; 7 mod −3 = 1; 5 mod 0 = NULL.
  - `mod_of_min_by_minus_one_errors`: the message is `Division of minimal signed number by
    minus one is an undefined behavior, -9223372036854775808 % -1. `
  - `double_divide_by_zero_is_null`: 1.0/0.0, 1.0/−0.0 and 0.0/0.0 are NULL; 1.0/NaN is NaN.
  - `decimal_multiply_rounds_half_away`, at (27,9)×(16,8)→(38,12):
    - raw 1 × raw 50000 → raw 1;
    - −raw 1 × raw 50000 → raw −1;
    - raw 1000000001 × raw 5 → raw 50000.
  - `decimal_multiply_past_i128_is_exact`: (38,10)×(38,10)→(38,10) with both raws
    10^19 + 7. The product needs 256 bits, and the rescaled result is checked against a value
    computed by hand. When it is past 10^38−1, the test expects the overflow error instead.
  - `decimal_multiply_overflow_errors`: (25,14) result past 10^25−1 errors with
    `Arithmetic overflow: ` + C++'s text.
  - `decimal_divide_truncates`:
    - (22,14)/(3,0): 1.00000000000000 / 3 → 0.33333333333333;
    - −2.00000000000000 / 3 → −0.66666666666666;
    - (38,10)/(38,2): 1.0000000000 / 3.00 → 0.33333333;
    - x / 0.00 → NULL.
  - `abs_widens`: abs(INT −2147483648) = BIGINT 2147483648; abs(BIGINT i64::MIN) = LARGEINT
    9223372036854775808.
  - `refuses_unlisted_signatures`: `arithmetic_supported` is false for any DECIMAL256, for
    mixed integer/double arguments, and for `divide` on BIGINT,BIGINT.
- [ ] **Step 2: Run** `cargo test -q -p doris-exec arith`. Expected: FAIL (module missing).
- [ ] **Step 3: Implement** `arith.rs`:
  - integers use `wrapping_*` at the result width (i8…i128);
  - doubles use IEEE, with the zero-divisor NULL;
  - decimals use raw i128 with an `I256` fallback for the product.
  `eval.rs` dispatches `N::ARITHMETIC_EXPR` by function name (decimal nodes carry no opcode),
  and `abs` by name. `check_nodes` refuses decimal arithmetic when the query option
  `check_overflow_for_decimal` is false. If `check_nodes` cannot see query options, refuse it
  in `fragment.rs` where the options are, and ledger where.
- [ ] **Step 4: Run** `cargo test -q -p doris-exec`. Expected: PASS.
- [ ] **Step 5: Commit** ("M6 s5: arithmetic").

### Task 3: Conditionals

**Files:**
- Create: `crates/doris-exec/src/cond.rs`
- Modify: `crates/doris-exec/src/eval.rs`

**Interfaces:**
- Produces:
  - `pub fn if_(c: &[Datum], then: &[Datum], else_: &[Datum]) -> Vec<Datum>`;
  - `pub fn coalesce(args: &[&[Datum]]) -> Vec<Datum>`; `ifnull` is `coalesce` of two.
- The FE gives every branch the result type. When a branch's type differs from ret_type in
  storage, `eval.rs` coerces it with `coerce` first.

- [ ] **Step 1: Write the failing tests:**
  - `if_null_condition_takes_else`: conditions [true, NULL, false] over then [1,2,3] and else
    [7,8,9] give [1,8,9].
  - `coalesce_takes_first_non_null`: [NULL,NULL,3] and [1,NULL,4] give [1,NULL,3].
  - `ifnull_keeps_nan`: ifnull(NaN, 1.0) is NaN.
  - `coalesce_three_args`: the first non-NULL of three wins per row.
- [ ] **Step 2: Run** `cargo test -q -p doris-exec cond`. Expected: FAIL.
- [ ] **Step 3: Implement** `cond.rs` and dispatch `if`, `ifnull`, `coalesce` in `eval.rs`.
  `check_nodes` accepts them when every argument type is `supported()` and, for `if`, the first
  is BOOLEAN.
- [ ] **Step 4: Run** `cargo test -q -p doris-exec`. Expected: PASS.
- [ ] **Step 5: Commit** ("M6 s5: if, ifnull, coalesce").

### Task 4: like, concat, length

**Files:**
- Create: `crates/doris-exec/src/strings.rs`
- Modify: `crates/doris-exec/src/eval.rs`, `crates/doris-exec/Cargo.toml` (`regex`)

**Interfaces:**
- Produces:
  - `pub struct Like` with `pub fn new(pattern: &[u8], escape: Option<u8>) -> Result<Like, ExecError>`
    and `pub fn matches(&self, value: &[u8]) -> bool`, plus `pub fn like_regex(pattern: &[u8]) -> String`
    (the `convert_like_pattern` translation, for tests).
  - Constant patterns are built once, with the fast paths. A non-constant pattern builds a
    `Like` per row through the `like_fn_scalar` path (`like.cpp:746-787`), which has correct
    escape parity and no divergence. Constant and per-row construction are separate
    constructors: `Like::constant` and `Like::per_row`.
  - The non-constant `pattern_type_recognition` path (`like.cpp:658-699`) applies one fast kind
    to all rows when every row fits it. That path carries the divergence for non-constant
    patterns too; reproduce it, and say in a comment which C++ lines it follows.
  - `pub fn concat_column(args: &[(&ColType, &[Datum])]) -> Vec<Datum>` and
    `pub fn length_column(a: &[Datum]) -> Vec<Datum>`.

- [ ] **Step 1: Write the failing tests:**
  - `like_regex_translation`: `a.c` → `^a\.c\z`; `%[x]%` → `.*\[x\]`; `50\%` → `^50%\z`;
    `ab\` → `^ab\\\z`; empty → `^\z`.
  - `like_underscore_is_one_character`: `_` matches `é` (2 bytes) and not `ab`.
  - `like_equals_divergence`: a constant `a\\_` matches `a\_` and not `a\x`. A per-row `a\\_`
    matches `a\x`.
  - `like_custom_escape`: pattern `a!%b` with ESCAPE `!` matches `a%b` only. A raw `\` with
    ESCAPE `!` is literal.
  - `like_is_case_sensitive_and_dot_matches_newline`.
  - `like_null_is_null`: a NULL value or pattern gives NULL.
  - `concat_null_is_null` and `length_counts_bytes` (`length('é')` = 2).
- [ ] **Step 2: Run** `cargo test -q -p doris-exec strings`. Expected: FAIL.
- [ ] **Step 3: Implement** with `regex::bytes::RegexBuilder` (`dot_matches_new_line(true)`,
  `unicode(true)`). Dispatch `like`, `concat`, `length` in `eval.rs`; `check_nodes` accepts them
  on text arguments only. Keep `expr.rs`'s `concat` for the write side, and share the
  byte-joining code.
- [ ] **Step 4: Run** `cargo test -q -p doris-exec`. Expected: PASS.
- [ ] **Step 5: Commit** ("M6 s5: like, concat, length").

### Task 5: round, and the replays pass

**Files:**
- Create: `crates/doris-exec/src/round.rs`
- Modify: `crates/doris-exec/src/eval.rs`; `crates/doris-be/tests/replay.rs` (`SCALARS`)

**Interfaces:**
- Produces:
  - `pub fn round_double(x: f64, d: i32) -> f64`;
  - `pub fn round_decimal(raw: i128, in_scale: i32, out_scale: i32) -> i128`.
- The digits argument must be a constant (INT_LITERAL, possibly under a CAST); otherwise
  `round` is refused at prepare. Int16 bounds error as `Scale argument for function round is
  out of bound: {d}`.

- [ ] **Step 1: Write the failing tests:**
  - `round_double_matches_cpp`:
    - (2.675,2)=2.68, (1.005,2)=1.0, (0.285,2)=0.28, (1.15,1)=1.2;
    - (−0.5,0)=−1, (−2.5,0)=−3;
    - (12345.6789,−2)=12300;
    - (1e300,20)=inf.
  - `round_decimal_half_away`: −1.235 at (38,3) to scale 2 gives −1.24, 1.234 gives 1.23,
    and 0.5 at (33,25) to scale 0 gives 1.
  - In `doris-be/tests/replay.rs`, add the `SCALARS` names to the replays
    `every_replay_matches_the_reference` runs.
- [ ] **Step 2: Run** `cargo test -q -p doris-exec round` and `cargo test -q -p doris-be --test
  replay`. Expected: FAIL.
- [ ] **Step 3: Implement** `round.rs` and dispatch. Then make every `SCALARS` replay match.
  Fix each mismatch test-first: a unit test for the cause, then the fix. Ledger each one that
  needs a feature beyond this plan (a cast, a type).
- [ ] **Step 4: Run** `cargo test -q` (workspace) and clippy. Expected: PASS.
- [ ] **Step 5: Commit** ("M6 s5: round; the scalar replays pass").

### Task 6: now(), current_date(), casts with a time zone

**Files:**
- Create: `crates/doris-exec/src/bind.rs`
- Modify:
  - `crates/doris-exec/src/value.rs`: a zone-aware datetime parse;
  - `crates/doris-exec/src/fragment.rs` (or wherever a fragment's TExprs are checked): bind
    them first;
  - `crates/doris-be/src/load.rs`: load column exprs are bound too;
  - `crates/doris-be/tests/stream_load.rs`: drop `AWAITING`;
  - `oracle/needs_supplement.json`: drop the 5 entries;
  - `crates/doris-exec/Cargo.toml`: `chrono`, `chrono-tz`.

**Interfaces:**
- Produces:
  - `pub struct QueryClock { pub zone: QueryZone, pub seconds: i64, pub nanos: i32 }`;
  - `pub fn QueryClock::from_globals(g: &TQueryGlobals) -> Result<QueryClock, ExecError>`;
  - `pub enum QueryZone { Offset(i32), Named(chrono_tz::Tz) }`;
  - `pub fn find_zone(name: &str) -> Option<QueryZone>`, with C++'s lookup: case-insensitive,
    `Z`, `CST`, `UTC`, `GMT±h`, ±hh[:mm];
  - `pub fn bind(e: &TExpr, clock: &QueryClock) -> Result<TExpr, ExecError>`;
  - `pub fn parse_datetimev2_in_zone(s: &str, scale: i32, zone: &QueryZone) -> Option<u64>`.

- [ ] **Step 1: Write the failing tests:**
  - `now_is_the_query_instant`: globals `timestamp_ms` 1262304930123, `nano_seconds`
    123456789, `time_zone` "Asia/Shanghai":
    - `now()` binds to DATETIMEV2(0) `2010-01-01 08:15:30`;
    - `now(3)` binds to `.123` (truncated, not rounded);
    - `current_date()` binds to `2010-01-01`.
  - `current_date_uses_the_query_zone`: 23:30 UTC in Asia/Shanghai is the next day's date.
  - `cast_with_named_zone`, in query zone `+02:00`:
    - `2010-01-05 08:15:30Europe/London` → `2010-01-05 10:15:30`;
    - `2010-08-05 08:15:30Europe/London` → `09:15:30`;
    - `…+08:00` → `03:15:30`;
    - `…UTC` → `10:15:30`.
  - `unknown_zone_is_null`: `2010-01-05 08:15:30Mars/Base` → NULL.
  - `date_target_checks_but_keeps_date`: a cast to DATEV2 of `2010-01-05 23:15:30+08:00` is
    `2010-01-05`.
  - In `stream_load.rs`, the two `json_load_default*` suites now match the corpus: remove
    `AWAITING` and its branch.
- [ ] **Step 2: Run** `cargo test -q -p doris-exec bind` and `cargo test -q -p doris-be --test
  stream_load`. Expected: FAIL.
- [ ] **Step 3: Implement.** `bind` folds:
  - `now`, `now(n)`, `current_date` and `curdate` into DATE_LITERALs of the node's type;
  - a CAST_EXPR whose only child is a STRING_LITERAL to DATEV2/DATETIMEV2 into the cast's
    value, parsed in the zone, or a NULL_LITERAL.

  The row-time cast refuses a zone suffix it cannot resolve without a clock (see Calls). Remove
  the 5 supplement entries (`test_current_date`, `test_get_stream_load_state`, both
  `test_json_load_default*`, `test_timezone`) and run `make -C oracle test`.
- [ ] **Step 4: Run** `cargo test -q` (workspace), clippy and `make -C oracle test`.
  Expected: PASS. Then run the 5 suites on their own (targeted runs), one at a time. Expected:
  each passes both gates.
- [ ] **Step 5: Commit** ("M6 s5: now, current_date, named time zones").

### Task 7: Gate, docs, As built

**Files:**
- Modify: `oracle/scores/epic1-out.txt`, `oracle/scores/epic1-strict.txt`, `oracle/README.md`
  (a story 5 paragraph after story 4's), this plan (an "As built" section)

- [ ] **Step 1: Full run** (fresh FE, storage wiped; `make epic-check TARGET=rust`). Expected:
  487 eligible, 0 failing in either gate, no ratchet suite lost. Fix any failure test-first,
  then re-run.
- [ ] **Step 2:** Regenerate the ratchets from the run's `grade.json`, as story 4 did (minus
  `test_move_column_with_cast`), and confirm that no suite left them.
- [ ] **Step 3:** Run `make -C oracle test`, the readback and swap checks, and `cargo test -q`.
  Expected: PASS.
- [ ] **Step 4:** Write the README paragraph (numbers, the story 6 move, the calls) and the
  As built section. Then commit ("M6 s5: gate, docs, As built").
