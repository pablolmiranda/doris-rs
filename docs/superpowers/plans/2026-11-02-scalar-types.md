# Remaining Scalar Types Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode every remaining V2-format scalar column type from reference-written segments and prove each row against the reference's own output.

**Architecture:** Extend the golden corpus with two tables, generalize `doris-segment`'s column reader from integers to a typed `ColumnValues` over all fixed-width types, then add binary columns (dictionary and plain pages). Corpus tests render decoded values in the reference's MySQL text form (test-side helper) and compare with the expected TSV.

**Tech Stack:** Rust 1.93 (`doris-segment` crate), existing corpus scripts (bash, python3), the running 4.1.5 reference.

**Spec:** `docs/superpowers/specs/2026-11-02-scalar-types-design.md` (and its parent `2026-11-02-golden-corpus-design.md`)

## Global Constraints

- Reference and protos stay pinned at `4.1.5-release`; corpus rebuilt only with `make corpus` from `oracle/` while the reference is up.
- Expected files are never hand-edited; every corpus table sets `"disable_auto_compaction" = "true"`; corpus total stays under 2 MB.
- Rust: `unsafe_code = "forbid"`, `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` clean; no panic reachable from crc-valid corrupt bytes.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. A dictionary codeword at or beyond the dictionary size (crc-valid page): `Error::Corrupt`, not a panic or wrong row. Test in Task 3 with a hand-built page.
2. A plain page whose `num_elems` trailer implies offsets outside the page, or offsets that decrease: `Error::Corrupt`. Test in Task 3.
3. CHAR with an interior NUL: the reference returns the prefix before the first NUL while VARCHAR keeps the NUL. Corpus row in Task 1, asserted in Task 3.
4. Bit-shuffle with element sizes 3 and 12 across more than one LZ4 block (block sizes 2728 and 680 elements): corpus rows in Task 1, asserted (a page larger than one block was decoded) in Task 2.
5. DATETIMEV2 with scale below 6 stores full microseconds but prints only `scale` digits: corpus columns `c_dt0` and `c_dt3`, asserted in Task 2.

---

### Task 1: Corpus tables `strings_fallback` and `scalar_more`

**Files:**
- Create: `oracle/corpus/tables/07_strings_fallback.sql`, `oracle/corpus/tables/08_scalar_more.sql`
- Modify: `oracle/tests/test_corpus_expect.py`, `oracle/tests/test_corpus_manifest.py` (TABLES list), `oracle/corpus/README.md` (type coverage)
- Regenerate: `oracle/corpus/{expected,data,manifest.json}`

**Interfaces:**
- Produces tables (unique id = schema position):
  - `strings_fallback(k INT NOT NULL, c_char CHAR(20), c_varchar VARCHAR(64), c_string STRING)` with `"storage_dict_page_size" = "1024"`; ~3000 rows of mostly distinct values plus rows with `''`, NULL, and `unhex('610062')` (a, NUL, b) in all three string columns.
  - `scalar_more(k INT NOT NULL, c_dec32 DECIMAL(9,2), c_dec128 DECIMAL(38,10), c_dt0 DATETIME, c_dt3 DATETIME(3), c_ipv4 IPV4, c_ipv6 IPV6, c_datev1 DATEV1, c_datetimev1 DATETIMEV1, c_decv2 DECIMALV2(27,9))`; handwritten edge rows (min, max, zero, negative, NULL, `::`, `::ffff:10.0.0.1`, `fe80:0:0:0:1:0:0:1`) plus ~5000 generated rows.

- [ ] **Step 1: Write both SQL files; add both names to TABLES in the two Python tests**
- [ ] **Step 2: Run `python3 -m unittest discover -s oracle/tests` — Expected: FAIL (expected files for the new tables missing)**
- [ ] **Step 3: `make -C oracle corpus`; Expected: both tables load; corpus under 2 MB**
- [ ] **Step 4: Probe the footers of the new segments: legacy columns must carry storage types 14, 15, 16 (not converted to V2); `strings_fallback` string columns must contain at least one data page with mode 2 (PLAIN). If a legacy type was converted, drop that column and record a Ruling.**
- [ ] **Step 5: Python tests pass; commit**

### Task 2: Fixed-width types through `read_column`

**Files:**
- Modify: `crates/doris-segment/src/column.rs` (generalize), `crates/doris-segment/src/lib.rs` (export new module if split)
- Create: `crates/doris-segment/src/values.rs` (`ColumnValues`, `Date`, `DateTime`, `Decimal12`, bit unpacking)
- Create: `crates/doris-segment/tests/common/mod.rs` (expected TSV loader, reference text rendering, float comparison)
- Modify: `crates/doris-segment/tests/ints_corpus.rs` (use `read_column` + common)
- Create: `crates/doris-segment/tests/scalars_corpus.rs`

**Interfaces:**
- Produces in `doris_segment::values`:
  - `pub struct Date { pub year: u32, pub month: u8, pub day: u8 }`
  - `pub struct DateTime { pub date: Date, pub hour: u8, pub minute: u8, pub second: u8, pub microsecond: u32 }`
  - `pub struct Decimal12 { pub integer: i64, pub fraction: i32 }`
  - `pub fn unpack_datev2(u32) -> Date`, `unpack_datetimev2(u64) -> DateTime`, `unpack_date_v1(u32) -> Date`, `unpack_datetime_v1(i64) -> DateTime`
  - `pub enum ColumnValues { Bool, I8, I16, I32, I64, I128, F32, F64 (Vec<Option<_>>), Decimal { unscaled: Vec<Option<i128>>, precision: u32, scale: u32 }, DecimalV2 { values: Vec<Option<Decimal12>>, precision: u32, scale: u32 }, Date(Vec<Option<Date>>), DateTime { values: Vec<Option<DateTime>>, scale: u32 }, Ipv4(Vec<Option<u32>>), Ipv6(Vec<Option<u128>>), Bytes(Vec<Option<Vec<u8>>>) }` with `len()`/`is_empty()`.
- Produces in `doris_segment::column`: `pub fn read_column(file: &SegmentFile, unique_id: u32) -> Result<ColumnValues, Error>`; `decode_bitshuffle_values(values: &[u8], non_null: usize, elem_size: usize)` now requires the header's element size to equal `elem_size`. `read_int_column` and `IntValues` are removed.
- Produces in `tests/common`: `expected_rows(table) -> Vec<Vec<String>>`, `render(&ColumnValues) -> Vec<String>` (reference text: Bool `1`/`0`; ints decimal; decimals with exactly `scale` fraction digits; DECIMALV2 with 9 digits; Date `YYYY-MM-DD`; DateTime `YYYY-MM-DD hh:mm:ss[.f{scale}]`; IPv4 dotted; IPv6 Doris inet_ntop6; floats rendered with `{}` only as a placeholder), `assert_column_matches(table, name, &ColumnValues, &[String])` which compares floats by `parse::<f32|f64>().to_bits()` and everything else by string.

- [ ] **Step 1: Write `tests/common/mod.rs` and `tests/scalars_corpus.rs`: `scalar_others_fixed_width_columns_match_expected` (k, c_float, c_double, c_decimal, c_date, c_datetime) and `scalar_more_fixed_width_columns_match_expected` (all columns of `scalar_more`), plus `odd_element_sizes_cross_lz4_blocks` asserting a `c_datev1` page with more than `default_block_size(3)` values and a `c_decv2` page with more than `default_block_size(12)` values was decoded; unit tests `datev2_and_datetimev2_unpack` (2024-02-29 and 9999-12-31 23:59:59.999999 from their packed ints) and `legacy_date_and_datetime_unpack`**
- [ ] **Step 2: `cargo test -p doris-segment` — Expected: compile errors (`read_column`, `values` missing)**
- [ ] **Step 3: Implement `values.rs` and generalize `column.rs` (element size table: BOOL/TINYINT 1, DATE 3, SMALLINT 2, INT/FLOAT/DATEV2/DECIMAL32/IPV4 4, BIGINT/DOUBLE/DATETIME/DATETIMEV2/DECIMAL64 8, DECIMAL 12, LARGEINT/DECIMAL128I/IPV6 16); migrate `ints_corpus.rs`**
- [ ] **Step 4: `cargo test -p doris-segment` all pass; clippy, fmt clean; commit**

### Task 3: Binary columns (dictionary and plain fallback)

**Files:**
- Create: `crates/doris-segment/src/binary.rs` (plain page V1 decode, dictionary page, dictionary data page modes, CHAR truncation)
- Modify: `crates/doris-segment/src/column.rs` (route CHAR/VARCHAR/STRING to `binary`)
- Create: `crates/doris-segment/tests/strings_corpus.rs`

**Interfaces:**
- Produces in `doris_segment::binary`: `pub fn decode_plain_v1(page: &[u8]) -> Result<Vec<&[u8]>, Error>`; `pub fn read_dictionary(file, meta) -> Result<Vec<Vec<u8>>, Error>`; `pub fn decode_dict_data_page(values: &[u8], non_null: usize, dict: &[Vec<u8>]) -> Result<(DictPageMode, Vec<Vec<u8>>), Error>` with `pub enum DictPageMode { Dict, Plain }`; CHAR values truncated at the first NUL in `read_column`.
- `read_column` returns `ColumnValues::Bytes` for types 13, 17, 26.

- [ ] **Step 1: Write `tests/strings_corpus.rs`: `scalar_others_string_columns_match_expected`, `strings_fallback_columns_match_expected` (asserting both Dict and Plain modes were seen per column), `big_tables_varchar_matches_under_compression` (`lz4f_big`, `zstd_big` column `c_varchar`), `char_truncates_at_first_nul_varchar_keeps_it` (the `unhex('610062')` row), and corrupt-input tests `dictionary_codeword_out_of_range_is_an_error`, `plain_page_offsets_outside_page_are_an_error`, `plain_page_decreasing_offsets_are_an_error`**
- [ ] **Step 2: `cargo test -p doris-segment --test strings_corpus` — Expected: compile errors**
- [ ] **Step 3: Implement `binary.rs` and routing; PLAIN_ENCODING_V2 mode returns `Error::UnsupportedEncoding`**
- [ ] **Step 4: Full suite, clippy, fmt; update `oracle/corpus/README.md` coverage table; commit**
