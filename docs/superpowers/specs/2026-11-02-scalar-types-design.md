# Corpus v1 completion: remaining scalar types — design

Date: 2026-11-02. Builds on `2026-11-02-golden-corpus-design.md`, whose v1 goal is "DUPLICATE
tables with all scalar types". The first increment decoded only integers and BOOL; this one
decodes every other scalar type the 4.1.5 reference writes in the V2 storage format.

## Goals

- Decode, from reference-written segments, every row of: FLOAT, DOUBLE, DECIMAL32, DECIMAL64,
  DECIMAL128I, DECIMALV2, DATEV2, DATETIMEV2 (scales 0, 3, 6), legacy DATE and DATETIME (v1),
  IPV4, IPV6, CHAR, VARCHAR, STRING.
- Binary columns through both of their on-disk shapes: dictionary codewords (bit-shuffled INT)
  against a dictionary page, and the plain fallback pages written once the dictionary fills.
- Prove it against the reference: decoded rows rendered the way the reference's MySQL text
  protocol prints them must equal the expected TSV, and floats must parse back to identical bits.

## Non-goals

DECIMAL256, TIMESTAMPTZ, JSONB, VARIANT, ARRAY/MAP/STRUCT, HLL/BITMAP/QUANTILE_STATE/AGG_STATE,
and the V3 storage format (PLAIN_ENCODING_V2 dictionary fallback, PLAIN numerics, externalized
column meta). Each is a later corpus version.

## Reference facts this design relies on (4.1.5 source, verified)

- Default V2 encodings: fixed-width numerics, dates, decimals, IPs use BIT_SHUFFLE; BOOL uses
  RLE; CHAR, VARCHAR, STRING use DICT_ENCODING (`encoding_info.cpp`).
- The bit-shuffle element size must equal the storage type's size (`bitshuffle_page.h`): DATE v1
  is 3 bytes (`uint24_t`), DECIMALV2 is 12 (`decimal12_t {int64 integer; int32 fraction}`
  packed), IPV6 and DECIMAL128I and LARGEINT are 16.
- DATEV2 is `day:5 | month:4 | year:23` from the low bit; DATETIMEV2 is
  `microsecond:20 | second:6 | minute:6 | hour:5 | day:5 | month:4 | year:18`; legacy DATE is
  `year << 9 | month << 5 | day`; legacy DATETIME is the integer `YYYYMMDDhhmmss`.
- IPV4 is a host-order u32 (`127.0.0.1` is `0x7f000001`); IPV6 is a u128 whose big-endian bytes
  are the address; text uses the APR `inet_ntop6` rules with the Doris IPv4-embedding condition.
- A dictionary data page body is `mode: u32 LE | payload | nullmap`, where mode 5 (DICT) means a
  bit-shuffled INT codeword page, 2 (PLAIN) a BinaryPlainPage V1, 8 (PLAIN_V2) the V3-format
  variant. The dictionary page itself is a BinaryPlainPage whose encoding is named in
  `DictPageFooterPB`. BinaryPlainPage V1 is `strings | offsets (u32 LE each) | num_elems u32 LE`.
- CHAR(N) is stored zero-padded to N bytes, including in the dictionary; the reference reads it
  back truncated at the first NUL (`strnlen`, `binary_plain_page_char_strip_pre_decoder.h`).
- Values are stored only for non-null rows; the page's null bitmap decides which rows take one.
- Footer `ColumnMetaPB` carries `precision` and `frac` (scale) for decimals and datetimes.

## Decisions

- One `read_column(file, unique_id) -> ColumnValues` replaces `read_int_column`; `ColumnValues`
  holds typed values per family (ints, floats, decimals as unscaled i128 with scale, DECIMALV2 as
  its raw integer and fraction, structured Date and DateTime, IPs, bytes).
- Rendering to the reference's text form is test code (it belongs to a future MySQL-protocol
  crate); floats are compared by parsing the expected text, which the reference prints as
  shortest round-trip, and comparing bits.
- New corpus tables: `strings_fallback` (small `storage_dict_page_size` to force the plain
  fallback in CHAR, VARCHAR and STRING, plus an interior-NUL row; the FE aligns the requested
  1024 up to 4096) and `scalar_more` (decimal and
  datetime scales, legacy types, IPs, enough rows for multi-block bit-shuffle at odd element
  sizes).
