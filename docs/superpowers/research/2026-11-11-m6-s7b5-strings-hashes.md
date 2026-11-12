# Batch 5 research: strings, encode/decode, hashes, crypto

C++ root: `.cache/doris-src/be/src`. Short paths:
- `fs.cpp` = `exprs/function/function_string.cpp`
- `concat.h` = `exprs/function/function_string_concat.h`
- `basic.cpp` = `exprs/function/function_string_basic.cpp`
- `search.cpp` = `exprs/function/function_string_search.cpp`
- `repl.h` = `exprs/function/function_string_replace.h`
- `misc.cpp` = `exprs/function/function_string_misc.cpp`
- `hash.cpp` = `exprs/function/function_hash.cpp`
- `mm3.cpp` = `util/hash/murmur_hash3.cpp`
- `hu.hpp` = `util/hash_util.hpp`
- `enc.cpp` = `exprs/function/function_encryption.cpp`
- `eu.cpp` = `util/encryption_util.cpp`
- `vsf.h` = `util/simd/vstring_function.h`

Plan signatures come from the traffic corpus (`oracle/epic1/traffic`). They were dumped with the b2 tool
(`scratchpad/b2/ws/target/debug/examples/dump_fn_exprs` piped through `b2/condense_dates.py`) into one file per suite,
`scratchpad/b5r/sig/*.txt`. A new example, `dump_fn_nodes` (same workspace), shows which plan-node field carries an
expression. Its raw dump is `scratchpad/b5r/raw.txt`. Notation: `T?` means the node is nullable.

**Constant-only calls reach the BE.** Most calls in these suites are all-literal (`elt(1,'hello','doris')`,
`sm3('abc')`, `aes_encrypt('text',…)`), and they appear in the plans. C++ evaluates them at open, so errors surface
in exec_plan_fragment, as in the b2/b3 docs. Calls with a NULL literal argument are mostly FE-folded (exceptions are
noted below).

Default NULL handling (`use_default_implementation_for_nulls() == true`) means any NULL argument gives NULL. These
functions turn it off: **elt, null_or_empty, not_null_or_empty**.

"UTF-8 chars" has two meanings in C++. Each site below says which one it uses:
- **lead-table**: `UTF8_BYTE_LENGTH[b]` steps 1/2/3/4/5/6 bytes by lead byte. This is the same as `utf8_len` in
  strfn.rs.
- **non-continuation count**: `get_char_len(src,len)` (vsf.h:313-332) counts bytes **not** in 0x80..=0xBF.

The two agree on valid UTF-8.

---

## 1. String functions (priority 1)

### elt: `elt(int, varchar…) -> VARCHAR?` (always nullable)
concat.h:236-345, registered fs.cpp:1515. `use_default_implementation_for_nulls = false`.
- `pos` is Int32 and `n = argc-1`. The result is **NULL** when `pos` is NULL, `pos <= 0`, `pos > n`, or **the chosen
  argument is NULL** for that row. NULLs in the arguments that are not chosen are ignored.
- Otherwise the result is the chosen argument's bytes, copied verbatim.
- Corpus (nereids test_string_function, all literal): `elt(0|1|2|3, "hello","doris")` → `\N`, `hello`, `doris`, `\N`.

### locate: 2-arg `locate(substr, str) -> INT`, 3-arg `locate(substr, str, pos) -> INT`
- **3-arg** (search.cpp:56-207, registered :876, alias `position` :884). Corpus:
  `locate(varchar(2) '78', varchar(65533) slot?, int 2) -> INT?`.
  - A special case comes first: `str=='' && substr=='' && pos==1` → **1** (MySQL compatibility, :154-159).
  - If **both columns are entirely ASCII** (column-level `is_ascii()`), positions are bytes:
    - `pos <= 0 || pos > len(str)` → 0.
    - If `substr` is empty → `pos`.
    - Otherwise, search `str[pos-1..]` for the first byte match. Found at offset `m` → `pos + m`; not found → 0.
  - Otherwise the UTF-8 path applies (:167-186). Character starts use the **lead-table** (`get_char_len(...,index)`,
    vsf.h:291).
    - `pos <= 0 || pos > nchars` → 0.
    - If `substr` is empty → `pos`.
    - Otherwise, search the bytes from the start of char `pos`. A match at byte offset `m` gives
      `pos + nonContinuationCount(adjusted[..m])`.
  - The two paths give the same results on valid UTF-8, so one per-row UTF-8 implementation is enough.
  - Search (`util/string_search.hpp:44-71`) is a plain byte search. It returns -1 when not found.
- **2-arg** (fs.cpp:346-500, `StringLocateImpl` = instr with arguments swapped, registered :1446):
  - Empty `substr` → **1**, even when `str` is empty.
  - Otherwise, the first byte match at offset `m` → `nonContinuationCount(str[..m]) + 1`. Not found → 0.
- Default NULLs. The return type is non-null INT in C++, and the FE wraps it nullable when the inputs are nullable.
- Expected values:
  - `LOCATE('bar','foobarbar')` = 4 and `LOCATE('xbar','foobar')` = 0. These are FE-folded and not in the corpus.
  - Table `strings_LOCATE_test`, `LOCATE('78', str, 2)` for rows `"123456789"`, `"1234567890"`, NULL, `"abcabc"`,
    `"789"` gives `7`, `7`, `\N`, `0`, `0`.

  In the last row, `'78'` is at byte 0, before pos 2, so the result is 0.

### null_or_empty / not_null_or_empty: `(varchar) -> BOOLEAN` (never NULL)
basic.cpp:304-371 (`NullOrEmptyImpl`), registered :382-383. Default NULLs are off.
- `null_or_empty(x) = x IS NULL OR length(x) == 0`, and `not_null_or_empty` is its negation.
- The result is never NULL.
- The corpus forwards `null_or_empty(NULL_LITERAL)` to the BE, so a NULL literal argument must work.
- Expected: `null_or_empty(null)`=true, `("")`=true, `("a")`=false; `not_null_or_empty`: false, false, true.

### sub_replace: `sub_replace(str, new, start[, len]) -> VARCHAR?` (always nullable)
repl.h:181-361, registered fs.cpp:1542-1543. Default NULLs. **`start` is 0-based.**
- **3-arg**: `len = nonContinuationCount(new)`, the character count of the replacement (repl.h:326-348).
- `start < 0 || len < 0` → **NULL**.
- **ASCII path**, used when the whole `str` column is ASCII (repl.h:252-277):
  - `len(str) <= start` → NULL. This means `sub_replace('', …)` is always NULL.
  - Otherwise the result is `std::string::replace(start, len, new)`: `len` is clamped to the end and the rest is kept.
  - Note: `len` counts **bytes** here. In 3-arg form it is still the **char count of `new`**, so
    `sub_replace('abcd','大家',0)` replaces 2 bytes → `大家cd`.
- **UTF-8 path** (repl.h:279-321):
  - `start_byte` = byte offset after stepping `start` chars (lead-table, `iterate_utf8_with_limit_length`, vsf.h:190).
  - `start_byte == size` → **NULL**, which happens whenever `start >= nchars`.
  - Otherwise `end_bytes` = bytes covered by `len` chars from there, clamped to the end. The result is
    `str[..start_byte] + new + str[start_byte+end_bytes..]`.
  - Edge: for a truncated multi-byte char at the end, the step can overshoot `size`. C++ would then throw
    `std::out_of_range`. This is not reachable with valid UTF-8, so clamp it.
- The two paths agree on ASCII data, so a single per-row UTF-8 implementation using byte positions is fine.
- Expected (nereids test_string_function, all literal, all in corpus):

  | call | result |
  |---|---|
  | `("this is origin str","NEW-STR",1)` | `tNEW-STRorigin str` |
  | `("doris","***",1,2)` | `d***is` |
  | `('你好世界','a',1)` | `你a世界` |
  | `('你好世界','ab',1)` | `你ab界` |
  | `('你好世界','ab',1,20)` | `你ab` |
  | `('你好世界','abcd我',1,2)` | `你abcd我界` |
  | `('你好世界','a',6)` | `\N` |
  | `('你好世界','大家',0)` | `大家世界` |
  | `('你好世界','大家114514',1,20)` | `你大家114514` |
  | `('你好世界','大家114514',6,20)` | `\N` |
  | `('你好世界','大家',4)` | `\N` (start == nchars) |
  | `('你好世界','大家',-1)` | `\N` |

### starts_with: `(varchar|char, varchar) -> BOOLEAN[?]`
fs.cpp:281-289 (`strl.starts_with(strr)`, plain bytes). It is registered via `FunctionBinaryToType` with
expr-zonemap hooks at fs.cpp:1361-1438. Default NULLs. This was already covered in the b3 doc.
- New in test_string_function: the literal form `starts_with("hello world","hello"|"world")` → `true` / `false`,
  return type BOOLEAN (not nullable).
- The `null` case is FE-folded to `\N`.
- test_expr_zonemap_pruning uses it in WHERE on `varchar(32|512)` and `char(10)` slots. CHAR values must arrive
  without trailing padding.

---

## 2. encode_as_* / decode_as_varchar (compressed materialize)

C++: `exprs/function/function_encode_varchar.cpp:55-110` (registered :112-117) and
`function_decode_varchar.cpp:40-105`. Both use `util/simd/reverse_copy_bytes.h`.

### Where the FE inserts them
`fe/.../nereids/rules/analysis/CompressedMaterialize.java`, gated by the session variable
`enable_compress_materialize`.
- **Aggregate group-by keys**, which are encoded. Agg outputs that referenced the key get
  `decode_as_varchar(encoded)`. `LogicalRepeat` grouping sets work the same way.
- **Sort keys**, which are encoded. No decode is needed because sort outputs are unchanged.

Only non-constant `CharacterType` expressions with declared `len > 0` are encoded. The encoder is chosen by length:

| declared len | encoder |
|---|---|
| < 2 | `encode_as_smallint` |
| < 4 | `encode_as_int` |
| < 7 | `encode_as_bigint` |
| < 15 | `encode_as_largeint` |
| ≥ 15 | not encoded |

`PushDownEncodeSlot`, `DecoupleEncodeDecode` and `SimplifyEncodeDecode` then push the encode below joins, unions and
set operations.

In the thrift plans, the corpus shows **only these carriers**:
- `TPlanNode.projections`: OLAP scan, agg, hash join, exchange, nested-loop join (12), analytic (14).
- `TPlanNode.intermediate_projections_list`.
- `union_node.const_expr_lists`, from test_code_varchar's constant SELECTs.

Grouping keys, sort ordering exprs, join conjuncts and sink partition exprs then reference the **encoded slot**, an
INT/BIGINT/LARGEINT column. So doris-rs needs nothing new beyond scalar functions in projections. The plan then sorts
or hashes plain integers.

Signatures seen: `encode_as_bigint(char(5) | varchar(4..6))`, `encode_as_int(varchar(2|3))`, `encode_as_smallint(varchar(1))`,
`encode_as_largeint(varchar(8..16))`, and `decode_as_varchar(smallint|int|bigint|largeint) -> VARCHAR[?]`. Both
nullable and non-null forms appear. Default NULLs.

### Algorithm (N = 2, 4, 8, 16 bytes; T = i16/i32/i64/i128)
- **encode(s)**, with `L = len(s)`:
  - If the **maximum row length in the whole input column** is greater than `N-1`, the call fails (see Errors below).
    The NULL rows' nested values count too; they are normally `''`.
  - Build an unsigned N-byte big-endian integer `V`. Byte 0 (MSB) is `s[0]`, …, byte `L-1` is `s[L-1]`. The bytes
    between are zero, and the **least significant byte is `L << 1`**.
  - The result is `(V >> 1)` as a logical shift, reinterpreted as T. It is always ≥ 0.

  This is equivalent to the C++ steps: memory written little-endian (`reverse_copy_bytes`), `mem[0] = L<<1`,
  arithmetic `>>= 1`, then `& T::MAX`.

  ```rust
  let mut v: u128 = 0;
  for (i, &c) in s.iter().enumerate() { v |= (c as u128) << (8 * (N - 1 - i)); }
  v |= (L as u128) << 1;
  (v >> 1) as T
  ```

- **decode(x)**:
  - `L = (x as u8 of the low byte) & 0x7F`.
  - If `L >= N`, the call fails (see Errors below).
  - Otherwise `V = (x << 1)` (wrapping) and `s[j] = byte (N-1-j)` of V counted from LSB, for `j < L`.
- Verified against test_code_varchar.out by script:

  | input | result |
  |---|---|
  | `'a'`→smallint | 12417 |
  | `'ab'`→int | 816906242 |
  | `'abcd'`→bigint | 3508640225272266756 |
  | `'abcdefgh'`→largeint | 64722988298011025234418333488001318920 |
  | `'abcdefghijklmno'`→largeint | 64722988298011025238216334405476497295 |
  | `'b'`→largeint | 65132171793460877772286545953736884225 |
  | `''` | 0 for every type |
  | `decode_as_varchar(SMALLINT 8321)` | `A` |

  `decode(encode('\0')) == '\0'` is true, because the length byte keeps trailing NULs.
- Errors. Both are in the corpus as fetch/report errors:
  - `(127.0.0.1)[INTERNAL_ERROR]String is too long to encode, input string size 16, max valid string size for encode_as_largeint is 15`
    comes from `Status::InternalError` (encode :73-80). The format is
    `String is too long to encode, input string size {max}, max valid string size for {name} is {N-1}`.
  - `(127.0.0.1)[INVALID_ARGUMENT]Invalid input of function decode_as_varchar, input type SMALLINT value 2, string size 2, should not be larger than 2`
    comes from `doris::Exception(INVALID_ARGUMENT)` (decode :82-90). The format is
    `Invalid input of function decode_as_varchar, input type {SMALLINT|INT|BIGINT|LARGEINT} value {x}, string size {L}, should not be larger than {N}`.
  - Both cases are constant calls, so the error fires at open.
- CHAR(5) input (pushdown_encode): the bytes must be **unpadded**, as for starts_with/rpad.

---

## 3. Hashes

`hash.cpp`, all through `FunctionVariadicArgumentsBase` (`function_variadic_arguments.h:32-80`).
- Default NULLs: **any NULL argument → NULL**. There is no "hash of NULL" value; the FE folds the literal
  `xxhash_64(null)` to `\N`.
- They are variadic. Argument 1 is hashed with the initial seed, and **each further argument is hashed with the
  previous result as seed**.
- String arguments only in these suites. xxhash also accepts VARBINARY. The suites have no zero-argument call; that
  case would return `(T)0xe28dbde7fe22e41c`.

| function | return | algorithm | initial seed | file |
|---|---|---|---|---|
| `murmur_hash3_32` | INT (i32 bits of u32) | standard MurmurHash3 **x86_32** | **104729** (`HashUtil::MURMUR3_32_SEED`, hu.hpp:218) | hash.cpp:117-208, mm3.cpp:109-166 |
| `murmur_hash3_64` | BIGINT (i64 bits of u64) | **Doris-specific `murmur_hash3_x64_64`**: one 64-bit lane over **8-byte** blocks (see below) | 0 | hu.hpp:228-236, mm3.cpp:486-548 |
| `murmur_hash3_64_v2` | BIGINT | `h1` of standard x64_128 (`murmur_hash3_x64_64_shared`), seed 0 | 0 | mm3.cpp:472-483 |
| `xxhash_32` | INT | **XXH32** | 0 | hash.cpp:305-385, hu.hpp:437 |
| `xxhash_64` (alias `xxhash3_64`) | BIGINT | **XXH3_64bits_withSeed (XXH3, not XXH64!)** | 0 | hu.hpp:447-449 (xxHash v0.8.3) |

Chaining:
- For INT results the next seed is the u32 bit pattern of the previous i32.
- For BIGINT results it is the u64 bit pattern of the previous i64.

**Doris murmur_hash3_64** (mm3.cpp:486), with c1=0x87c37b91114253d5 and c2=0x4cf5ad432745937f. Note that it is not
the 128-bit algorithm truncated, so no crate provides it.

```text
h = seed
for each little-endian u64 block k (len/8 of them):
    k *= c1; k = rotl(k,31); k *= c2; h ^= k
    h = rotl(h,27); h = h*5 + 0x52dce729
tail (len&7 bytes, little-endian into k, k = 0 if no tail):
    k *= c1; k = rotl(k,31); k *= c2; h ^= k
h ^= len (as u64); h = fmix64(h)
```

murmur_hash3_32 is textbook x86_32: `h1 ^= len` truncates len to u32, followed by `fmix32`.

Expected values from test_hash_function.out (nereids; corpus literal calls), all verified bit-exact:

| call | result |
|---|---|
| `murmur_hash3_32("hello")` | 1321743225 |
| `murmur_hash3_32("hello","world")` | 984713481 |
| `murmur_hash3_64("hello")` | -3215607508166160593 |
| `murmur_hash3_64("hello","world")` | 3583109472027628045 |
| `xxhash_32("hello")` | -83855367 |
| `xxhash_32("hello","world")` | -920844969 |
| `xxhash_64("hello")` | -7685981735718036227 |
| `xxhash_64("hello","world")` | 7001965798170371843 |

Extra reference values: murmur_hash3_32('') = -965378730, murmur_hash3_64('') = 0, xxhash_32('') = 46947589,
xxhash_64('') = 3244421341483603138.

How they were verified:
- murmur by a Python port (`scratchpad/b5r/mm.py`).
- xxhash by **`twox-hash` 2.1.5** (already in Cargo.lock), offline scratch crate `scratchpad/b5r/xx`:
  `XxHash32::oneshot(seed, b)` and `XxHash3_64::oneshot_with_seed(seed, b)`, with features
  `xxhash32, xxhash3_64`.

Recommendation: hand-port the two murmur functions (about 40 lines; they cannot be paired with a crate for the 64
variant). The `murmur3` crate's `murmur3_32` would match the 32-bit one, but it is not worth the dependency.

---

## 4. Base64 (priority 2)

### to_base64: `(varchar) -> STRING[?]`
fs.cpp:1133-1168, `FunctionStringEncode<ToBase64Impl,false>` (:1460). Default NULLs; nullability follows the input.
- `''` → `''`.
- Otherwise standard RFC 4648 encoding, alphabet `A-Za-z0-9+/`, with `=` padding and no line breaks.
- `base64` 0.22 `STANDARD.encode` matches exactly.

### from_base64: `(varchar) -> VARCHAR?` (always nullable)
fs.cpp:1170-1215, `FunctionStringOperateToNullType` (:1461). The decoder is the aklomp/base64 v0.5.2 library
(`thirdparty/vars.sh:540`), with the symbol renamed `do_base64_decode` (url_coding.cpp:104-114).
- `''` → `''`. Invalid input → **NULL**; it never raises an error.
- aklomp rules:
  - Any byte outside the alphabet is invalid, and that includes **whitespace and newlines**.
  - **Unpadded input is invalid**, because the length must end on a 4-char boundary (`state.bytes == 0`). For
    example `YQ` → NULL.
  - `=` in position 0 or 1 of a quad is invalid.
  - `XY==` and `XYZ=` are valid only at the very end; anything after the padding is invalid.
  - **Non-zero trailing bits are accepted** (`YR==` → `a`).
  - Unverified: a lone `XY=` at end-of-input sets AEOF with ret=1, and the `state.bytes` check decides the outcome.
    Treat it as NULL.
- Rust: `base64::engine::GeneralPurpose::new(&alphabet::STANDARD, GeneralPurposeConfig::new()
  .with_decode_allow_trailing_bits(true).with_decode_padding_mode(DecodePaddingMode::RequireCanonical))`.
  Map `Err` → NULL. The default `STANDARD` engine **rejects trailing bits**, which is a difference.
- Corpus: always nested as `aes_decrypt(from_base64(lit|slot), …)`. Literal inputs: `wr2JEDVXzL9+2XtRhgIloA==`, etc.

---

## 5. SM3: `sm3(varchar)`, `sm3sum(varchar…)` → `VARCHAR`
`exprs/function/function_string_digest.cpp:44-125`. `sm3` is an alias of `sm3sum` (:295). Default NULLs, so any NULL
argument → NULL.
- One SM3 digest is computed over the **concatenation of all arguments**. Empty arguments are skipped, which makes
  no difference to the result.
- Output: **64 lowercase hex chars**.
- Expected (verified with Python `hashlib.new('sm3')`):
  - `sm3("abc")` = `66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0`
  - `sm3("abcd")` = `sm3sum("ab","cd")` = `82ec580fe6d36ae4f81cae3c73f4a5b3b5a09c943172dc9053c69fd8e18dca1e`
- Rust: the `sm3` crate (RustCrypto) implements standard GB/T 32905. It is not cached offline and needs a network
  fetch.

---

## 6. aes_encrypt / aes_decrypt / sm4_encrypt / sm4_decrypt (priority 2)

`enc.cpp` holds the dispatch (:116-450) and the registrations (:470-503). `eu.cpp` holds the crypto, using OpenSSL
EVP; the BE builds OpenSSL **1.1.1s**. Return type is **`VARCHAR?` (always nullable)**, and the value is raw bytes.

### What the FE sends
- The FE always expands the 2/3-argument SQL forms to **4 args**: `(src, key, iv, mode)`.
- The default mode comes from the session variable `block_encryption_mode`, and `''` means default. Corpus examples:
  - `aes_encrypt('text', key, '', 'AES_128_ECB')`
  - `aes_encrypt('text', key, '0123456789', 'AES_256_CBC')`
- A **5-arg** form `(src, key, iv, mode, aad)` exists for GCM.
- `sm4_*` gets mode `'SM4_128_ECB'`.

### Registrations
- 3-arg `EncryptionAndDecryptTwoImpl`: `(src,key,mode)` with IV pointer = nullptr. It is not seen in the corpus.
- 4-arg `EncryptionAndDecryptMultiImpl`: default mode AES_128_ECB / SM4_128_ECB.
- 5-arg form: default mode **AES_128_GCM**.

### Per row
1. Any NULL argument → NULL (default NULLs).
2. **Mode string**:
   - If it is non-empty, it is looked up **case-insensitively** (`StringCaseUnorderedMap`) in `aes_mode_map` (27 AES
     modes) or `sm4_mode_map` (`SM4_128_{ECB,CBC,CFB128,OFB,CTR}`).
   - **An unknown mode gives NULL**, with no error. For example, an AES mode passed to sm4 gives NULL.
   - An empty mode means the registration default.
   - For literal modes the FE rejects bad modes first, with `mode X is not supported` (an FE error, not BE).
3. **Key folding** (MySQL `my_aes_create_key`, eu.cpp:125-145): `k = [0u8; bits/8]`, then
   `for (i, b) in key.enumerate() { k[i % k.len()] ^= b }`.
   - 128/192/256 bits come from the mode, and SM4 is 128.
   - An empty key gives an all-zero key.
4. **IV** (eu.cpp:227-238, 345-356):
   - `ivlen` = the cipher's IV length: 0 for ECB, 16 for CBC/CFB/OFB/CTR, **12 for GCM**.
   - Start from `buf = *b"DORISDORISDORIS_"` (16 bytes), copy `iv[..min(len,16)]` over its prefix, and use
     `buf[..ivlen]`. So IV `''` → `DORISDORISDORIS_`, and `'0123456789'` → `0123456789DORIS_`.
   - If `ivlen > 0` and the IV pointer is null, the result is NULL. Only the 3-arg form passes a null pointer.
5. **Non-GCM**:
   - EVP with **padding on**: PKCS#7 for ECB/CBC; the stream modes are unpadded.
   - Encrypt output length = `n + 16 - n%16` for block modes.
   - Decrypt failure (bad final block or padding) → **NULL**.
6. **GCM**:
   - Encrypt output = `IV(12) || ciphertext || tag(16)`.
   - AAD is used only in the 5-arg form; the 4-arg form passes nullptr, so there is no AAD.
   - Decrypt **takes the IV from the first 12 bytes of the input and ignores the iv argument**. Input shorter than
     28 bytes → NULL; a tag mismatch → NULL.
7. A decrypted result may be empty: `sm4_decrypt(sm4_encrypt(''))` = `''`, not NULL.

### Expected values
All non-GCM ones were verified with `openssl enc` (Homebrew OpenSSL 4.0.3) via `scratchpad/b5r/enc.py`.

key `F3229A0B371ED2D9441B830D21A390C3` (32 ASCII bytes), plaintext `'text'`:

| mode | iv | to_base64 result |
|---|---|---|
| AES_128_ECB | any | `wr2JEDVXzL9+2XtRhgIloA==` |
| AES_256_ECB | any | `BO2vxHeUcw5BQQalSBbo1w==` |
| AES_256_CBC | `''` | `3dym0E7/+1zbrLIaBVNHSw==` |
| AES_256_CBC | `0123456789` | `tsmK1HzbpnEdR2//WhO+MA==` |
| AES_256_CBC | `0123456789ff` | `ciacXDLHMNG7CD9Kws8png==` |

Decrypt cases:
- `mvZT1KJw7N0RJf27aipUpg==` under 128_ECB and under 256_CBC with iv `0123456789` → `\N` (bad padding).
- `BO2v…` under 256_ECB → `text`.
- `wr2J…` under 256_ECB → `\N`.

Other cases:
- key_case_sensitive: `hex(aes_encrypt('text','aabbccdd1234','','AES_128_ECB'))` = `B1438FAE21B290AD4C639782F3C2FAA6`.
- `hex(aes_encrypt('', 'securekey456'))` = `1AA197B230C9488519819C2D25AF4CB0`. The same value comes from
  `rpad('',16|17,'')`, which is `''`.
- `hex(sm4_encrypt('', 'securekey456'))` = `0D56319E329CDA9ABDF5870B9D5ACA57`, and decrypting that gives `''`.
- GCM (expected values from .out; not independently verified here):
  - `aes_encrypt('Spark SQL','1234567890abcdef','123456789012','aes_128_gcm','Some AAD')` →
    `MTIzNDU2Nzg5MDEyMdXvR41sJqwZ6hnTU8FRTTtXbL8yeChIZA==`
  - `aes_encrypt('Spark','abcdefghijklmnop12345678ABCDEFGH',unhex('00'*12),'aes_256_gcm','This is an AAD mixed into the input')`
    → `AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4`

  Both decrypt back with iv `''`. The table versions with slot args (gcm_5..8) give the same values.
- test_stream_load_with_aes_encrypt: `aes_decrypt(from_base64(slot STRING?), key, '', 'AES_128_ECB')` decodes to
  `hello world`, `doris is great`, `test encryption`.

  The encrypt side runs inside the stream-load column mapping; check whether doris-rs evaluates load exprs.

### Rust
RustCrypto `aes` + `ecb` + `cbc` + `aes-gcm` (12-byte nonce, AAD) and `sm4` + `ecb`. These match OpenSSL bit-exactly
for these modes. None are cached in `~/.cargo/registry`, so they need a network fetch.

The rarer modes:
- `CFB` = `CFB128` (`cfb-mode`)
- `CFB8` (`cfb8`)
- `CTR` = 128-bit big-endian counter (`ctr::Ctr128BE`)
- `OFB` (`ofb`)
- **CFB1 has no RustCrypto crate.** Hand-roll it, or use the `openssl` crate for everything; linking system OpenSSL
  is exact by construction.

---

## 7. unicode_normalize: `(text, const varchar) -> type of arg 0` (priority 2)
misc.cpp:1510-1650. ICU 69.1 (Unicode 13). Default NULLs, so `unicode_normalize(NULL,'NFC')` = `\N`.
`''` → `''`.

### Mode argument
- It must be constant (`get_arguments_that_are_always_constant {1}`). The mode is checked in `open()` (fragment
  scope), so its errors fire at **open**.
- Non-constant mode: `(ip)[INVALID_ARGUMENT]The second argument 'mode' of function unicode_normalize must be constant`.
  The corpus has a slot/slot plan for this.
- Lookup: the mode is trimmed of whitespace and lowercased, then must be one of `nfc` `nfd` `nfkc` `nfkd` `nfkc_cf`.
  Otherwise:
  `(127.0.0.1)[INVALID_ARGUMENT]Invalid normalization mode 'invalid_mode' for function unicode_normalize. Supported modes: NFC, NFD, NFKC, NFKD, NFKC_CF`.
  The message prints the **lowercased** mode. This message is in the corpus.

### Per row
- Convert UTF-8 → UTF-16; invalid bytes become U+FFFD.
- If `quickCheck == YES`, return **the original input bytes**.
- Otherwise return the normalized result, converted back to UTF-8.

### Expected
- `hex(unicode_normalize(unhex('43616665CC81'),'NFC'))` = `436166C3A9`
- `('436166C3A9','NFC')` → same
- `(…,'  nFd  ')` = `43616665CC81`
- `('ABC 123',' nfkc_cf ')` = `abc 123`
- `('plain-ascii','NFKD')` = `plain-ascii`

### Rust
`unicode-normalization` gives NFC, NFD, NFKC and NFKD, plus `is_nfc_quick`, etc., which return `IsNormalized::Yes`.
Copy the quick-check shortcut so that invalid UTF-8 round-trips the same way.

**NFKC_CF has no crate equivalent.** ICU's `nfkc_cf` data is the NFKC_Casefold mapping. An approximation is
`nfkc(casefold(nfkc(x)))` with default-ignorables removed. The suite only needs ASCII lowercasing. The crate's
Unicode version (≥15) differs from ICU 69's Unicode 13 for newly assigned characters only.

---

## 8. xpath_string: `(text, text) -> VARCHAR?` (priority 2, high risk)
misc.cpp:1127-1253, using pugixml **1.15** (`load_buffer` with `parse_default`, `xpath_query::evaluate_string`).
Default NULLs: any NULL → NULL.

### Per row
The checks run in this order; the xpath is checked first.
1. Empty xpath → **NULL**.
2. Compile the xpath. A failure is an error: `Function xpath_string failed to build XPath query: {pugi what()}`.
3. Empty xml → **NULL**.
4. Parse the xml. A failure is an error, `(ip)[INVALID_ARGUMENT]Function xpath_string failed to parse XML string: {desc}`.
   - The corpus has `No document element found` for `'wrong xml'`.
   - Other pugixml descriptions: `Error parsing start element tag`, `Start-end tags mismatch`,
     `Error parsing end element tag`, `Error parsing element attribute`, `Error parsing comment`,
     `Error parsing CDATA section`, `Could not determine tag type`, etc.
5. The result is the XPath 1.0 `string()` of the expression:
   - For a node-set, the string-value of the **first node in document order**. An empty node-set gives `''`.
   - For a number, XPath formatting (`4`, not `4.0`).
   - For a boolean, `true` / `false`.

### pugixml `parse_default` details
- Whitespace-only PCDATA is **dropped**.
- Comments and PIs are not kept.
- CDATA is kept as text.
- Entities are expanded, and EOL `\r\n` becomes `\n`.

When an argument is constant, it is checked once for all rows. A constant empty xpath yields all NULL without parsing
the xml.

### Expected values
From test_xpath_string.out, about 55 corpus plans, mostly literal:

| expression | result |
|---|---|
| `/a/b` | `123` |
| `/data/a[last()]` | `3` |
| `/mixed/text()` | `Text` |
| `/test/*[2]` | `y` |
| CDATA | `<nested>value</nested>` |
| `<item val="2"/>` | `''` |
| `substring-before` | `J` |
| `substring-after` | `hn` |
| `contains` | `true` |
| `string-length` | `4` |
| `normalize-space` | `John` |
| `translate` | `HELLO WORLD` |
| `concat(substring(...))` | `hello_test` |
| `substring(…,7)` | `world` |

There are also table tests over nullable/non-null STRING slots (see the .out).

### Rust
There is no pugixml port. `sxd-document` + `sxd-xpath` is XPath 1.0, but it keeps whitespace-only text nodes, has
different error texts, and is stricter or looser in different places.
- Options: a small hand-written XML parser that mimics pugixml's `parse_default` plus sxd-xpath on the resulting
  tree, or a narrow hand-written XPath subset.
- The error texts must be mapped to pugixml's strings.
- This is the largest and riskiest function in the batch.

---

## Crate summary

| need | crate | bit-exact? | cached offline? |
|---|---|---|---|
| xxhash_32 / xxhash_64 | `twox-hash` 2.1.5 (`XxHash32`, `XxHash3_64`) | yes (verified) | yes (in Cargo.lock) |
| murmur_hash3_32/64 | hand-port (~40 lines) | yes (verified vs .out) | n/a |
| base64 | `base64` 0.22.1 + custom config (allow trailing bits, canonical padding) | yes | yes |
| sm3 | `sm3` | yes (standard) | no |
| AES ECB/CBC/GCM, SM4 ECB | `aes`, `ecb`, `cbc`, `aes-gcm`, `sm4` (or the `openssl` crate) | yes | no |
| AES CFB1 | none (hand-roll or `openssl`) | — | — |
| NFC/NFD/NFKC/NFKD | `unicode-normalization` | yes for Unicode 13 chars | no |
| NFKC_CF | none | approximate | — |
| xpath_string | none matching pugixml | no | — |

## Risks / cannot match exactly
1. **xpath_string**: pugixml parse rules, error descriptions and number formatting must be hand-matched. This is
   high risk.
2. **unicode_normalize NFKC_CF**: no crate. ASCII is fine; full Unicode is approximate. Unicode version skew.
3. **from_base64** edge `XY=` (single pad at end) is unverified. Everything else is specified above.
4. **Crypto crates need a network fetch.** CFB1 is not in RustCrypto.
5. **encode_as_*** raises its error on the **column-wide max length**, not per row. A batch-splitting difference
   could change which query errors only if a row is too long; the FE guarantees declared len ≤ limit, so this only
   matters for hand-written calls.
6. **CHAR inputs** (encode_as_bigint(char(5)), starts_with(char(10))) depend on doris-rs delivering CHAR without
   padding.
7. The open-time errors (unicode_normalize mode, decode_as_varchar / encode_as_largeint on constants, xpath parse on
   constants) must use the same `[INVALID_ARGUMENT]` / `[INTERNAL_ERROR]` coded path as parse_data_size (b3 doc).
