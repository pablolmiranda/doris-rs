# M5 research: SELECT on the wire (the read loop)

Sources: `.cache/doris-src` (4.1.5-release), paths relative to it. Corpus: `oracle/epic1/traffic/*.bin.gz`
(1456 suites, every one in the gate list), decoded with a throwaway Rust tool built against
`doris-wire`/`doris-thrift`/`doris-proto`: `<scratchpad>/m5/wire` (an extended copy of the M4 tool). Its modes are
`dump <suite> [idx,..]` (full Debug), `outline <suite>` (one line per RPC, a plan tree per fragment, and decoded
result rows) and `qscan` (corpus statistics). The tool is throwaway and is not kept.

Counting rule: a **result query** is a query_id with at least one fragment whose `output_sink` is `RESULT_SINK`.
There are 32057 of them, in 1330 of the 1456 suites (126 suites have none).

---

## 1. RPC sequence for a query

### 1.1 Which exec method

| fragments | method | queries |
|---|---|---|
| 1 | `exec_plan_fragment` (`need_wait_execution_trigger=false`) | 21328 |
| ≥2 | `exec_plan_fragment_prepare` + `exec_plan_fragment_start{query_id}` (`need_wait_execution_trigger=true`) | 10729 |

**The proto comment "<3 fragments → exec_plan_fragment" (`gensrc/proto/internal_service.proto:1233`) is stale.**
The FE rule is `fragments > 1` → two-phase (`fe/.../qe/CoordinatorContext.java:207-215`,
`qe/Coordinator.java:954-961,998`). Every query sends exactly one exec request: all fragments arrive in one
`TPipelineFragmentParamsList` (the corpus has `q.nreq=1` for all 32057).

BE side: `exec_plan_fragment_prepare` builds everything. If `need_wait_execution_trigger` is set, the BE waits
until `start` (`runtime/fragment_mgr.cpp:696-697`). `start_query_execution` looks up the QueryContext and calls
`set_ready_to_execute` (`fragment_mgr.cpp:404-415`); an unknown query → InternalError "Failed to get query
fragments context". Both replies are the same 25-byte `PExecPlanFragmentResult{status 0, received/execution/execution_done_time}`
used in M4.

### 1.2 Worked example: `SELECT nationkey FROM tpch_tiny_nation ORDER BY nationkey DESC LIMIT 5`
(`query_p0.limit.sql.withSimpleLimit`, rows 24..20)

| # | RPC | content |
|---|---|---|
| 0/1 | `exec_plan_fragment_prepare` | 2 fragments (below) → `{status 0}` |
| 3 | `fetch_data` req | `PFetchDataRequest{finst_id = query_id, resp_in_attachment=false}` (25 B) |
| 5/4 | `exec_plan_fragment_start{query_id}` | `{status 0}` |
| 2 | `fetch_data` resp | `{status 0, packet_seq 0, eos false, row_batch = TResultBatch{5 rows "24".."20"}}` (69 B) |
| 6 | `fetch_data` req | second long-poll (never answered: the FE cancels first) |
| 8/7 | `cancel_plan_fragment` | `{finst_id 0/0, query_id, cancel_reason LIMIT_REACH(1), cancel_status{9 LIMIT_REACH, "query reach limit"}}` → `{status 0}` |
| 10/9 | `update_cache` | SQL cache store (§4) → `PCacheResponse{CACHE_OK}` |

Records in the same ms are not reliably ordered: the first fetch can come before `start` is sent. **The result buffer
must therefore exist when `prepare` returns.** C++ creates it in `ResultSinkOperatorX::prepare`
(`exec/operator/result_sink_operator.cpp:135-145`, keyed by query_id when `enable_parallel_result_sink`).

`SELECT count() FROM tbl_scalar_types_dup_bitmapindex WHERE c_tinyint = 14` (`datatype_p0.scalar_types.sql.dup_bitmap_index_q02_tinyint`):
prepare → start → `fetch_data` → `{seq 0, eos false, rows [["396"]]}` → `fetch_data` → `{seq 1, eos true,
query_statistics{returned_rows 1}}` → `cancel_plan_fragment{reason INTERNAL_ERROR(3), status{76 FINISHED,"Finished"}}`
→ `update_cache`. **No `reportExecStatus` for a successful SELECT.**

### 1.3 Decoded `TPipelineFragmentParamsList` (withSimpleLimit)

List level: `params_list` [F1, F0] (**the receiving/result fragment comes first**); `runtime_filter_info{merge_addr 127.0.0.1:8060,
topn_filter_descs [{source_node_id 1, is_asc false, null_first false, target {0: SLOT_REF slot0}}]}` (a TopN runtime
filter is an optimization: 42 queries have one, so it can be ignored).

**F1** (`fragment_id 1`, full params): `per_exch_num_senders {2: 4}` (exchange node 2 waits for 4 senders' EOS),
`desc_tbl` (slots 0/4/5 BIGINT `nationkey` in tuples 0/1/2; `table_descriptors [{OLAP_TABLE, tpch_tiny_nation}]`),
`destinations []`, `num_senders 1`, `coord 127.0.0.1:9020`, `query_globals{now_string, timestamp_ms, time_zone "UTC",
nano_seconds, lc_time_names "en_US"}`, `query_options` (§1.5), `fragment_num_on_host 5`, `backend_id`,
`need_wait_execution_trigger true`, `local_params [ {fragment_instance_id {hi, lo+5}, per_node_scan_ranges {}, sender_id 0, backend_num 4} ]`,
`total_instances 1`, `is_nereids true`, `current_connect_fe`.
Plan: `EXCHANGE_NODE#2{limit 5, exchange_node{input_row_tuples [2], sort_info{ordering [SLOT_REF slot5], is_asc [false],
nulls_first [false]}, offset 0, partition_type UNPARTITIONED}}`; `output_exprs [SLOT_REF slot5 "nationkey"]`;
`output_sink RESULT_SINK{type MYSQL_PROTOCOL(0)}`; `partition UNPARTITIONED`.

**F0** (`fragment_id 0`, **`is_simplified_param=true`: no `desc_tbl`, `query_globals`, `coord` or `resource_info`.
It shares them with F1 through the QueryContext**, `fragment_mgr.cpp:471-479`. This applies to 19145 fragments.)
`destinations [{fragment_instance_id = F1's instance, server :9060, brpc_server 127.0.0.1:8060}]`, `num_senders 4`, `parallel_instances 1`,
`total_instances 4`. `local_params` has 4 instances (`lo+1..lo+4`, sender_id/backend_num 0..3). **Only instance 0 has scan
ranges**: `per_node_scan_ranges {0: [3 × TScanRangeParams{TPaloScanRange{hosts [127.0.0.1:9060], schema_hash "0",
version "2", version_hash "", tablet_id …, db_name ""}}]}`. The other three instances are empty and must still send EOS.
Plan (pre-order): `SORT_NODE#1{limit 5, row_tuples [2], sort_node{sort_info{ordering [SLOT_REF slot5 tuple2],
asc [false], nulls_first [false], sort_tuple_slot_exprs [SLOT_REF slot4 tuple1]}, use_top_n true, offset 0, merge_by_exchange true,
algorithm HEAP_SORT(0), use_local_merge true}}` → `OLAP_SCAN_NODE#0{tuple 0, key_column_name ["nationkey"], key_type DUP_KEYS,
is_preaggregation true, columns_desc [4 TColumn], sort_info (same), sort_limit 5, push_down_agg NONE,
output_column_unique_ids {0}, projections [SLOT_REF slot0], output_tuple_id 1, is_serial_operator true}`.
`output_sink DATA_STREAM_SINK{dest_node_id 2, output_partition UNPARTITIONED, is_merge true}`; `partition HASH_PARTITIONED`.

`TPaloScanRange` over all 200079 corpus ranges: always `schema_hash "0"`, `version_hash ""`, one host, `db_name ""`,
and no `index_name`/`table_name`/key ranges. **`version` is the visible version to read** (read rowsets in [0, version]);
the tablet is found by `tablet_id` alone. 1062 ranges are non-palo (schema/meta/data-gen scans).

### 1.4 Instances

Instances per fragment: 1 (37926), 4 (11270), 2 (1325), 5 (361), 10 (227), 16 (51), other (≈50). This follows
`parallel_instance` (4: 17092 queries, 1: 14300). Scan ranges are spread over instances ("4 of 4" 6516, "1 of 4" 3426).
Since everything runs on one BE, doris-rs may collapse instances. What must be preserved: the per-exchange sender count,
ordering via merge exchanges, and limit/offset.

### 1.5 Query options/globals (result queries)

| field | values (count) |
|---|---|
| `query_type` | SELECT(0) always |
| `be_exec_version` | 13 always |
| `batch_size` | 8160 (30462), 4064 (810), 4096 (691), 1/2/3 (91), other |
| `enable_local_exchange` | true always |
| `enable_parallel_result_sink` | true (32032), false (25: then `finst_id` = the result instance id, 54 fetches) |
| `serde_dialect` | DORIS(0) always |
| `mysql_row_binary_format` | false (32032), **true (25: binary protocol rows, `MysqlRowBinaryBuffer`)** |
| `return_object_data_as_binary` | false always |
| `is_report_success`/`enable_profile` | unset/false except 14 queries |
| `query_timeout` | 900 (18740), 43200 (13313, FE-internal queries) |
| `time_zone` | "UTC" 31124, "+08:00" 477, "Z" 236, ±hh:mm/IANA names ≈200 |
| `dry_run_query` | true in 3 |
| `single_backend_query` | true always |

### 1.6 `reportExecStatus` for a query

`PipelineFragmentContext::send_report` (`exec/pipeline/pipeline_fragment_context.cpp:2248-2298`): when
`!_is_report_success` (every SELECT that is not profiled), it **returns without reporting if the status is OK,
LIMIT_REACH or FINISHED**. It reports only on error, once per fragment. Corpus reports for result queries: status
INTERNAL_ERROR(6) 503, INVALID_ARGUMENT(33) 80, CANCELLED(1) 8, LIMIT_REACH 1, and OK 27+3 (the profiled ones).
An error report as captured (`datatype_p0.decimalv3.test_decimalv3_overflow`), sent for **each** fragment (fid 0 and fid 1):

```
query_id, backend_num -1, fragment_instance_id {0,0}, status TStatus{6, ["(127.0.0.1)[E-255]Arithmetic overflow: …"]},
done true, backend_id <be>, fragment_id <fid>, query_type SELECT(0)     # nothing else is set
```

The FE then sends `cancel_plan_fragment{reason INTERNAL_ERROR(3), cancel_status{6, same msg}}`. The parked fetch is
answered `PFetchDataResult{status PStatus{-255, ["(127.0.0.1)[E-255]…"]}}` with no other fields set. **The PStatus carries the
raw Doris error code (negative E-codes, e.g. -255, -218, or 33), while the thrift TStatus maps E-codes to INTERNAL_ERROR(6).**
The message format is `"(<be ip>)[<code name>]<msg>"`.

### 1.7 `cancel_plan_fragment` for queries

The FE sends it after **every** result query (16660 FINISHED, 473 LIMIT_REACH, the rest errors). It always has `finst_id 0/0`
and `query_id` set. FINISHED → no-op. LIMIT_REACH (the FE has the rows it needs before eos) → stop the query and
release the buffer. Reply `{status 0}` (`service/internal_service.cpp:627-674`). 436 fetch requests have no captured
response (55484 requests, 55048 responses). That is close to the 473 LIMIT_REACH cancels, and withSimpleLimit shows the
pattern: a second fetch is parked, then the cancel comes. I infer, without checking each one, that the FE dropped those fetches.

---

## 2. Fragments on one BE: EXCHANGE + DATA_STREAM_SINK are local

There is no `transmit_block` in the corpus because it is BE→BE traffic and the capture is FE↔BE only. **And C++ does not
send it to itself:** a channel is local when `brpc_server == BackendOptions::get_localhost() && port == config::brpc_port`
(`exec/exchange/vdata_stream_sender.h:118-120`) and `enable_local_exchange` (`vdata_stream_sender.cpp:57-63`). A local
channel hands blocks straight to the `VDataStreamRecvr` from `vstream_mgr()->find_recvr(instance_id, dest_node_id)`
(`vdata_stream_sender.cpp:115-128`); `exchange_sink_operator.cpp:110-119` counts the local channels, and when all are
local there is no sink buffer and no RPC. Every destination in the corpus is `127.0.0.1:8060` → doris-rs can wire
fragments with in-process queues keyed by (dest instance id, dest node id).

| | values (fragment/node count) |
|---|---|
| DATA_STREAM_SINK `output_partition` | UNPARTITIONED 9052, **UNPARTITIONED + `is_merge`** 6971 (feeds a merge exchange), HASH_PARTITIONED 2207, RANDOM 421, BUCKET_SHFFULE_HASH_PARTITIONED 261 |
| EXCHANGE_NODE `partition_type` / merge | UNPARTITIONED plain 8472, UNPARTITIONED merge (`sort_info` set) 5554, +limit 1381/674, HASH 2246, RANDOM 869, BUCKET_SHUFFLE 261; `offset>0` in 48 (always with limit) |
| fragment `partition` | HASH 27876, UNPARTITIONED 18468, RANDOM 4839 |
| `destinations` per sink | 1 (16162), 4 (1973), 2-16 (≈780) |

The exchange source merges sorted streams when `exchange_node.sort_info` is set, applying `limit`/`offset`
(`exec/operator/exchange_source_operator.cpp:120-162`). HASH/BUCKET_SHUFFLE sinks hash `partition_exprs` to choose among
the destinations. Those appear only in agg/join shapes (§6), so they come after M5's core.

---

## 3. The result path: RESULT_SINK → `fetch_data`

### 3.1 Protocol

* Request `PFetchDataRequest{finst_id, resp_in_attachment}`: `finst_id == query_id` (55313 requests) or the result
  instance id (54, when `enable_parallel_result_sink=false`); `resp_in_attachment` is always **false**, so the data is in the message
  body (`fe/.../qe/ResultReceiver.java:75-96`).
* The FE keeps **one outstanding fetch** (a long poll). The BE answers when it has a batch, when the result is closed, or on failure
  (`runtime/result_block_buffer.cpp:149-184`: queued batch → `on_data`; closed → `on_close`; otherwise the ctx is parked).
  When a batch arrives with a waiter parked, it goes straight to the waiter (`:248-253`). A buffer that is not found is
  logged and **never answered** (`service/internal_service.cpp:679-695`), so the FE waits until it times out.
* FE: `ResultReceiver.getNext` (`ResultReceiver.java:98-200`) waits with `timeoutTs = start + query_timeout`
  (→ TIMEOUT "query timeout"). A non-OK status → error. **`packet_seq` must equal the FE's counter (0,1,2,…) or the FE fails with
  "receive error packet"**. `eos` ends the query. `empty_batch=true` → empty, not eos (never seen in the corpus). `row_batch`
  is deserialized with **TBinaryProtocol** (`rpc/TCustomProtocolFactory.java:33-36`).
* Data packet (`exec/sink/writer/vmysql_result_writer.cpp:74-99`): `status OK, packet_seq n, eos false, row_batch =
  ThriftSerializer(compact=false)` of `TResultBatch{rows, is_compressed false, packet_seq 0 (never set; the BE leaves it 0),
  attached_infos unset}`. **No `query_statistics` on data packets.**
* EOS packet (`vmysql_result_writer.cpp:64-72`): `status OK, query_statistics{returned_rows = total rows}, packet_seq =
  number of data packets sent, eos true`. No `row_batch`.
* Failure (`:58-62`): only `status` is set.

Exact bytes:
```
data (5 rows "24".."20", seq 0):
0a 02 08 00 | 10 00 | 18 00 | 2a 3b  0f 00 01 0b 00 00 00 05  00 00 00 03 02 32 34  …×5  02 00 02 00  0a 00 03 00*8  00
 status{0}   seq 0   eos 0   field 5: TBinary list<binary>[5], each row = i32 len + lenenc cells; is_compressed=false; packet_seq=0; stop
eos, empty result:     0a 02 08 00 10 00 18 01 22 02 18 00      (seq 0, eos, returned_rows 0)
eos after 1 data pkt:  0a 02 08 00 10 01 18 01 22 02 18 01      (seq 1, eos, returned_rows 1)
```

Corpus: fetch responses are 30876 eos, 23699 data, 473 error. Per query: 0 data + eos 12590 (**empty result**, including
10837 FE stats queries), 1+eos 13551, 2+eos 4368, 3-5+eos 320, no eos (LIMIT_REACH) 836.
Batching: each sink block (≤ `batch_size` rows) becomes one TFetchDataResult. While it waits in the queue, the next one is
appended to it if the rows stay `< 32768` (`RESULT_SINK_BUFFER_SIZE`, `result_sink_operator.h:132`) and the bytes stay ≤
`thrift_max_message_size` (`result_block_buffer.cpp:208-238`). How rows split into packets is not observable to the
client, but `packet_seq` must be contiguous.

### 3.2 Row encoding (MySQL text protocol, `serde_dialect DORIS`)

Each row is a concatenation of cells. A cell is a MySQL length-encoded string (len < 251: 1 byte; < 65536: `0xFC`+u16 LE;
< 2^24: `0xFD`+u24; else `0xFE`+u64), and **NULL is the single byte `0xFB`** (`util/mysql_global.h:67-102`, used by
`vmysql_result_writer.cpp:210-226`). The cell text comes from `serde->write_column_to_mysql_text`. The default is `to_string`
(`core/data_type_serde/data_type_serde.cpp:226-231`); Nullable returns false for NULL (`data_type_nullable_serde.cpp:474-484`).
The serde comes from the **output expr's block type**, and its scale is the expr's (`vmysql_result_writer.cpp:152-174`).

| type | rule (C++) | corpus samples |
|---|---|---|
| BOOLEAN | `fmt::format_int` of UInt8 → `0`/`1` (`exprs/function/cast/cast_to_string.h:183`, `data_type_number_serde.cpp:1787-1792`) | `0`, `1` |
| TINYINT..BIGINT | `fmt::format_int` (`cast_to_string.h:196-265`) | `-127`, `-128`, `-9`, `6001215` |
| LARGEINT | int128 decimal (`cast_to_string.h:287-300`) | `-999999999`, `2` |
| FLOAT / DOUBLE | `NaN`, `Infinity`, `-Infinity`, else **fmt 7.1.3 `"{}"`** (`cast_to_string.h:98-123`; fmt version from `thirdparty/vars.sh:293`): shortest round-trip digits for the type's own width; exponent form when exp10 < -4 or ≥ 16, written `e±dd`; no trailing `.0`; `-0` kept | FLOAT `6.6`, `-32769.79`, `12345678`, `1.2345678e+16`, `1.23456e-05`, `1e+35`, `-0`; DOUBLE `-6349.399999999999`, `1.7`, `1e-308`, `1.2345678901234568e+16`, `-1.7976931348623157e+308`, `NaN` |
| DECIMAL32/64/128I/256 | `decimal_to_string(value, scale)`: exactly `scale` fraction digits, `-` sign, no exponent (`cast_to_string.h:358-405`, `core/types.h:126,393`; `data_type_decimal_serde.cpp:1187-1192`) | (9,2) `123.45`; (38,0) `369`; (20,3) `1.100`; (38,38) `0.000…0`; (76,9) `1.000000000` |
| DECIMALV2 | `DecimalV2Value::to_string(scale)` (`core/value/decimalv2_value.cpp:371-…`), with scale from the expr (`vmysql_result_writer.cpp:159-167`) | **always 9 fraction digits in the corpus**, even for declared (10,3)/(18,2): `95357.100000000` |
| DATEV2 / DATE | `YYYY-MM-DD` (`cast_to_string.h:452-465`) | `2017-10-01` |
| DATETIMEV2(s) | `YYYY-MM-DD HH:MM:SS` + (`.` + exactly s digits if s>0) (`cast_to_string.h:487-500`, `core/value/vdatetime_value.cpp:2327`) | s=0 `2020-01-02 00:00:00`; s=1 `…00.5`; s=3 `…11.110`; s=6 `…11.110111` |
| DATETIME (v1) | `YYYY-MM-DD HH:MM:SS` | `2026-11-03 03:51:52` |
| CHAR/VARCHAR/STRING | raw bytes, no quoting, **no CHAR padding** (`data_type_string_serde.cpp:857-870`) | CHAR(3) `H`, `` (empty), `仓` |
| NULL | `0xFB` cell | |
| (beyond M4) TIMESTAMPTZ uses the query `time_zone` (`2000-01-01 00:00:00+08:00`); IPV4/6 text; TIMEV2 `-24:00:00`; ARRAY/MAP/STRUCT/JSON in their own text forms |

Strings inside nested types are quoted (`_nesting_level > 1`). Top-level values are never quoted.

---

## 4. `update_cache` / `fetch_cache` (FE SQL cache)

* `update_cache` (4609 calls in **679 gate suites**): `PUpdateCacheRequest{sql_key PUniqueId (md5 of the SQL),
  cache_type SQL_CACHE(1), values [1 × PCacheValue{param{partition_key = latest partition id, last_version,
  last_version_time, partition_num}, data_size = Σ row bytes, rows = the exact lenenc row bytes from §3.2}]}`. Empty results send
  `rows []`, `data_size 0`. It arrives after the query finishes. Reply: always `PCacheResponse{status CACHE_OK(1)}` (2 bytes)
  (`service/internal_service.cpp:1441-1452`, `runtime/cache/result_cache.cpp:86-116`).
* `fetch_cache` (2139 calls, 43 gate suites): `{sql_key, params[{partition_key, last_version, last_version_time, partition_num}]}`.
  The corpus has only hits: `{status CACHE_OK, values [1]}`. On a hit **the FE never sends `exec_plan_fragment`**.
  Unknown key → `NO_SQL_KEY(4)`; no data → `EMPTY_DATA(8)` (`result_cache.cpp:122-155`). The FE treats any status ≠ CACHE_OK as a miss
  and executes the query (`fe/.../qe/cache/SqlCache.java:103-115`, `CacheBeProxy.java:57-60,132-136`).
* **Minimal correct doris-rs**: `update_cache` → CACHE_OK and drop the data; `fetch_cache` → NO_SQL_KEY. Results stay the same, with
  extra exec traffic. Caching for real means storing the rows and matching on sql_key plus params (`runtime/cache/result_node.cpp`).

---

## 5. Sort, limit, offset, topn, MATERIALIZATION

### 5.1 SORT_NODE (`TSortNode`, gen `plan_nodes.rs:11288`, thrift `PlanNodes.thrift:1372-1388`)

`sort_info{ordering_exprs, is_asc_order[], nulls_first[], sort_tuple_slot_exprs (materialize the sort tuple from the
child tuple)}`, `use_top_n`, `offset`, `algorithm` (HEAP_SORT 0 / TOPN_SORT 1 / FULL_SORT 2; dispatched at
`exec/operator/sort_sink_operator.cpp:53-78`), `merge_by_exchange`, `use_local_merge`.

| combination | nodes |
|---|---|
| full sort (`use_top_n=false`, FULL_SORT, merge_by_exchange) | 5553 |
| top-n HEAP_SORT, limit, merge_by_exchange | 929 (+50 with `use_topn_opt`) |
| top-n FULL_SORT, limit | 435 |
| sort without merge exchange (analytic/inner sorts) | 433 + ≈25 |
| `use_two_phase_read` (legacy two-phase fetch, `TResultSink.fetch_option`) | 3 |

`nulls_first` comes per key and is **independent of direction**. Common patterns: `[true]/asc[true]` 3018,
`[false]/asc[false]` 459, `[false]/asc[true]` 47 (ASC NULLS LAST), `[true]/asc[false]` 9.

**Limit/offset placement** (`correctness_p0.test_merge_topn_offset`, `… ORDER BY k LIMIT 3 OFFSET 4`): the per-instance
`SORT_NODE{limit = limit+offset = 5 (here capped by the inner LIMIT 5), offset 0}` → `DATA_STREAM_SINK is_merge` →
`EXCHANGE_NODE{limit 1, offset 4, sort_info}`. **The offset is applied once, at the merging exchange.** The
`OLAP_SCAN_NODE` gets `sort_info` + `sort_limit` (= sort limit), a hint the scan may use to stop early; it can be ignored.
LIMIT is also set directly on scans (≈60 scan nodes) and on plain exchanges (674).

### 5.2 MATERIALIZATION_NODE (Doris 4.x top-n lazy materialization)

This is the third most common shape (258 gate suites, 290 queries): `SELECT * … ORDER BY k LIMIT n`.
`F0: SORT(OLAP_SCAN) → DATA_STREAM(merge)`, `F1: MATERIALIZATION(EXCHANGE merge) → RESULT`. Example
`datatype_p0.scalar_types.sql.dup_key_topn_q01_asc` (`SELECT * … ORDER BY c_datetimev2 LIMIT 30`):

* The scan projects **only the sort key and a row-id column**: `proj [SLOT_REF c_datetimev2, SLOT_REF __DORIS_GLOBAL_ROWID_COL__<table>]`.
  The row-id slot is `STRING`, `col_unique_id 2147483647`, not null. The scan's `columns_desc` also lists `__DORIS_ROW_STORE_COL__`
  (hidden STRING, uid 17) when the table has a row store.
* `TMaterializationNode{tuple_id 3, intermediate_tuple_id 3, fetch_expr_lists [SLOT_REF __DORIS_GLOBAL_ROWID_COL__…],
  column_descs_lists [[16 TColumn: the remaining columns]], slot_locs_lists [[1..16]], column_idxs_lists [[1..16]],
  fetch_row_stores [true|false] (166 true / 137 false), gc_id_map true (always), nodes_info [{id <this BE>, 127.0.0.1, 8060}]}`,
  with projections putting the columns in output order.
* C++ (`exec/operator/materialization_opertor.cpp:600-628, 690-730`): for each `nodes_info` entry it sends a
  **`multiget_data_v2` brpc, to itself too** (`PMultiGetRequestV2{request_block_descs [{fetch_row_store, slots,
  column_descs, file_id[], row_id[], column_idxs, row_location_version}], query_id, gc_id_map, be_exec_version}`),
  then merges the responses back into the original row order. The row id is `GlobalRowLoacationV2{u8 version, 3 reserved,
  u32, i64 backend_id, u32 file_id, u32 row_id}` (`storage/utils.h:270-310`). Its `file_id` points into a per-query
  id→(rowset, segment) map (`gc_id_map`, `storage/id_manager.h`). The corpus has **zero `multiget_data_v2`** (BE→BE).
* **Scope for doris-rs**: the row id never leaves the BE, so its format is private. The scan emits a row locator per row
  (tablet/rowset/segment/ordinal); MATERIALIZATION looks up the listed columns for the surviving ≤ limit rows in
  process, from columns or from the row store, and keeps the order. Reading by ordinal is the segment researcher's part.
  A shortcut is to have the scan read all columns, carrying them through the sort, and then just project; but the
  plan's tuples/slots only carry the key + row id, so the node must still map `slot_locs` to output slots.

`push_down_agg_type_opt` COUNT (514 scans) / MINMAX (193) / COUNT_ON_INDEX (7) lets C++ skip column reads
(`storage/segment/segment.cpp:468-472`). The aggregation above still counts rows, so ignoring it is correct.

---

## 6. Shapes in the gate (scoping)

Shapes are written as fragments in fragment_id order, `tree->sink`. These counts are over all 1456 corpus suites; all are gate suites.

| # | shape | suites | queries | example |
|---|---|---|---|---|
| A | `OLAP_SCAN->RESULT` (1 frag) | 719 | 12928 | **10837 are FE stats-cache reads of `__internal_schema.column_statistics` (UNIQUE MoW, 4 conjuncts, 15 cols), in 707 suites, all returning empty**; 2091 are user queries |
| B | `SORT(OLAP_SCAN)->DS(merge) \| EXCHANGE->RESULT` | 471 | 3752 | withSimpleLimit, like_backslash_9 |
| C | `SORT(OLAP_SCAN)->DS(merge) \| MATERIALIZATION(EXCHANGE)->RESULT` | 258 | 290 | dup_key_topn_q01_asc |
| D | `AGG(OLAP_SCAN)->DS \| AGG(EXCHANGE)->RESULT` (2-phase agg: `need_finalize` false/true, `is_first_phase`) | 171 | 1762 | dup_bitmap_index_q02_tinyint |
| E | `UNION->RESULT` (const select) | 118 | 4768 | |
| F | `SCHEMA_SCAN->RESULT` | 85 | 1074 | |
| G | `OLAP_SCAN->DS \| SORT(HASH_JOIN(OLAP_SCAN,EXCHANGE))->DS(merge) \| EXCHANGE->RESULT` | 62 | 561 | |
| H | `EMPTY_SET->RESULT` | 58 | 1421 | |
| I | `AGG(OLAP)->DS(HASH) \| SORT(AGG(EXCH))->DS(merge) \| EXCH->RESULT` (GROUP BY … ORDER BY) | 39 | 194 | |
| J | `SORT(AGG(OLAP_SCAN))->DS(merge) \| EXCH->RESULT` | 25 | 115 | |
| K | `AGG(EMPTY_SET)->RESULT` | 23 | 807 | |
| | 560 distinct shapes in total; the remaining ones are joins, windows, set ops and multi-cast | | | |

Suites **whose every result query** has a shape in the set: {A} 91, {A,B} 391, {A,B,C} 640, {A..D} 730, {A..D,E,H,K}
837 (of the 1330 suites with result queries). Suites whose queries use only OLAP_SCAN/EXCHANGE/SORT/AGGREGATION/MATERIALIZATION/UNION/EMPTY_SET
nodes: 1030. (Shape is necessary, not sufficient: expressions, functions and storage reads also gate a suite.)
Fragments per result query: 1: 21328, 2: 6944, 3: 1722, 4: 944, 5: 568, 6-8: 515, ≥9: 36. Node totals in result plans: OLAP_SCAN
28908, EXCHANGE 19502, AGGREGATION 10320, SORT 7427, UNION 5249, HASH_JOIN 4641, EMPTY_SET 2313, SCHEMA_SCAN 1163, CROSS_JOIN
1127, ANALYTIC 505, META_SCAN 461, SELECT 357, MATERIALIZATION 303. Scans by key type: UNIQUE 12.6k (mostly the stats
table), DUP 11.7k, AGG 4.6k. Agg functions in D: `count` (most), `count/ndv/min/max` (FE column stats), `sum`, `min/max`.

RPC presence per gate suite (needs index): fetch_data 1327, cancel_plan_fragment 1349, prepare/start 1197,
exec_plan_fragment 908, reportExecStatus 707 (mostly loads), **update_cache 679**, fetch_cache 43, `tablet_fetch_data` 11
(short-circuit point queries), `fold_constant_expr` 6.

---

## What doris-rs must build (M5)

1. **brpc**: `exec_plan_fragment_prepare`/`start` for queries. Build the whole query at prepare, gate execution on `start`
   when `need_wait_execution_trigger`, and share desc_tbl/globals with `is_simplified_param` fragments. Treat `cancel_plan_fragment`
   FINISHED as a no-op; on LIMIT_REACH/errors, stop and fail any parked fetch.
2. **Result buffer** keyed by query_id (or instance id when `enable_parallel_result_sink=false`), created at prepare. Long-poll
   `fetch_data`: data packets `{seq n, eos false, row_batch TBinary TResultBatch}`, then `{seq N, eos true,
   query_statistics{returned_rows}}`; failure `{status PStatus{raw code, ["(ip)[E-xxx]msg"]}}`.
3. **MySQL text row writer** per §3.2: lenenc cells, `0xFB` NULL, fmt-7 shortest float formatting, decimal scale, datetime scale.
4. **In-process exchange** (UNPARTITIONED, merge-sorted with limit/offset; HASH/RANDOM later) honoring `per_exch_num_senders`.
5. Operators for shapes A-D (+E/H/K reuse M4's const evaluator): OLAP scan of `[tablet_id, version]` with conjuncts/projections,
   SORT (full / top-n with nulls_first per key), 2-phase AGGREGATION (count/sum/min/max), MATERIALIZATION (in-process row-id fetch).
   Shape A needs MoW unique-key reads just to answer the FE's empty `column_statistics` probes.
6. **Error report**: on failure, one `reportExecStatus{status TStatus(6 or 33…), done, fragment_id, backend_num -1, finst 0/0,
   query_type SELECT, backend_id}` per fragment; nothing on success.
7. `update_cache` → CACHE_OK; `fetch_cache` → NO_SQL_KEY (or implement the cache).
