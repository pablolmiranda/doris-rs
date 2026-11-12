# Join distribution in Doris 4.1.5: what doris-rs must honour

Paths: `be/` = `.cache/doris-src/be/src`, `fe/` = `.cache/doris-src/fe/fe-core/src/main/java/org/apache/doris`, `rs/` = `doris-rs/crates`.

## 1. BUCKET_SHFFULE_HASH_PARTITIONED

**Hash and modulo (BE sender).**
- `be/exec/operator/exchange_sink_operator.cpp:142-148`: `_partition_count = channels.size()`, and the partitioner is `Crc32HashPartitioner<ShuffleChannelIds>`. It always uses zlib crc32. `enable_new_shuffle_hash_method` (crc32c) only applies to HASH_PARTITIONED (`:127-141`).
- `be/exec/partitioner/partitioner.cpp:31-66`:
  - The hash starts at 0 (`partitioner.h:150-153`).
  - For each partition expr in order, `column->update_crcs_with_value(hash, expr->root()->data_type()->get_primitive_type())` is called.
  - **Const columns are skipped** (`is_const → continue`, `partitioner.cpp:44-46`).
  - Then `hash % partition_count` (`partitioner.h:159-162`). `HashValType = uint32_t` (`partitioner.h:31`).
- Value encoding:
  - Fixed-width numerics, decimals and v2 date types: `HashUtil::zlib_crc32_fixed(value, hash)` over the in-memory `sizeof(T)` little-endian bytes (`core/column/column_vector.cpp:154-182`, `column_decimal.cpp:148-170`). This means the hash depends on the column's physical type: INT is 4 bytes, BIGINT 8, LARGEINT, DECIMAL128 and DECIMALV2 16, DECIMAL256 32.
  - v1 DATE/DATETIME hash their text from `VecDateTimeValue::to_buffer` (`column_vector.cpp:172-176`).
  - Strings hash their raw bytes (`column_string.cpp:273-295`).
  - NULL is `zlib_crc_hash_null` = crc of an int32 0, i.e. 4 zero bytes (`column_nullable.cpp:165-181`, `util/hash_util.hpp:120-124`).
- This is the same function the load path uses to place rows into buckets: `storage/tablet_info.h:253-257` (`RawValue::zlib_crc32`, with the same null rule).
- `rs/doris-exec/src/route.rs:125-137,193-216` reimplements exactly this: zlib crc32 chained from 0, per-type width, NULL as `[0;4]`, v1 dates as text, `% num_buckets`.
  - **doris-rs placed every row into its tablets with route.rs, so the bucket-shuffle sender must reuse route.rs's `hash_bytes` chain verbatim.** Matching C++ only matters because route.rs already matches it.
  - Small gaps in route.rs: its default width is 16 bytes, which is wrong for DECIMAL256 (32). Also, BE skips const partition columns, which route.rs does not model.

**Bucket count and the bucket → channel mapping.**
- The modulo is `channels.size()`, i.e. `len(TPlanFragmentDestination)` of the sink. The FE makes that list exactly `bucketNum` long, **indexed by bucket seq**:
  - `fe/nereids/trees/plans/distribute/DistributePlanner.java:218-219,237-243` calls `getDestinationsByBuckets`, which takes `bucketNum = olapScanNodes.get(0).getBucketNum()` (the natural-side table's bucket count).
  - `sortDestinationInstancesByBuckets` (`:260-287`) puts, in slot `bucketIndex`, the receiver instance whose `BucketScanSource` contains that bucket.
  - A bucket that no instance scans (pruned) gets a dummy `TUniqueId(-1,-1)` on worker `0.0.0.0:0` (`:276-283`).
- The same instance appears once per bucket it owns. BE deduplicates channels by `fragment_instance_id.lo` (`exchange_sink_operator.cpp:204-221`). Rows that hash to a `(-1,-1)` dest are **dropped** (`exec/exchange/vdata_stream_sender.cpp:107-110,131-133`). That is only safe for INNER/CROSS joins. For other bucket-shuffle joins the FE fills up missing buckets with real (empty-scan) instances (`UnassignedScanBucketOlapTableJob.java:149-156,292-310`).
- No Thrift field other than the order of `TPipelineFragmentParams.destinations` carries the mapping. `TDataStreamSink` has no bucket fields (`gensrc/thrift/DataSinks.thrift:184-212`).
- `TPipelineFragmentParams.num_buckets`, `bucket_seq_to_instance_idx` and `shuffle_idx_to_instance_idx` (`ThriftPlansBuilder.java:505-519`) describe the **receiving** fragment. BE only uses them for *local* exchange (`be/exec/pipeline/pipeline_fragment_context.cpp:797-835`).
- Senders: `per_exch_num_senders` counts sender *instances* (`ThriftPlansBuilder.java:345-350`), not destination entries.
- **doris-rs `send_stream` (`rs/doris-exec/src/pipeline.rs:582-600`) currently sends and ends once per destination entry.** For bucket shuffle it must:
  - hash with the route.rs crc % `destinations.len()`;
  - group parts by instance id and send **one** end per distinct instance;
  - skip `(-1,-1)`.

**Scan-range assignment (FE).**
- `UnassignedScanBucketOlapTableJob.java:86-160`: `selectReplicaAndWorkerWithBucket` maps bucket index → {scan node → tablets}. Bucket i is the tablet at position i of each selected partition's index (all partitions together).
- `insideMachineParallelization` splits whole buckets across instances.
- Each instance's `per_node_scan_ranges` is the union of its buckets' tablets (`ThriftPlansBuilder.java:603-615`).
- `sortDestinationInstancesByBuckets` reads the same `BucketScanSource`, so bucket i's destination is by construction the instance that scans bucket i.

**Type of the hashed value.** BE hashes the expr's own result column, so the expr's type determines the bytes. The FE keeps that type equal to the bucket column's hash type:
1. Hash-join keys are coerced to a common type, and non-slot keys (casts) are pushed into a child project as new slots (`fe/nereids/rules/rewrite/PushDownExpressionsInHashCondition.java:47-62`).
2. A project keeps the NATURAL (storage) distribution through an alias only for a bare slot, or for a cast whose hash value is unchanged. That is only string → wider varchar/string (`fe/nereids/properties/ChildOutputPropertyDeriver.java:347-350,675-682`). So `cast(int_bucket_col as bigint)` loses bucketing and becomes a plain shuffle.
3. The partition exprs are emitted in the order of the hash spec's equivalence sets (bucket-column order), as slot refs (`PhysicalPlanTranslator.java:3324-3352`). Order matters for the crc chain.

## 2. COLOCATE and BUCKET_SHUFFLE probe side

- Colocate uses the same job class. Every bucket's tablets of **all** scan nodes in the fragment go to one instance (example in `UnassignedScanBucketOlapTableJob.java:89-104,117-141`), and both tables' tablets are in `per_node_scan_ranges`.
- Colocate groups guarantee the same bucket count and the same bucket-column types, and all partitions use the same hash.
- **An instance that scans all its assigned tablets of both tables and joins them gives the correct result.** The same holds for bucket-shuffle: the natural side scans its buckets, and the other side arrives via the crc mapping above. Nothing else in C++ is needed **unless the FE chose local shuffle** (next item).
- **Local-shuffle trap.** The FE uses a "serial source" when `ignore_storage_data_distribution` (default true, `SessionVariable.java:1702`) is set and a scan is serial (`PlanFragment.java:523-534`). A scan is serial when `scanRangeNum < parallel_pipeline_task_num * numBackends` (`ScanNode.java:763-773`). With few buckets and the gate's `parallel_pipeline_task_num=4`, that is common. In that case:
  - Instances are `LocalShuffle(Bucket)AssignedJob`. **Only the first instance per BE scans the serial scan nodes for ALL buckets.** The other instances get empty scan sources, or only the non-serial scan's buckets (`UnassignedScanBucketOlapTableJob.java:163-231`, `AbstractUnassignedScanJob.java:130-160`).
  - **Remote exchanges send only to the first instance per BE** (`DistributePlanner.java:245-258`).
  - `parallel_instances=1` is set on the fragment (`ThriftPlansBuilder.java:571-590`).
  - BE then local-shuffles (BUCKET_HASH_SHUFFLE via `bucket_seq_to_instance_idx`, or HASH_SHUFFLE) to the other tasks.
  - So running each instance independently is **wrong**. Example: instance 1 holds probe buckets {2,3} while their build rows were scanned by instance 0. The exchanges of instances 2..N also never get an end, while `per_exch_num_senders` still declares N.
  - Fix for doris-rs: when `parallel_instances` is set, execute the fragment's instances on the BE as **one merged instance**: the union of all instances' scan ranges, plus the first instance's exchanges. Every instance id must still send its (empty) end to downstream exchanges, because downstream counts all instances as senders.
  - Today `check_senders` (`rs/doris-exec/src/query.rs:680-697`) would reject such plans with "expects N senders, the plan has 0".

## 3. PARTITIONED (shuffle) joins, INT vs BIGINT

- The keys are never hashed in different types. Nereids coerces `a.int = b.bigint` to `cast(a.int as bigint) = b.bigint`. `PushDownExpressionsInHashCondition` turns the cast into a project slot under the join, so both HASH_PARTITIONED sinks' `partition_exprs` are slots of the same type (`PushDownExpressionsInHashCondition.java:47-77`).
- Translation: EXECUTION_BUCKETED → HASH_PARTITIONED with slot refs (`PhysicalPlanTranslator.java:3343-3349`).
- A side can also be "already EXECUTION_BUCKETED" with no exchange of its own, e.g. an aggregate's fragment fed by a HASH_PARTITIONED exchange on the same key (`ChildrenPropertiesRegulator.java:442-500`). The join then relies on the **two different exchanges using the same hash and the same destination order**.
  - doris-rs uses one FNV everywhere, and the destination lists are the same `receiverPlan.getInstanceJobs()` order, so this is fine.
  - doris-rs's `hash_values` hashes `Datum::Int` as i64 regardless of width, so it is type-agnostic for ints anyway.
  - Watch decimals of different scale and dates vs datetimes: the FE prevents these by coercion.

## 4. Runtime filters: ignoring them is safe

- **No FE participation.**
  - `runtime_filter_merge_addr` is a **BE**: the first instance of the top fragment (`fe/qe/runtime/RuntimeFiltersThriftBuilder.java:160-163`). The FE sends `rid_to_runtime_filter`, `rid_to_target_paramv2` and `runtime_filter_builder_num` only to that BE (`ThriftPlansBuilder.java:406-421`, `RuntimeFiltersThriftBuilder.java:113-150`).
  - The RPCs `merge_filter`, `send_filter_size`, `sync_filter_size` and `apply_filterv2` are BE→BE `PBackendService` calls (`gensrc/proto/internal_service.proto:1253-1256`).
  - doris-rs is the only BE, so nobody else sends or waits for them. If it never builds, publishes or waits, no one blocks: consumers in C++ wait only up to `runtime_filter_wait_time_ms`, and that is on the BE side.
- **No semantic dependence.**
  - Join eq/other conjuncts stay on the join node. Runtime filters are extra conjuncts on scans and exchanges.
  - `null_aware` IN filters (for `<=>` and NAAJ) exist only so that the *filter* does not drop null-matching rows. The join itself still implements the semantics.
  - The only BE behaviour that *depends* on a filter is `left_semi_direct_return`: a left-semi join with a single conjunct and an exact local IN filter skips the probe (`be/exec/operator/hashjoin_build_sink.cpp:282-291`, `hashjoin_probe_operator.cpp:251-256`). Without filters, a normal semi-join gives the same result.
  - TopN filters (`topn_filter_descs`) are also pure pruning.
- **Result: ignoring all runtime filters gives identical results.**
- One cosmetic point: `pruneBroadcastRuntimeFilterProducers` may strip filter descs from some instances' plans (`RuntimeFiltersThriftBuilder.java:80-108`). This does not matter if they are ignored.

## 5. Other fragment params and broadcast build

- **Broadcast hash join build: shared, not one stream per instance.**
  - With `enable_share_hash_table_for_broadcast_join` (TQueryOptions field 54; FE default true, `SessionVariable.java:2337-2338`, fuzzy in tests), the build exchange is marked `isRightChildOfBroadcastHashJoin` (`PhysicalPlanTranslator.java:1820-1826`). Its UNPARTITIONED sender gets **only the first instance per BE** as destination (`DistributePlanner.java:253-254`).
  - BE: only `task_idx==0` builds. The other tasks block their build source and reuse the shared hash table (`hashjoin_build_sink.cpp:58-76,798-799`; `pipeline_fragment_context.cpp:1552-1565`; `PASS_TO_ONE` local exchange `:856-866`).
  - **doris-rs must give the build chunk received by the first instance to every instance of that fragment.** Otherwise instances 2..N inner-join against an empty build and lose rows. Their build exchange receives nothing and must not wait.
  - `per_exch_num_senders` for that exchange counts sender instances once.
  - When the option is false, every instance is a destination (normal UNPARTITIONED copy).
- **CROSS_JOIN_NODE** (nested loop): not flagged as shared, so its UNPARTITIONED build goes to every instance (unless local shuffle applies). No shared table: `nested_loop_join_build_operator.cpp` has no `task_idx` logic.
- **`parallel_instances` (field 37):** the "ignore data distribution / serial source" switch. See §2: merge the instances.
- **`num_buckets` (34), `bucket_seq_to_instance_idx` (35), `shuffle_idx_to_instance_idx` (39), `total_instances` (38):** local-exchange only. They are not needed if doris-rs merges instances under local shuffle and runs each instance on its own otherwise.
- **`per_node_shared_scans` (36, instance 7)** and **`local_params[].build_hash_table_for_broadcast_join` (instance field 2):** deprecated and not set by the 4.1.5 FE (`gensrc/thrift/PaloInternalService.thrift:716,721,765`).
- **`TPlanNode.is_serial_operator` (51):** set only together with serial source (`PlanNode.java:568`, `ExchangeNode.java:117-118`). It is consistent with `parallel_instances`.
- **OLAP_TABLE_SINK_HASH_PARTITIONED:** channel = `zlib_crc(low 4 bytes of tablet_id, 0) % channels` (`be/exec/sink/tablet_sink_hash_partitioner.cpp:107-111`). Since every receiving instance routes rows itself, any split is correct for doris-rs.
- **Sink `conjuncts`:** `TDataStreamSink.conjuncts` must be applied, and doris-rs already checks them (`query.rs:721-727`). `TDataStreamSink.runtime_filters` can be ignored like the other runtime filters.
