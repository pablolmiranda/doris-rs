# Java-dependent features in Doris 4.1.5 and their Rust alternatives

Research for issue #18 (child of map #14, "doris-rs: the route to one Rust binary").

**Question.** Which Doris 4.1.5 features run Java in the BE (JNI) or need Java-only
libraries in the FE? For each one, is there a usable Rust library? The answer lets the
roadmap mark each feature as Rust or "not supported".

**Constraints (already decided).** doris-rs is 100% Rust. It never embeds a JVM, not even
as an option. A feature becomes Rust when a reasonable Rust library exists. Otherwise it
fails with a clear "not supported" error. Java UDFs are a recorded migration gap.

**Sources.**
- Upstream Doris at tag `4.1.5-release` (commit `917b20be83878e98f94e3ebf7efccc80346e3e50`).
  Paths below are relative to that tree (`.cache/doris-src`).
- crates.io API metadata and GitHub READMEs and source trees, all read on 2026-11-04.
  Versions and dates are as of that day.

## Short answer

- **Doris already uses Rust for some lake reads.** Paimon reads go through `paimon-rust`
  over C FFI, and Lance reads go through `lance-c`. In both cases the JVM path is now only
  a fallback.
- **Rust now (mature libraries exist):** Iceberg, Hudi and Paimon reads, including
  metadata and the main catalogs. Also HDFS without Kerberos, Avro, Lance, LDAP, and JDBC
  catalogs for MySQL, PostgreSQL, SQL Server, ClickHouse, SAP HANA and Trino.
- **Rust later (a library exists but has gaps):** Iceberg row-level writes and maintenance
  actions, Paimon writes, Kerberos (HDFS and HMS), Hive ACID, CDC streaming jobs, LakeSoul,
  Oracle and DB2.
- **Not supported (no Rust equivalent, or the feature is "run arbitrary Java"):**
  - Java UDF, UDAF and UDTF.
  - The Trino-connector catalog.
  - MaxCompute.
  - Alibaba DLF metastores.
  - JDBC with a user-supplied driver jar.
  - Ranger authorization.
  - Java FE plugins: audit loaders and the dialect converters.
  - Pluggable Java TVF writers.
  - The Java HDFS broker.

## Where Java runs in 4.1.5

### BE: classes loaded over JNI

These are the class names the BE C++ code loads. They come from string literals in
`be/src` (`grep '"org/apache/doris/...'`):

| JNI class (in `fe/be-java-extensions/`) | BE caller | Feature |
|---|---|---|
| `udf/UdfExecutor`, `udf/UdafExecutor` (`java-udf`) | `be/src/exprs/function/function_java_udf.cpp`, `be/src/exprs/aggregate/aggregate_function_java_udaf.h`, `be/src/exprs/table_function/udf_table_function.cpp` | Java UDF, UDAF and UDTF |
| `jdbc/JdbcJniScanner`, `jdbc/JdbcExecutorFactory` (`jdbc-scanner`; executors for MySQL, PG, Oracle, SQL Server, ClickHouse, DB2, SAP HANA, Trino, Gbase) | `be/src/exec/connector/vjdbc_connector.cpp`, `be/src/exec/sink/writer/vjdbc_table_writer.cpp` | JDBC catalog reads and `INSERT` into JDBC |
| `hudi/HadoopHudiJniScanner` (`hadoop-hudi-scanner`) | `be/src/format/table/hudi_jni_reader.cpp` | Hudi MOR snapshot reads |
| `paimon/PaimonJniScanner`, `paimon/PaimonJniWriter` (`paimon-connector`) | `be/src/format/table/paimon_jni_reader.cpp`, `be/src/exec/sink/writer/paimon/jni_paimon_write_backend.cpp` | Paimon fallback reads; all Paimon writes |
| `iceberg/IcebergSysTableJniScanner` (`iceberg-metadata-scanner`) | `be/src/format/table/iceberg_sys_table_jni_reader.cpp` | Iceberg system tables (`$snapshots`, `$history`, `$manifests`, `$partitions`, `$refs`, …) |
| `maxcompute/MaxComputeJniScanner`, `MaxComputeJniWriter` (`max-compute-connector`) | `be/src/format/table/max_compute_jni_reader.cpp`, `be/src/exec/sink/writer/maxcompute/` | MaxCompute read and write |
| `trinoconnector/TrinoConnectorJniScanner`, `TrinoConnectorPluginLoader` (`trino-connector-scanner`) | `be/src/format/table/trino_connector_jni_reader.cpp` | `trino-connector` catalog |
| `lakesoul/LakeSoulJniScanner` (`lakesoul-scanner`) | `be/src/format/table/lakesoul_jni_reader.cpp` | LakeSoul catalog |
| `avro/AvroJNIScanner` (`avro-scanner`) | `be/src/format/avro/avro_jni_reader.cpp`, `be/src/exec/scan/file_scanner.cpp` (`FORMAT_AVRO`) | Avro files in TVF and load |
| `writer/LocalFileJniWriter` (`java-writer`), plus any `tvf_sink.writer_class` | `be/src/format/transformer/vjni_format_transformer.cpp` | `INSERT INTO` a TVF through a pluggable Java writer |

There are two other JVM users in the BE.

**HDFS through Hadoop `libhdfs` (JNI).** On Linux amd64 and aarch64 (the release
platforms), `be/CMakeLists.txt` (lines 590–611) links Hadoop 3.4's `libhdfs.a` and defines
`USE_HADOOP_HDFS`. Every other platform uses the C++ `libhdfs3`. So production BE HDFS
reads, HDFS writes and Kerberos login run inside the JVM. See
`be/src/io/hdfs_builder.cpp:231` (`hdfsBuilderSetKerb5Conf`, `hdfsBuilderSetKeyTabFile`)
and the JVM-heap backpressure in `be/src/io/fs/hdfs_file_writer.cpp`.

**JVM housekeeping.** `be/src/common/metrics/jvm_metrics.cpp` exports JVM metrics.
`be/src/agent/task_worker_pool.cpp:2591` cleans the UDF classloader cache. Neither has a
counterpart once there is no JVM.

### BE-launched Java sidecar

The BE starts `fs_brokers/cdc_client`, a Java service built on Flink CDC
(`flink-connector-mysql-cdc` and `flink-connector-postgres-cdc`). See
`be/src/runtime/cdc_client_mgr.cpp` and `be/src/common/config.cpp:77-79`
(`cdc_client_port`, `cdc_client_java_opts`). It feeds the `cdc_stream` TVF and streaming
insert jobs (`fe/fe-core/.../tablefunction/CdcStreamTableValuedFunction.java`). The older
`fs_brokers/apache_hdfs_broker` is a separate Java process used by broker load.

### Native paths Doris already has (no JVM)

These show which formats need no Java at all:

**Hudi.** The FE sends COW tables, and MOR splits that have no log files, to the native
Parquet and ORC reader. Only MOR splits that have log files use JNI. See
`fe/fe-core/.../datasource/hudi/source/HudiScanNode.java`, lines 176–197, 319–328 and 404.

**Paimon** has three read tiers:
- `NATIVE`: C++ Parquet and ORC, for splits that can be converted to raw files.
- `PAIMON_RUST`: Doris pins `apache/paimon-rust` `v0.4.0-rc1` in `thirdparty/vars.sh:589-592`
  and reads through `be/src/format_v2/table/paimon_rust_table_reader.cpp` (`#include "paimon_rust/paimon.h"`).
- JNI: the fallback when a split falls outside what Rust has verified.

`fe/.../paimon/source/PaimonRustReaderCapabilities.java` and `PaimonScanNode.java` (lines
152–175 and 1051) define what Rust may read:
- Storage schemes: `s3`, `s3a`, `oss`, `hdfs` and `file` only.
- HDFS without Kerberos only.
- Primary-key splits with only one data file and no deletes.
- Supported merge engines and aggregates only.

Doris has also reserved an `FfiPaimonWriteBackend`, described in
`be/src/exec/sink/writer/paimon/ffi_paimon_write_backend.h` as a "placeholder for the
future paimon-rust writer". So upstream Doris is itself moving Paimon off the JVM.

**Iceberg** data files (Parquet and ORC, with position and equality deletes) are read and
written natively (`be/src/exec/sink/writer/iceberg/`). Only the system tables use JNI.

**Lance** is read through `lance-c`, the C ABI over the Rust `lance` crate
(`thirdparty/vars.sh:584`, `be/src/format_v2/table/lance_reader.cpp`).

**Kafka** routine load uses `librdkafka` in the BE. The FE does not import Kafka
classes. **Elasticsearch** is REST only. Neither involves Java.

### FE: Java-only libraries

The whole Doris FE is Java. Here we list only the FE dependencies whose job a Rust FE
must replace with a library, not with ordinary code. Sources are `fe/fe-core/pom.xml` and
the catalog types in `fe/fe-core/.../datasource/CatalogFactory.java:90-119`, which are
`hms`, `es`, `jdbc`, `iceberg`, `paimon`, `lance`, `trino-connector`, `max_compute`,
`lakesoul` and `doris`.

- **HMS catalog:** the Hive metastore Thrift client (`hive-catalog-shade`,
  `ThriftHMSCachedClient.java`), plus Hadoop UGI for Kerberos
  (`fe/fe-common/.../security/authentication/HadoopKerberosAuthenticator.java`). It also
  covers Hive ACID (`AcidUtil.java`, `HiveTransactionMgr.java`).
- **Iceberg:** `iceberg-core` and `iceberg-aws`. The catalog flavours are
  HMS, Glue, REST, Hadoop, DLF, JDBC and S3 Tables (`datasource/iceberg/Iceberg*ExternalCatalog.java`).
  Maintenance actions are expire snapshots, rewrite data files, rewrite manifests,
  rollback, cherry-pick, fast-forward and others (`datasource/iceberg/action/`).
  Row-level `DELETE`, `UPDATE` and `MERGE` are in `IcebergMergeOperation.java`.
- **Paimon:** `paimon-core` handles planning and commits. The catalog flavours are
  filesystem, HMS, DLF and REST (`datasource/paimon/Paimon*ExternalCatalog.java`).
- **Hudi:** `hudi-common` and `hudi-hadoop-mr` handle the timeline and file-slice
  listing. Hudi tables are reached through the HMS catalog.
- **JDBC:** the user supplies a driver jar, which is loaded with `URLClassLoader`
  (`datasource/jdbc/client/JdbcClient.java`, `catalog/JdbcResource.java`). There are
  dialect clients for MySQL, MariaDB, OceanBase, PostgreSQL, Oracle, SQL Server,
  ClickHouse, SAP HANA, Trino, Presto, DB2 and Gbase (`datasource/jdbc/client/`).
  Iceberg and Paimon JDBC metastores also load JDBC drivers.
- **MaxCompute:** `odps-sdk-core` and `odps-sdk-table-api`.
- **Trino connector:** `trino-main` loads Trino connector plugins through the Trino SPI.
- **Cloud SDKs:** the AWS Java SDK (S3, Glue, STS, DynamoDB), Azure, Alibaba OSS and DLF,
  Tencent COS, Huawei OBS and Baidu BCE.
- **Security:**
  - Ranger: `ranger-plugins-common`, used by 8 files under `catalog/authorizer/ranger`.
  - LDAP: `spring-boot-starter-data-ldap`, in `mysql/authenticate/ldap/LdapClient.java`.
- **Plugins:**
  - Java UDF jars are loaded and validated by `CreateFunctionCommand.java`.
  - Dynamic FE plugins (`plugin/DynamicPluginLoader.java`): audit loaders in `fe_plugins/`.
  - SQL-dialect converters (`fe_plugins/trino-converter`, `sparksql-converter`; session
    variable `sql_dialect`).

## Rust libraries checked

All figures are from the crates.io API, read on 2026-11-04.

| Crate | Latest | Last publish | Downloads | Repo |
|---|---|---|---|---|
| `iceberg` (+ `iceberg-catalog-{rest,hms,glue,sql,s3tables}`) | 0.11.0 | 2026-11-03 | 4.6M (rest 3.2M) | apache/iceberg-rust |
| `paimon` | 0.4.0 | 2026-10-12 | 14k | apache/paimon-rust |
| `hudi` | 0.6.0 | 2026-11-03 | 14k | apache/hudi-rs |
| `deltalake` | 1.1.2 | 2026-10-17 | 3.6M | delta-io/delta.rs |
| `lance` | 14.0.2 | 2026-10-29 | 4.8M | lance-format/lance |
| `opendal` | 0.59.5 | 2026-10-18 | 19.7M | apache/opendal |
| `object_store` | 0.14.2 | 2026-09-15 | 105.6M | apache/arrow-rs-object-store |
| `hdfs-native` | 0.14.8 | 2026-11-02 | 1.8M | Kimahriman/hdfs-native |
| `hive_metastore` | 0.2.0 | 2025-04-15 | 536k | Xuanwo/hive_metastore_rs |
| `aws-sdk-glue` | 1.182.0 | 2026-10-30 | 2.2M | awslabs/aws-sdk-rust |
| `apache-avro` | 0.22.0 | 2026-11-02 | 27.9M | apache/avro-rs |
| `orc-rust` | 0.9.0 | 2026-09-02 | 658k | datafusion-contrib/orc-rust |
| `sqlx` | 0.9.0 | 2026-05-21 | 181M | launchbadge/sqlx |
| `mysql_async` | 0.37.1 | 2026-09-01 | 8.6M | blackbeam/mysql_async |
| `tokio-postgres` | 0.7.18 | 2026-06-12 | 83M | rust-postgres/rust-postgres |
| `tiberius` (SQL Server) | 0.13.0 | 2026-09-25 | 5.8M | prisma/tiberius |
| `clickhouse` | 0.15.3 | 2026-10-13 | 16.5M | ClickHouse/clickhouse-rs |
| `hdbconnect` (SAP HANA, pure Rust) | 0.33.0 | 2026-10-01 | 158k | emabee/rust-hdbconnect |
| `trino-rust-client` | 0.12.0 | 2026-08-19 | 659k | nudibranches-tech/trino-rust-client |
| `oracle` (wraps ODPI-C, needs Oracle Instant Client) | 0.6.3 | 2025-01-02 | 3.5M | kubo/rust-oracle |
| `odbc-api` / `arrow-odbc` | 29.3.0 / 26.0.4 | 2026-11-01/03 | 1.8M / 586k | pacman82 |
| `connectorx` | 0.4.6 | 2026-09-18 | 125k | sfu-db/connector-x |
| `datafusion-table-providers` | 0.15.1 | 2026-10-29 | 218k | datafusion-contrib |
| `libgssapi` / `cross-krb5` | 0.11.0 / 0.5.0 | 2026-05-30/31 | 1.6M / 1.3M | estokes |
| `sspi` (pure-Rust Kerberos/NTLM) | 0.23.1 | 2026-10-27 | 2.0M | devolutions/sspi-rs |
| `ldap3` | 0.12.1 | 2025-09-18 | 5.4M | inejge/ldap3 |
| `pgwire-replication` | 0.5.0 | 2026-10-19 | 323k | vnvo/pgwire-replication |
| `mysql-binlog-connector-rust` | 0.3.3 | 2025-10-31 | 17k | apecloud/mysql-binlog-connector-rust |
| `wasmtime` | 50.0.1 | 2026-10-29 | 43M | bytecodealliance/wasmtime |
| `sqlparser` | 0.63.0 | 2026-09-13 | 89M | apache/datafusion-sqlparser-rs |

Crates that do not exist on crates.io: `lakesoul`, `lakesoul-io`, `odps`, `maxcompute`.
LakeSoul's Rust core lives in the `rust/` workspace of lakesoul-io/LakeSoul (`lakesoul-io`,
`lakesoul-metadata`, …) but is not published.

### Capability notes (from each project's README and source tree)

**iceberg-rust**
- Catalogs: REST, HMS, Glue, SQL and S3 Tables (crate list in the README).
- Storage: OpenDAL with S3, GCS, OSS, Azure DLS and `hdfs-native` features
  (`crates/storage/opendal/Cargo.toml`).
- Transactions (`crates/iceberg/src/transaction/`): `append`, `expire_snapshots`,
  update schema, properties, location and sort order, upgrade format version, and update
  statistics.
- Writers include position-delete and equality-delete writers
  (`writer/base_writer/`), and reads apply delete files (`arrow/delete_filter.rs`).
- Metadata tables (`inspect/`): `snapshots`, `history` and `manifests` only.
- **Gaps:**
  - No overwrite, row-delta or replace commits, so no Iceberg `INSERT OVERWRITE`,
    `DELETE`, `UPDATE` or `MERGE` commits yet.
  - No rewrite-data-files, rewrite-manifests or rollback actions.
  - No Hadoop (filesystem) catalog.
  - Doris's other metadata tables (`$partitions`, `$refs`, `$all_*`, `$position_deletes`,
    `$metadata_log_entries`) are missing.

**hudi-rs** (README feature table)
- COW and MOR tables.
- Read table versions 6, 8 and 9 (Hudi 0.14 and later).
- Queries: snapshot, time travel, incremental and read-optimized.
- Writes for table versions 8 and 9.
- Storage: local, S3, Azure and GCS.
- **Gaps:**
  - No HDFS storage is listed.
  - No HMS-based table discovery. Doris finds Hudi tables through HMS, so doris-rs pairs
    `hive_metastore` with `hudi` itself.

**paimon-rust** (docs `index.md`, `crates/paimon/src/catalog/`)
- Native reader.
- Storage: FS, S3, OSS, COS, Azure, OBS, GCS and HDFS.
- Catalogs: filesystem and REST.
- DataFusion integration. Status: "under active development".
- Doris has already integrated it (above). Its own FE capability gate lists today's read
  gaps:
  - Multi-file primary-key merges.
  - Files with deletes.
  - Some merge engines and aggregates.
  - Pruned nested projections.
  - Kerberized or HA HDFS.
- **Gaps:**
  - No HMS or DLF Paimon catalog.
  - Writes are not wired into Doris yet.

**hdfs-native**
- Covers name services and HA, ViewFS, router federation and erasure coding (RS only).
- Authentication: Kerberos (GSSAPI SASL) and delegation tokens (DIGEST-MD5).
- Encryption: data transfer encryption, and KMS for encryption at rest (feature `kms`).
- Kerberos works by dynamically linking the system `libgssapi_krb5` at runtime. That is a
  C library, not a JVM. Doris's BE already links MIT `krb5` (`thirdparty/vars.sh:392`).

**OpenDAL.** `services-hdfs` wraps Hadoop's `libhdfs`, so it needs a JVM.
**doris-rs must use `services-hdfs-native`** or `hdfs-native` directly. Never use
`services-hdfs`.

**hive_metastore** is a Volo-Thrift client and is what `iceberg-catalog-hms` uses. Its
README documents no SASL or Kerberos transport.

## Decision table

Recommendation key:
- **Rust now:** ship in the first lakehouse epic.
- **Rust later:** library exists with named gaps; return "not supported" until filled.
- **Not supported:** permanent clear error.

| Feature | Where Java runs in Doris 4.1.5 | Rust option | Maturity | Recommendation |
|---|---|---|---|---|
| Java UDF / UDAF / UDTF | BE JNI `UdfExecutor`/`UdafExecutor`; FE loads jar (`CreateFunctionCommand`) | None for Java. A replacement extension point could be RPC UDFs (Doris already has gRPC `function_rpc.cpp`, language-neutral) or WASM (`wasmtime` 50) | n/a | **Not supported.** `CREATE FUNCTION ... "type"="JAVA_UDF"` errors. Recorded migration gap. Consider RPC/WASM UDFs as a separate feature. |
| JDBC catalog: MySQL / MariaDB / OceanBase(MySQL mode) | BE `JdbcJniScanner` + FE JDBC driver | `mysql_async` / `sqlx` | mature | **Rust now** |
| JDBC catalog: PostgreSQL | same | `tokio-postgres` / `sqlx` | mature | **Rust now** |
| JDBC catalog: SQL Server | same | `tiberius` 0.13 | mature, Prisma-maintained | **Rust now** |
| JDBC catalog: ClickHouse | same | `clickhouse` 0.15 (official) | mature | **Rust now** |
| JDBC catalog: SAP HANA | same | `hdbconnect` 0.33 (pure Rust, sync + async variant) | usable, single maintainer | **Rust later** |
| JDBC catalog: Trino / Presto | same | `trino-rust-client` 0.12, `prusto` 0.5 (last publish 2024-12) | usable, HTTP client | **Rust later** |
| JDBC catalog: Oracle | same | `oracle` 0.6 needs Oracle Instant Client C library at runtime; last publish 2025-01 | mature but not pure Rust | **Rust later**, if a dlopen'd vendor C lib is accepted; else not supported |
| JDBC catalog: DB2, Gbase | same | Only ODBC (`odbc-api`), which needs vendor C drivers | n/a | **Not supported** |
| JDBC with arbitrary user driver jar (`driver_url`) | FE/BE `URLClassLoader` | impossible by definition | n/a | **Not supported**. JDBC catalog becomes a fixed set of typed drivers. |
| `INSERT` into JDBC tables | BE `vjdbc_table_writer` via JNI | same drivers as reads | as above | follows read row |
| Hive (HMS) catalog: metadata | FE Hive Thrift client | `hive_metastore` 0.2 (Volo Thrift) | works (used by iceberg-rust), thin, last publish 2025-04 | **Rust now** for simple auth. Kerberos SASL: see Kerberos row. |
| Hive table data read/write | Native C++ (Parquet/ORC/text) | `parquet`/`arrow` (arrow-rs), `orc-rust` 0.9 | Parquet mature; ORC usable | **Rust now** (Parquet, text); ORC **Rust now** with conformance tests |
| Hive ACID (transactional) tables | FE `AcidUtil`/`HiveTransactionMgr` + native read | No library; must hand-write delta/base merge | n/a | **Rust later** (not supported initially) |
| HDFS access (no Kerberos) | BE Hadoop `libhdfs` JNI on Linux release builds; FE Hadoop client | `hdfs-native` 0.14.8 (via `opendal` `services-hdfs-native`) | mature: HA, ViewFS, EC, tokens, encryption | **Rust now** |
| Kerberos for HDFS | BE `libhdfs` keytab login; FE Hadoop UGI | `hdfs-native` Kerberos via runtime `libgssapi_krb5`; `sspi` 0.23 (pure-Rust Kerberos client) | hdfs-native: supported; sspi: not proven against Hadoop SASL | **Rust later**. Needs a decision on whether a dlopen'd system `libgssapi_krb5` fits "100% Rust". |
| Kerberos for HMS (SASL over Thrift) | FE Hadoop UGI | None in `hive_metastore`; would need SASL/GSSAPI over Volo (`libgssapi`/`cross-krb5`) | gap | **Rust later** |
| Java HDFS broker (`apache_hdfs_broker`, broker load) | separate Java process | not needed: BE-local `hdfs-native`/`opendal` | n/a | **Not supported** as a separate process. Broker load maps onto native storage access. |
| Iceberg: catalog + scan planning | FE `iceberg-core` | `iceberg` 0.11 + REST/HMS/Glue/SQL/S3Tables catalogs | most mature lake crate; Apache project | **Rust now** (REST, HMS, Glue, JDBC→`iceberg-catalog-sql`, S3 Tables) |
| Iceberg: Hadoop (filesystem) catalog, DLF catalog | FE `iceberg-core`, DLF SDK | none in iceberg-rust; no Rust DLF SDK | gap | Hadoop: **Rust later** (small, writable). DLF: **not supported**. |
| Iceberg: data read (incl. delete files) | Native C++ | `iceberg` ArrowReader with delete filter | mature | **Rust now** |
| Iceberg: `INSERT` (append) | Native writer + FE commit | `iceberg` `fast_append` + data-file writer | mature | **Rust now** |
| Iceberg: `INSERT OVERWRITE`, `DELETE`/`UPDATE`/`MERGE` | Native writer + FE `iceberg-core` commit | Delete-file writers exist; no overwrite/row-delta transaction yet | gap | **Rust later** |
| Iceberg maintenance actions (rewrite data files/manifests, rollback, cherry-pick…) | FE `iceberg-core` | only `expire_snapshots` | gap | **Rust later** (`expire_snapshots` now) |
| Iceberg system tables | BE JNI `IcebergSysTableJniScanner` | `iceberg::inspect`: snapshots, history, manifests | partial | **Rust now** for those 3; rest **Rust later** |
| Hudi COW + MOR-without-logs | Native C++; FE `hudi-common` for listing | `hudi` 0.6 | young Apache project, broad feature table | **Rust now** (S3/GCS/Azure/local) |
| Hudi MOR snapshot with log files | BE JNI `HadoopHudiJniScanner` | `hudi` 0.6 (MOR merge, read-optimized, incremental, time travel) | as above | **Rust now** on object stores. HDFS-hosted Hudi: **Rust later** (no HDFS backend listed). |
| Paimon read | Native / `paimon-rust` FFI / JNI fallback | `paimon` 0.4 (already in Doris 4.1.5) | young, but upstream Doris relies on it | **Rust now** for the subset Doris gates on (`PaimonRustReaderCapabilities`); other splits **Rust later** |
| Paimon catalogs | FE `paimon-core` | filesystem, REST | partial | filesystem/REST **Rust now**. HMS: **Rust later**. DLF: **not supported**. |
| Paimon write | BE JNI `PaimonJniWriter` | `paimon` 0.4 has writers; Doris FFI backend is a placeholder | gap | **Rust later** |
| Avro files (TVF / load) | BE JNI `AvroJNIScanner` | `apache-avro` 0.22 (Apache), arrow-rs `arrow-avro` | mature | **Rust now** |
| Lance catalog | BE `lance-c` (Rust); FE `lance-core` (Java over Rust JNI) | `lance` 14 | mature, actively released | **Rust now** |
| LakeSoul catalog | BE JNI `LakeSoulJniScanner` | LakeSoul's own Rust core, unpublished (git only) | niche | **Rust later** (low priority; not supported initially) |
| MaxCompute (ODPS) catalog | FE `odps-sdk`, BE JNI scanner/writer | none found | none | **Not supported** |
| Trino-connector catalog | FE `trino-main` SPI, BE JNI | none (runs Java connector plugins by design) | n/a | **Not supported**. Covered cases (Delta, Kudu, …) need native catalogs; Delta has `deltalake` 1.1 if wanted later. |
| CDC streaming (`cdc_stream` TVF, streaming jobs) | BE-launched Java `cdc_client` (Flink CDC MySQL/PG) | Postgres: `pgwire-replication` 0.5. MySQL: `mysql-binlog-connector-rust` 0.3, or `mysql_async` binlog stream | Postgres usable; MySQL thin | **Rust later** |
| Pluggable Java TVF writer (`writer_class`) | BE JNI `VJniFormatTransformer` | n/a | n/a | **Not supported** |
| Ranger authorization | FE `ranger-plugins-common` | none | none | **Not supported** (internal RBAC only). Could be revisited by fetching Ranger REST policies. |
| LDAP authentication | FE Spring LDAP | `ldap3` 0.12 | mature | **Rust now** |
| AWS Glue / S3 / STS, Azure, GCS, OSS, COS, OBS | FE cloud Java SDKs (BE already native C++) | `aws-sdk-*`, `opendal`, `object_store` | mature | **Rust now** |
| FE dynamic plugins (audit loader etc.) | FE `DynamicPluginLoader` | n/a (Java jars) | n/a | **Not supported** (built-in audit log instead) |
| SQL dialect conversion (`sql_dialect`, trino/spark converters) | FE plugins on `trino-main` parser | `sqlparser` 0.63 parses many dialects but does not translate | gap | **Not supported** initially; **Rust later** if wanted |
| JVM metrics, UDF classloader cache | BE | n/a | n/a | Drop |

## What this means for the roadmap

1. **Lakehouse epic, first wave (all Rust now):**
   - Storage: `opendal` with `hdfs-native` (never `services-hdfs`).
   - Iceberg through `iceberg` and its catalogs.
   - Hudi and Paimon readers.
   - HMS with simple auth, plus Parquet, ORC and Avro.
   - Typed JDBC drivers for MySQL, PostgreSQL, SQL Server and ClickHouse.
   - LDAP.

   This covers everything Doris itself already does without Java, plus most of what it
   does with JNI today.
2. **Rust-later backlog**, each item returning a clear "not supported" until done:
   - Iceberg overwrite and row-level commits, and maintenance actions.
   - Paimon writes.
   - Hive ACID.
   - Kerberos for HDFS and HMS.
   - CDC.
   - Hadoop catalog, Paimon on HMS, and Hudi on HDFS.
   - Oracle, SAP HANA and Trino as JDBC sources.
3. **Not supported, documented as permanent:**
   - Java UDF, UDAF and UDTF (migration gap; RPC or WASM UDFs could be a separate feature).
   - Arbitrary JDBC driver jars, DB2 and Gbase.
   - The Trino-connector catalog, MaxCompute and DLF.
   - Ranger.
   - Java FE plugins and the dialect converters.
   - Pluggable Java TVF writers.
   - The HDFS broker process.
4. **Open decision.** Kerberos and Oracle both work only through a system C library loaded
   at runtime (`libgssapi_krb5`, Oracle Instant Client). The "100% Rust, no JVM" rule
   clearly allows this, since no JVM is involved. Whether "100% Rust" also allows a
   dlopen'd C library needs an explicit call. `sspi-rs` is the pure-Rust alternative for
   Kerberos, but nobody has proven it works with Hadoop SASL.

## Caveats

- Maturity statements come from upstream READMEs, source-tree listings and crates.io
  metadata. None of these libraries was tested against Doris data in this research.
- Gap claims (for example "no overwrite transaction in iceberg-rust") are based on the
  default-branch file listing on 2026-11-04 and can change quickly. Re-check before
  planning a specific ticket.
- "Rust now" for Paimon is limited to what Doris's own FE gate sends to `paimon-rust`
  0.4.0-rc1. Newer `paimon` releases may widen that.
