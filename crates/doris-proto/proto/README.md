Verbatim copies of these `gensrc/proto/*.proto` files from Apache Doris at tag `4.1.5-release`
(commit 917b20be838), Apache License 2.0, headers kept:

| File | Defines |
|---|---|
| `segment_v2.proto` | On-disk segment format: footer, column meta, pages, indexes |
| `olap_file.proto`, `olap_common.proto`, `types.proto` | Tablet and rowset metadata, shared types |
| `internal_service.proto` | `PBackendService`, the FE→BE and BE→BE gRPC service (brpc port 8060) |
| `data.proto`, `descriptors.proto`, `runtime_profile.proto` | Types `internal_service.proto` imports |

Update them only together with `oracle/versions.env`, and regenerate both golden corpora
(`oracle/corpus` and `oracle/wire/corpus`) when you do.
