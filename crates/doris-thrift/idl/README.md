Verbatim copies of `gensrc/thrift/*.thrift` (all 27 files) from Apache Doris at tag
`4.1.5-release` (commit 917b20be838), Apache License 2.0, headers kept. At this tag every IDL
file is tracked in `gensrc/thrift`; the build-time `gensrc/build/thrift` directory is empty.

Update them only together with `oracle/versions.env`, then run `../regen.sh` and regenerate
`oracle/wire/corpus`.
