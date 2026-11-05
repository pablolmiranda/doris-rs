#!/usr/bin/env bash
# Captures the reference C++ BE's meta/ and stream_load/ databases for doris-rocks and
# doris-storage tests (make meta-fixtures). On an empty root:
#   cpp-wal        the M2 scenario's tables, then a graceful stop: tablet headers mostly in the log
#   cpp-compacted  four more restarts, each creating one table and dropping the previous one:
#                  each start replays the log into a level-0 table file, and the fourth reaches
#                  the level-0 compaction trigger, so compacted, compressed table files appear
# Each fixture also holds headers/<tablet>.json (GET /api/meta/header/<id>?byte_to_base64=true)
# for every tablet the BE holds, and keys.txt: tabletmeta_<id>_<hash> for each, sorted. Tables
# dropped in the rounds leave SHUTDOWN headers in meta/ until the BE's trash sweep (at least
# min_garbage_sweep_interval, 180 s, after start); their keys are in dropped.txt.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
OUT="${1:-${REPO}/crates/doris-rocks/tests/fixtures}"

save() {
  local dir="${OUT}/$1"
  rm -rf "${dir}" && mkdir -p "${dir}/headers"
  cpp_tablets | sort -n > "${SWAP_LOGS}/tablets.txt"
  while read -r id; do
    curl -sf "http://127.0.0.1:8040/api/meta/header/${id}?byte_to_base64=true" > "${dir}/headers/${id}.json"
    python3 -c 'import json,sys; print("tabletmeta_%s_%s" % (sys.argv[1], json.load(open(sys.argv[2]))["schema_hash"]))' \
      "${id}" "${dir}/headers/${id}.json"
  done < "${SWAP_LOGS}/tablets.txt" | sort > "${dir}/keys.txt"
  stop_cpp
  cp -R "${SWAP_ROOT}/meta" "${SWAP_ROOT}/stream_load" "${dir}/"
  rm -f "${dir}/meta/LOCK" "${dir}/stream_load/LOCK" "${dir}"/meta/LOG* "${dir}"/stream_load/LOG*
  say "$1: $(wc -l < "${dir}/keys.txt") tablets, $(ls "${dir}/meta" | tr '\n' ' ')"
}

swap_up
start_cpp
sql "
CREATE DATABASE m3;
CREATE TABLE m3.dup (k INT NOT NULL, b BIGINT, v VARCHAR(32), ts DATETIMEV2(3), d DECIMAL(12, 2))
  DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 2 PROPERTIES ('replication_num' = '1');
CREATE TABLE m3.mow (k INT NOT NULL, v VARCHAR(16)) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
  PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true');
CREATE TABLE m3.ranged (dt DATE NOT NULL, k INT) DUPLICATE KEY(dt)
  PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-01-01'), PARTITION p2 VALUES LESS THAN ('2025-01-01'))
  DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1');
ALTER TABLE m3.ranged ADD PARTITION p3 VALUES LESS THAN ('2026-01-01');
"
sleep 12 # two tablet reports
save cpp-wal

for round in 1 2 3 4; do
  start_cpp
  sql "CREATE TABLE m3.r${round} (k INT NOT NULL) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES ('replication_num' = '1');"
  if [[ ${round} -gt 1 ]]; then
    sql "SHOW TABLETS FROM m3.r$((round - 1))" | awk -F'\t' '{print "tabletmeta_" $1 "_" $4}' >> "${SWAP_LOGS}/dropped.txt"
    sql "DROP TABLE m3.r$((round - 1)) FORCE;"
  fi
  sleep 12
  if [[ ${round} -lt 4 ]]; then stop_cpp; fi
done
save cpp-compacted
sort "${SWAP_LOGS}/dropped.txt" > "${OUT}/cpp-compacted/dropped.txt"
say "done"
