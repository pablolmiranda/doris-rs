#!/usr/bin/env bash
# The swap check (make swap-check): doris-rs and the reference C++ BE take turns as the one
# backend of the FE-only cluster, on one storage root, and the FE must notice nothing.
#   1. doris-rs creates the tables (the M2 scenario's shapes); snapshot S0
#   2. doris-rs stops, the C++ BE starts on its root; snapshot S1; S0 == S1
#   3. the C++ BE creates a table and drops one doris-rs created; snapshot S2
#   4. the C++ BE stops, doris-rs starts on its root; snapshot S3; S2 == S3
#   5. doris-rs creates a table and drops one the C++ BE created (leaving SHUTDOWN headers);
#      snapshot S4
#   6. doris-rs stops, the C++ BE starts on its root; snapshot S5; S4 == S5, and the C++ BE
#      sweeps doris-rs's SHUTDOWN tablets into trash
# A snapshot is SHOW TABLETS for every table, SHOW BACKENDS, and the incoming BE's log (no DROP or
# CLONE may arrive to repair anything). The last line is "swap check: PASS" or
# "swap check: FAIL (<first reason>)".
set -euo pipefail
source "$(dirname "$0")/lib.sh"
SNAP="${SWAP_LOGS}/snapshots"
REPORTS=12 # two tablet reports at report_tablet_interval_seconds = 5

fail() { echo "swap check: FAIL ($*)"; exit 1; }

cleanup() {
  [[ -f "${SWAP_LOGS}/rs.pid" ]] && kill -TERM "$(cat "${SWAP_LOGS}/rs.pid")" 2>/dev/null || true
  docker rm -f "${CPP}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# The FE log's length, so checks read only what follows a swap.
fe_mark=0
mark_fe_log() { fe_mark="$(docker exec "${FE}" sh -c 'wc -l < /opt/apache-doris/fe/log/fe.log' | tr -d ' ')"; }
fe_log_since_mark() { docker exec "${FE}" tail -n "+$((fe_mark + 1))" /opt/apache-doris/fe/log/fe.log; }

sql_tsv() { docker exec "${FE}" mysql -uroot -h127.0.0.1 -P9030 --batch -e "$1"; }

# Every OLAP table, from FE metadata only (information_schema needs a BE that runs queries).
tables() {
  local db
  for db in $(sql "SHOW DATABASES" | grep -vxE 'information_schema|mysql'); do
    sql "SHOW FULL TABLES FROM \`${db}\`" | awk -F'\t' -v db="${db}" '$2 == "BASE TABLE" {print db "." $1}'
  done | sort
}

# The incoming BE's log since it started.
rs_log_start=0
incoming_log() {
  case "$1" in
    rs) tail -c "+$((rs_log_start + 1))" "${SWAP_LOGS}/rs.err" ;;
    cpp) docker exec "${CPP}" cat /opt/apache-doris/be/log/be.INFO 2>/dev/null || true ;;
  esac
}

snapshot() {
  local dir="${SNAP}/$1" incoming="$2" t
  rm -rf "${dir}" && mkdir -p "${dir}"
  for t in $(tables); do
    sql_tsv "SHOW TABLETS FROM ${t}" > "${dir}/tablets-${t}.tsv"
  done
  sql_tsv "SHOW BACKENDS" > "${dir}/backends.tsv"
  incoming_log "${incoming}" > "${dir}/tasks.log"
}

# No tablet may hold data: doris-rs refuses such a root until milestone 4.
no_data() {
  local t rows
  for t in $(tables); do
    rows="$(sql "SHOW TABLETS FROM ${t}" | awk -F'\t' '{s += $11} END {print s + 0}')"
    [[ "${rows}" == 0 ]] || fail "harness: ${t} holds ${rows} rows before a swap"
  done
}

compare() {
  local out
  if ! out="$(python3 "${ORACLE}/swap/swap_compare.py" "${SNAP}/$1" "${SNAP}/$2")"; then
    echo "${out}" >&2
    fail "$1 -> $2: $(echo "${out}" | head -1)"
  fi
  say "$1 -> $2: the FE sees the same backend and tablets"
}

ids() { sql "SHOW TABLETS FROM $1" | cut -f1 | sort -n; }

# The incoming BE loaded every tablet: the FE's last tablet report diff (ReportHandler,
# LocalTabletInvertedIndex.logTabletReportSummary) counts as many tablets on the backend as in
# its metadata, and deleted none from its metadata.
reported() {
  local line fe be
  line="$(fe_log_since_mark | grep "finished to do tablet diff" | tail -1)"
  fe="$(sed -nE 's/.*fe tablet num: ([0-9]+).*/\1/p' <<< "${line}")"
  be="$(sed -nE 's/.*backend tablet num: ([0-9]+).*/\1/p' <<< "${line}")"
  [[ -n "${fe}" && "${fe}" == "${be}" && "${line}" == *"metaDel: 0"* ]] || fail "$1: tablet report diff: ${line##*]}"
  say "$1 reports all ${be} tablets"
}

# After a DROP TABLE, the incoming BE must receive DROP for each of that table's tablets. The FE
# sends them from its tablet report handling, so they can take a few report rounds.
dropped() {
  local table="$1" incoming="$2" want got i
  want="$(wc -l < "${SWAP_LOGS}/drop-${table}.ids" | tr -d ' ')"
  for i in $(seq 1 45); do
    got="$(incoming_log "${incoming}" | grep -cE 'task DROP signature=|type=DROP\|' || true)"
    [[ "${got}" -ge "${want}" ]] && break
    sleep 2
  done
  [[ "${got}" -ge "${want}" ]] || fail "${incoming} received ${got} DROP tasks for the ${want} tablets of ${table}"
  say "${incoming} dropped ${table} (${want} tablets), which the other binary created"
}

make_table() { sql "CREATE TABLE $1 (k INT NOT NULL) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES ('replication_num' = '1');"; }

drop_table() {
  ids "$1" > "${SWAP_LOGS}/drop-$1.ids"
  sql "DROP TABLE $1 FORCE;"
}

cargo build --release -p doris-rs --manifest-path "${REPO}/Cargo.toml" >&2
swap_up
: > "${SWAP_LOGS}/rs.err"

start_rs
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
sleep "${REPORTS}"
no_data
snapshot S0 rs

stop_rs
mark_fe_log
start_cpp
sleep "${REPORTS}"
snapshot S1 cpp
reported cpp
compare S0 S1

make_table m3.by_cpp
drop_table m3.dup
dropped m3.dup cpp
[[ -n "$(ids m3.by_cpp)" ]] || fail "m3.by_cpp has no tablets"
no_data
snapshot S2 cpp

stop_cpp
rs_log_start="$(wc -c < "${SWAP_LOGS}/rs.err" | tr -d ' ')"
mark_fe_log
start_rs
sleep "${REPORTS}"
snapshot S3 rs
reported rs
compare S2 S3

make_table m3.by_rs
drop_table m3.by_cpp
dropped m3.by_cpp rs
[[ -n "$(ids m3.by_rs)" ]] || fail "m3.by_rs has no tablets"
snapshot S4 rs

stop_rs
mark_fe_log
start_cpp
sleep "${REPORTS}"
snapshot S5 cpp
reported cpp
compare S4 S5
# the C++ BE's sweep runs at its start: doris-rs's SHUTDOWN tablets go to its trash
for id in $(cat "${SWAP_LOGS}/drop-m3.by_cpp.ids"); do
  ls -d "${SWAP_ROOT}"/trash/*/"${id}" >/dev/null 2>&1 || fail "the C++ BE did not sweep tablet ${id}, which doris-rs dropped"
done
say "cpp swept the $(wc -l < "${SWAP_LOGS}/drop-m3.by_cpp.ids" | tr -d ' ') tablets doris-rs had shut down"
stop_cpp
echo "swap check: PASS"
