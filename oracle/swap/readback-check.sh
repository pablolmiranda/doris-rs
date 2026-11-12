#!/usr/bin/env bash
# The readback check (make readback-check): what doris-rs writes, the reference C++ BE reads
# back exactly as it reads its own writes (M4); and doris-rs answers the queries as the C++ BE
# does, over the C++ BE's writes and over its own (M5).
#   1. a fresh swap cluster with the C++ BE: the M4 load dataset (load-dataset.sql: INSERT ...
#      VALUES and CSV/JSON stream loads), then the readback queries -> replies R1. The C++ BE
#      stops, doris-rs starts on its root and runs the queries -> RS1 (round b)
#   2. a fresh swap cluster with doris-rs: the same loads, then the queries -> RS2
#      (round c); doris-rs stops, the C++ BE starts on its root, reports every tablet (no DROP or
#      CLONE, nothing deleted from FE metadata), and runs the same queries -> replies R2
#   3. R1 == R2 (round a), R1 == RS1 and R1 == RS2 for every query they ran (those named
#      *_unordered compared as sorted rows: their order comes from the storage; in rounds b and c
#      *_head_unordered, a LIMIT without ORDER BY, by row count, since which rows come first is
#      each BE's read order)
# The last line is "readback check: PASS" or "readback check: FAIL (<first reason>)".
set -euo pipefail
source "$(dirname "$0")/lib.sh"
REPORTS=12
OUT="${SWAP_LOGS%/*}/readback"

fail() { echo "readback check: FAIL ($*)"; exit 1; }
cleanup() {
  [[ -f "${SWAP_LOGS}/rs.pid" ]] && kill -TERM "$(cat "${SWAP_LOGS}/rs.pid")" 2>/dev/null || true
  docker rm -f "${CPP}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

dataset() { python3 "${ORACLE}/swap/run_dataset.py" load "${ORACLE}/swap/load-dataset.sql" "${ORACLE}/swap/load-dataset"; }
queries() { python3 "${ORACLE}/swap/run_dataset.py" query "${ORACLE}/swap/readback-queries.sql" "$1"; }

cargo build --release -p doris-rs --manifest-path "${REPO}/Cargo.toml" >&2
rm -rf "${OUT}" && mkdir -p "${OUT}"

# 1. the reference's own writes
swap_up
start_cpp
dataset || fail "the C++ BE could not load the dataset"
sleep "${REPORTS}"
queries "${OUT}/R1"
stop_cpp
say "R1: $(ls "${OUT}/R1" | wc -l | tr -d ' ') replies from the C++ BE's own loads"
: > "${SWAP_LOGS}/rs.err"
start_rs
sleep "${REPORTS}"
queries "${OUT}/RS1" || fail "doris-rs could not answer the queries over the C++ BE's root (see ${SWAP_LOGS}/rs.err)"
stop_rs
say "RS1: $(ls "${OUT}/RS1" | wc -l | tr -d ' ') replies from doris-rs over the C++ BE's loads"

# 2. doris-rs writes, the C++ BE reads
swap_up
: > "${SWAP_LOGS}/rs.err"
start_rs
dataset || fail "doris-rs could not load the dataset (see ${SWAP_LOGS}/rs.err)"
sleep "${REPORTS}"
queries "${OUT}/RS2" || fail "doris-rs could not answer the queries over its own loads (see ${SWAP_LOGS}/rs.err)"
stop_rs
mark_fe_log
start_cpp
sleep "${REPORTS}"
reported cpp
if docker exec "${CPP}" cat /opt/apache-doris/be/log/be.INFO 2>/dev/null | grep -qE 'task_type=(DROP|CLONE)|type=(DROP|CLONE)\|'; then
  fail "the C++ BE received a DROP or CLONE for doris-rs's tablets"
fi
queries "${OUT}/R2"
stop_cpp

# 3. the replies
compare() { # <round> <dir>: every reply in <dir> equals R1's
  local f name other n=0
  for other in "$2"/*.tsv; do
    name="$(basename "${other}" .tsv)"
    f="${OUT}/R1/${name}.tsv"
    [[ -f "${f}" ]] || fail "$1 ${name}: no reply from the C++ BE"
    if [[ "${name}" == *_head_unordered && "$1" != a ]]; then
      # LIMIT without ORDER BY: which rows come first is the storage read order, which another
      # BE may not share; only the count is the reply's
      [[ "$(wc -l < "${f}")" -eq "$(wc -l < "${other}")" ]] || fail "$1 ${name}: $(wc -l < "${other}") lines, the C++ BE $(wc -l < "${f}")"
    elif [[ "${name}" == *_unordered ]]; then
      diff <(sort "${f}") <(sort "${other}") > "${OUT}/$1-${name}.diff" || fail "$1 ${name}: rows differ (${OUT}/$1-${name}.diff)"
    else
      diff "${f}" "${other}" > "${OUT}/$1-${name}.diff" || fail "$1 ${name}: reply differs (${OUT}/$1-${name}.diff)"
    fi
    n=$((n + 1))
  done
  say "round $1: ${n} replies equal the C++ BE's"
}
for f in "${OUT}"/R1/*.tsv; do
  [[ -f "${OUT}/R2/$(basename "${f}")" ]] || fail "a $(basename "${f}" .tsv): no reply after the swap"
done
compare a "${OUT}/R2"
compare b "${OUT}/RS1"
compare c "${OUT}/RS2"
# doris-rs answers every query, the join included (M6), in both rounds
for round in b:RS1 c:RS2; do
  [[ "$(ls "${OUT}/${round#*:}" | wc -l)" -eq "$(ls "${OUT}/R1" | wc -l)" ]] ||
    fail "${round%%:*}: doris-rs answered $(ls "${OUT}/${round#*:}" | wc -l | tr -d ' ') queries"
done
say "the C++ BE read doris-rs's writes as its own, and doris-rs answered as the C++ BE"
echo "readback check: PASS"
