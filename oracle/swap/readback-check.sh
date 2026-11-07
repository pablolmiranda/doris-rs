#!/usr/bin/env bash
# The readback check (make readback-check): what doris-rs writes, the reference C++ BE reads
# back exactly as it reads its own writes.
#   1. a fresh swap cluster with the C++ BE: the M4 load dataset (load-dataset.sql: INSERT ...
#      VALUES and CSV/JSON stream loads), then the readback queries -> replies R1
#   2. a fresh swap cluster with doris-rs: the same loads; doris-rs stops, the C++ BE starts on
#      its root, reports every tablet (no DROP or CLONE, nothing deleted from FE metadata), and
#      runs the same queries -> replies R2
#   3. R1 == R2, every query (those named *_unordered compared as sorted rows: their order comes
#      from the storage, which may lay ties out differently)
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

# 2. doris-rs writes, the C++ BE reads
swap_up
: > "${SWAP_LOGS}/rs.err"
start_rs
dataset || fail "doris-rs could not load the dataset (see ${SWAP_LOGS}/rs.err)"
sleep "${REPORTS}"
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
for f in "${OUT}"/R1/*.tsv; do
  name="$(basename "${f}" .tsv)"
  other="${OUT}/R2/${name}.tsv"
  [[ -f "${other}" ]] || fail "${name}: no reply after the swap"
  if [[ "${name}" == *_unordered ]]; then
    diff <(sort "${f}") <(sort "${other}") > "${OUT}/${name}.diff" || fail "${name}: rows differ (${OUT}/${name}.diff)"
  else
    diff "${f}" "${other}" > "${OUT}/${name}.diff" || fail "${name}: reply differs (${OUT}/${name}.diff)"
  fi
done
say "the C++ BE read every query's reply from doris-rs's writes as from its own"
echo "readback check: PASS"
