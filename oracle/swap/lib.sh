# Shared functions for the swap cluster: the FE-only cluster (compose/fe-only.yml) with one
# backend entry, served in turns by doris-rs (native, on this host) and the reference C++ BE (a
# container with host networking). Both use one storage root at the same absolute path, so the
# disk keeps its identity (path_hash = hash_of_path(host, path), data_dir.cpp:202).
#
# The FE reaches the backend at the address host.docker.internal resolves to (make fe-up
# registers it). OrbStack forwards that address's ports to host-network containers, and the C++
# BE takes a backend_ip from the FE without checking it is local (heartbeat_server.cpp:137-185).
# Its path_hash uses the address priority_networks picks at start, the VM's first address, so
# doris-rs is given the same string as `host`.
# shellcheck shell=bash

ORACLE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "${ORACLE}/.." && pwd)"
SWAP_ROOT="${SWAP_ROOT:-/tmp/doris-rs-oracle/swap}"
SWAP_LOGS="${SWAP_LOGS:-/tmp/doris-rs-oracle/swap-logs}"
FE=doris-oracle-fe-only
CPP=doris-oracle-swap-be
IMAGE="${DORIS_IMAGE:-apache/doris:all-in-one-4.1.5}"
RS_BIN="${REPO}/target/release/doris-rs"

say() { echo "swap: $*" >&2; }
die() { say "$*"; exit 1; }

sql() { docker exec "${FE}" mysql -uroot -h127.0.0.1 -P9030 --batch --skip-column-names -e "$1"; }

# The address the C++ BE's priority_networks picks inside the VM.
vm_ip() {
  docker run --rm --network host --entrypoint sh "${IMAGE}" -c 'hostname -I' | awk '{print $1}'
}

# A fresh FE-only cluster, an empty root, and no internal table that would receive data
# (audit log, statistics): a tablet with data would make doris-rs refuse the root.
swap_up() {
  docker rm -f "${CPP}" >/dev/null 2>&1 || true
  make -C "${ORACLE}" fe-down >/dev/null 2>&1 || true
  rm -rf "${SWAP_ROOT}" "${SWAP_LOGS}" && mkdir -p "${SWAP_ROOT}" "${SWAP_LOGS}"
  make -C "${ORACLE}" fe-up >&2
  sql "SET GLOBAL enable_audit_plugin = false; SET GLOBAL enable_auto_analyze = false;"
  VM_IP="$(vm_ip)"
  [[ -n "${VM_IP}" ]] || die "no VM address"
  export VM_IP
}

alive() { sql "SHOW BACKENDS" | awk -F'\t' '{print $10}'; }

# Waits until the backend is alive (true) or dead (false).
wait_alive() {
  local want="${1:-true}" i
  for i in $(seq 1 60); do
    [[ "$(alive)" == "${want}" ]] && return 0
    sleep 2
  done
  die "backend never became Alive=${want}"
}

# Waits until the FE has the backend's disk (a CREATE TABLE before the first disk report fails).
wait_disk() {
  local id i
  id="$(sql "SHOW BACKENDS" | cut -f1)"
  for i in $(seq 1 60); do
    [[ -n "$(sql "SHOW PROC '/backends/${id}'" | grep -F "${SWAP_ROOT}" | grep ONLINE)" ]] && return 0
    sleep 2
  done
  die "the FE never received the disk report"
}

start_cpp() {
  local conf="${SWAP_LOGS}/cpp.conf"
  { cat "${ORACLE}/swap/be.conf"; echo "storage_root_path = ${SWAP_ROOT}"; echo "priority_networks = ${VM_IP}/32"; } > "${conf}"
  docker run -d --name "${CPP}" --network host \
    -v "${SWAP_ROOT}:${SWAP_ROOT}" -v "${conf}:/swap/be.conf:ro" \
    --entrypoint bash "${IMAGE}" \
    -c 'cat /swap/be.conf >> /opt/apache-doris/be/conf/be.conf && exec /opt/apache-doris/be/bin/start_be.sh --console' >/dev/null
  wait_alive true
  wait_disk
}

# start_be.sh runs doris_be as a child, so stop it with the image's own graceful stop.
stop_cpp() {
  docker exec "${CPP}" /opt/apache-doris/be/bin/stop_be.sh --grace >/dev/null 2>&1 || true
  docker wait "${CPP}" >/dev/null 2>&1 || true
  docker logs "${CPP}" > "${SWAP_LOGS}/cpp-$(date +%s).out" 2>&1 || true
  docker cp "${CPP}:/opt/apache-doris/be/log/be.INFO" "${SWAP_LOGS}/cpp-$(date +%s).INFO" >/dev/null 2>&1 || true
  docker rm -f "${CPP}" >/dev/null
  wait_alive false
}

start_rs() {
  local conf="${SWAP_LOGS}/rs.conf"
  { cat "${ORACLE}/swap/be.conf"; echo "storage_root_path = ${SWAP_ROOT}"; echo "host = ${VM_IP}"; } > "${conf}"
  [[ -x "${RS_BIN}" ]] || die "build doris-rs first: cargo build --release -p doris-rs"
  "${RS_BIN}" be --config "${conf}" 2>> "${SWAP_LOGS}/rs.err" &
  echo $! > "${SWAP_LOGS}/rs.pid"
  disown
  wait_alive true
  wait_disk
}

stop_rs() {
  local pid
  pid="$(cat "${SWAP_LOGS}/rs.pid")"
  kill -TERM "${pid}" 2>/dev/null || true
  while kill -0 "${pid}" 2>/dev/null; do sleep 0.2; done
  wait_alive false
}

# Every tablet id the C++ BE holds (GET /tablets_json).
cpp_tablets() {
  curl -sf "http://127.0.0.1:8040/tablets_json?limit=all" |
    python3 -c 'import json,sys; [print(t["tablet_id"]) for t in json.load(sys.stdin)["data"]["tablets"]]'
}

# The FE log's length, so checks read only what follows a swap.
fe_mark=0
mark_fe_log() { fe_mark="$(docker exec "${FE}" sh -c 'wc -l < /opt/apache-doris/fe/log/fe.log' | tr -d ' ')"; }
fe_log_since_mark() { docker exec "${FE}" tail -n "+$((fe_mark + 1))" /opt/apache-doris/fe/log/fe.log; }

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
