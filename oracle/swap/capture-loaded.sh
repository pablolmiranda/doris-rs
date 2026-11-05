#!/usr/bin/env bash
# Captures the reference C++ BE after the M4 load dataset (make loaded-fixture): INSERT ... VALUES
# and CSV/JSON stream loads into duplicate-key tables. Kept in
# crates/doris-storage/tests/fixtures/cpp-loaded:
#   meta/, stream_load/   the BE's RocksDB databases after a graceful stop
#   data/                 the tablet directories with their segment files
#   headers/<id>.json     GET /api/meta/header/<id> for every tablet, rowsets included
#   rowsets.txt           <tablet_id> <rowset_id> <start>-<end> <num_rows> <num_segments>, per rowset
#   layout.txt            every file under data/, with its size
#   replies/<name>.tsv    the readback queries' replies (swap/readback-queries.sql)
# A separate target from meta-fixtures, which would rewrite M3's fixtures.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
OUT="${1:-${REPO}/crates/doris-storage/tests/fixtures}/cpp-loaded"

swap_up
start_cpp
python3 "${ORACLE}/swap/run_dataset.py" load "${ORACLE}/swap/load-dataset.sql" "${ORACLE}/swap/load-dataset"
sleep 12 # two tablet reports
rm -rf "${OUT}" && mkdir -p "${OUT}/headers"
python3 "${ORACLE}/swap/run_dataset.py" query "${ORACLE}/swap/readback-queries.sql" "${OUT}/replies"
cpp_tablets | sort -n > "${SWAP_LOGS}/tablets.txt"
while read -r id; do
  curl -sf "http://127.0.0.1:8040/api/meta/header/${id}?byte_to_base64=true" > "${OUT}/headers/${id}.json"
done < "${SWAP_LOGS}/tablets.txt"
python3 - "${OUT}" <<'PY'
import json, pathlib, sys
out = pathlib.Path(sys.argv[1])
lines = []
for f in sorted(out.glob("headers/*.json"), key=lambda p: int(p.stem)):
    h = json.load(open(f))
    for r in h.get("rs_metas", []):
        lines.append(f'{h["tablet_id"]} {r["rowset_id_v2"]} {r["start_version"]}-{r["end_version"]} {r.get("num_rows", 0)} {r.get("num_segments", 0)}')
(out / "rowsets.txt").write_text("\n".join(lines) + "\n")
PY
stop_cpp
cp -R "${SWAP_ROOT}/meta" "${SWAP_ROOT}/stream_load" "${SWAP_ROOT}/data" "${OUT}/"
rm -f "${OUT}/meta/LOCK" "${OUT}/stream_load/LOCK" "${OUT}"/meta/LOG* "${OUT}"/stream_load/LOG*
(cd "${OUT}" && find data -type f | sort | while read -r f; do echo "${f} $(wc -c < "${f}" | tr -d ' ')"; done) > "${OUT}/layout.txt"
say "cpp-loaded: $(ls "${OUT}/headers" | wc -l | tr -d ' ') tablets, $(grep -c . "${OUT}/rowsets.txt") rowsets, $(ls "${OUT}/replies" | wc -l | tr -d ' ') replies"
