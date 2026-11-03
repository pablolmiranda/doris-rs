#!/usr/bin/env bash
# The Epic 1 reference: four serial runs of the epic's suites through the MySQL tap, two on a
# fresh cluster A (run 1 also captures FE<->BE traffic from boot) and two on a fresh cluster B,
# then everything derived from them into OUT (default epic1/).
#
#   epic_baseline.sh [SUITES]        SUITES defaults to suites/epic1.txt
#
# Progress goes to results/epic-baseline-<stamp>.log, one timestamped line per phase.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORACLE="$(cd "$HERE/.." && pwd)"
cd "$ORACLE"
SUITES="${1:-suites/epic1.txt}"
OUT="${OUT:-epic1}"
STAMP="$(date +%Y%m%d-%H%M%S)"
mkdir -p results/captures
LOG="results/epic-baseline-$STAMP.log"
phase() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$LOG"; }

SINGLE=(docker compose -f compose/reference-single.yml)
CAPTURED=(docker compose -f compose/reference-single.yml -f compose/capture.yml)
SPLIT=(docker compose -f compose/reference-split.yml)
export CAPTURE_DIR="$ORACLE/results/captures" CAPTURE_FILE="epic-$STAMP.pcap"
export CAPTURE_FILTER="tcp port 9020 or tcp port 9050 or tcp port 9060 or tcp port 8060 or tcp port 8040"

wait_healthy() {
  local status=starting
  for _ in $(seq 1 180); do
    status="$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null || echo missing)"
    [[ "$status" == healthy ]] && return 0
    sleep 2
  done
  echo "error: $1 not healthy ($status)" >&2
  return 1
}

run() {  # run TARGET RUN_ID [CAPTURE]
  phase "run $2 on $1: start"
  RUN_ID="$2" PROXY=1 SERIAL=1 bin/run_suites.sh "$1" "$SUITES" >> "$LOG" 2>&1
  phase "run $2 on $1: $(python3 -c "import json;print(json.load(open('results/$1/$2/results.json'))['meta']['counts'])")"
}

phase "suites: $SUITES; products: $OUT"
"${SPLIT[@]}" down -v >/dev/null 2>&1 || true
"${CAPTURED[@]}" down -v >/dev/null 2>&1 || true

phase "cluster A with capture from boot"
"${CAPTURED[@]}" up -d --no-deps doris >> "$LOG" 2>&1
"${CAPTURED[@]}" up -d --no-deps capture >> "$LOG" 2>&1
wait_healthy doris-oracle-doris-1
bin/cluster_setup.sh doris-oracle-doris-1 >> "$LOG"
run reference-single "$STAMP-a1"
"${CAPTURED[@]}" stop capture >> "$LOG" 2>&1
phase "capture: $(docker logs doris-oracle-capture-1 2>&1 | grep -E 'captured|dropped|received' | tr '\n' ' ')"
"${CAPTURED[@]}" rm -f capture >> "$LOG" 2>&1
mv "results/captures/$CAPTURE_FILE" "results/reference-single/$STAMP-a1/traffic.pcap"
phase "extract run a1 traffic"
python3 checker/traffic.py "results/reference-single/$STAMP-a1/traffic.pcap" "results/reference-single/$STAMP-a1/traffic.bin.gz" >> "$LOG" 2>&1
run reference-single "$STAMP-a2"
"${SINGLE[@]}" down -v >> "$LOG" 2>&1

phase "cluster B"
"${SPLIT[@]}" up --wait >> "$LOG" 2>&1
bin/cluster_setup.sh doris-oracle-split-fe-1 >> "$LOG"
run reference-split "$STAMP-b1"
run reference-split "$STAMP-b2"
"${SPLIT[@]}" down -v >> "$LOG" 2>&1

phase "derive into $OUT"
rm -rf "$OUT"
python3 checker/derive.py "$OUT" "results/reference-single/$STAMP-a1" "results/reference-single/$STAMP-a2" \
  "results/reference-split/$STAMP-b1" "results/reference-split/$STAMP-b2" --scope suites/epic1.scope.json | tee -a "$LOG"
phase "needs index and ranking"
cargo run -q --release -p doris-wire --bin doris-needs -- "$ORACLE/$OUT/traffic" > "$OUT/needs.json" 2>> "$LOG"
python3 checker/rank.py "$OUT" | tee -a "$LOG"
phase "done: runs $STAMP-{a1,a2,b1,b2}"
