#!/usr/bin/env bash
# Record FE<->BE RPC traffic for the wire corpus: recreate the reference container, start the
# capture sidecar immediately (before the FE has booted, so every connection is recorded from
# its first byte), wait for the cluster, run the scenario, then stop the capture.
# SCENARIO picks the scenario (default wire/scenario.sh). With OUT set, the files the scenario
# left in /tmp/scenario-out inside the container are copied to OUT.
# Prints the path of the new pcap (oracle/wire/captures/<run>.pcap, never committed).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WIRE="$(cd "$HERE/.." && pwd)"
ORACLE="$(cd "$WIRE/.." && pwd)"
# shellcheck disable=SC1091
source "$ORACLE/versions.env"

export CAPTURE_DIR="$WIRE/captures"
export CAPTURE_FILE="$(date +%Y%m%d-%H%M%S).pcap"
mkdir -p "$CAPTURE_DIR"
COMPOSE=(docker compose -f "$ORACLE/compose/reference-single.yml" -f "$ORACLE/compose/capture.yml")

"${COMPOSE[@]}" rm -sf capture >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --force-recreate --no-deps doris
"${COMPOSE[@]}" up -d --no-deps capture

status=starting
for _ in $(seq 1 120); do
  status="$(docker inspect -f '{{.State.Health.Status}}' "$("${COMPOSE[@]}" ps -q doris)")"
  [[ "$status" == healthy ]] && break
  sleep 2
done
[[ "$status" == healthy ]] || { echo "error: reference not healthy ($status)" >&2; exit 1; }

SCENARIO="${SCENARIO:-$WIRE/scenario.sh}"
"${COMPOSE[@]}" exec -T doris bash -s < "$SCENARIO" >&2
if [[ -n "${OUT:-}" ]]; then
  rm -rf "$OUT/headers" "$OUT/layout.txt"
  mkdir -p "$OUT"
  docker cp "$("${COMPOSE[@]}" ps -q doris):/tmp/scenario-out/." "$OUT" >&2
fi
"${COMPOSE[@]}" stop capture >/dev/null
"${COMPOSE[@]}" rm -f capture >/dev/null
echo "$CAPTURE_DIR/$CAPTURE_FILE"
