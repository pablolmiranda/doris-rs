#!/usr/bin/env bash
# Run upstream regression suite directories against a target cluster and record results.
#
#   run_suites.sh TARGET SUITE_LIST [extra framework args...]
#
# TARGET      name of a file in targets/ (reference, rust, ...)
# SUITE_LIST  file with one suite directory per line (see suites/)
#
# Output goes to results/TARGET/<run-id>/: the rendered conf, the raw TeamCity log, results.json
# (parsed), and meta.json (what ran against what). The framework's exit code is recorded, not
# propagated: a baseline is allowed to contain failures.
#
# Switches (environment):
#   PROXY=1   put the MySQL tap (checker/tap.py) in front of the target's FE ($FE_CONTAINER) and
#             record every session into tap/; the suites connect through it
#   SERIAL=1  one suite at a time, so each suite's time window is its own (the checker links
#             replies and FE<->BE traffic to suites by time)
# The target's deployment facts ($FACTS in its targets/ file) are copied to facts.json.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORACLE="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$ORACLE/.." && pwd)"
# shellcheck disable=SC1091
source "$ORACLE/versions.env"

TARGET="${1:?usage: run_suites.sh TARGET SUITE_LIST [framework args]}"
SUITE_LIST="${2:?usage: run_suites.sh TARGET SUITE_LIST [framework args]}"
shift 2
EXTRA_ARGS=("$@")

TARGET_ENV="$ORACLE/targets/$TARGET.env"
[[ -f "$TARGET_ENV" ]] || { echo "error: no target file $TARGET_ENV" >&2; exit 1; }
[[ -f "$SUITE_LIST" ]] || SUITE_LIST="$ORACLE/$SUITE_LIST"
[[ -f "$SUITE_LIST" ]] || { echo "error: suite list not found: $2" >&2; exit 1; }
# shellcheck disable=SC1090
source "$TARGET_ENV"

SRC="${DORIS_SRC:-$ROOT/.cache/doris-src}"
"$HERE/ensure_src.sh" >/dev/null
SRC_COMMIT="$(git -C "$SRC" rev-parse --short HEAD)"

DIRS="$(grep -vE '^\s*(#|$)' "$SUITE_LIST" | tr -d ' ' | paste -sd, -)"
[[ -n "$DIRS" ]] || { echo "error: suite list is empty" >&2; exit 1; }

RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
OUT="$ORACLE/results/$TARGET/$RUN_ID"
mkdir -p "$OUT/conf"
[[ -n "${FACTS:-}" ]] && cp "$ORACLE/$FACTS" "$OUT/facts.json"

QUERY_PORT="$FE_QUERY_PORT"
TAP_NAME=doris-oracle-tap
stop_tap() { docker stop -t 10 "$TAP_NAME" >/dev/null 2>&1 || true; docker rm -f "$TAP_NAME" >/dev/null 2>&1 || true; }
if [[ "${PROXY:-0}" == 1 ]]; then
  : "${FE_CONTAINER:?target $TARGET has no FE_CONTAINER for the tap}"
  mkdir -p "$OUT/tap"
  stop_tap
  docker run -d --name "$TAP_NAME" --network "container:$FE_CONTAINER" \
    -v "$ORACLE/checker:/checker:ro" -v "$OUT/tap:/out" "${TAP_IMAGE:-python:3.11-slim}" \
    python -u /checker/tap.py --listen 0.0.0.0:9031 --upstream "127.0.0.1:$FE_QUERY_PORT" --out /out >/dev/null
  trap stop_tap EXIT
  for _ in $(seq 1 30); do docker logs "$TAP_NAME" 2>&1 | grep -q "tap listening" && break; sleep 1; done
  docker logs "$TAP_NAME" 2>&1 | grep -q "tap listening" || { echo "error: tap did not start" >&2; exit 1; }
  QUERY_PORT=9031
fi
if [[ "${SERIAL:-0}" == 1 ]]; then
  PARALLEL=1 SUITE_PARALLEL=1
  EXTRA_ARGS+=(-actionParallel 1)
fi

JDBC="jdbc:mysql://${FE_HOST}:${QUERY_PORT}/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"
cp "$SRC/regression-test/conf/logback.xml" "$OUT/conf/logback.xml"
python3 "$HERE/override_conf.py" \
  "$SRC/regression-test/conf/regression-conf.groovy" "$OUT/conf/regression-conf.groovy" \
  "jdbcUrl=\"$JDBC\"" \
  "targetJdbcUrl=\"$JDBC\"" \
  "feSourceThriftAddress=\"${FE_HOST}:${FE_RPC_PORT}\"" \
  "feTargetThriftAddress=\"${FE_HOST}:${FE_RPC_PORT}\"" \
  "feHttpAddress=\"${FE_HOST}:${FE_HTTP_PORT}\"" \
  "excludeDockerTest=true"

PARALLEL="${PARALLEL:-4}"
SUITE_PARALLEL="${SUITE_PARALLEL:-1}"
NETWORK="${TARGET_NETWORK:-$ORACLE_NETWORK}"

cat > "$OUT/meta.json" <<JSON
{
  "target": "$TARGET",
  "fe": "${FE_HOST}:${FE_QUERY_PORT}",
  "suite_list": "$(basename "$SUITE_LIST")",
  "directories": "$DIRS",
  "doris_src_tag": "$DORIS_SRC_TAG",
  "doris_src_commit": "$SRC_COMMIT",
  "reference_image": "$DORIS_IMAGE",
  "runner_image": "$RUNNER_IMAGE",
  "proxy": "${PROXY:-0}",
  "serial": "${SERIAL:-0}",
  "started": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
JSON

echo "target=$TARGET fe=${FE_HOST}:${FE_QUERY_PORT} src=$DORIS_SRC_TAG@$SRC_COMMIT dirs=$DIRS"
echo "output: $OUT"

# host.docker.internal is only meaningful on a bridge network; a container: network shares the
# target's namespace and forbids --add-host.
HOST_ALIAS=(--add-host host.docker.internal:host-gateway)
[[ "$NETWORK" == container:* ]] && HOST_ALIAS=()

set +e
docker run --rm \
  --network "$NETWORK" \
  ${HOST_ALIAS[@]+"${HOST_ALIAS[@]}"} \
  -v "$SRC:/doris" \
  -v "$OUT:/out" \
  -v doris-oracle-m2:/root/.m2 \
  -e DORIS_HOME=/doris \
  "$RUNNER_IMAGE" \
  "cd /doris && ./run-regression-test.sh --teamcity --conf /out/conf/regression-conf.groovy --run -d '$DIRS' -parallel $PARALLEL -suiteParallel $SUITE_PARALLEL ${EXTRA_ARGS[@]+"${EXTRA_ARGS[*]}"}" \
  2>&1 | tee "$OUT/teamcity.log"
FRAMEWORK_EXIT=${PIPESTATUS[0]}
set -e
[[ "${PROXY:-0}" == 1 ]] && stop_tap

if ! python3 "$HERE/parse_teamcity.py" "$OUT/results.json" "$OUT/teamcity.log"; then
  echo "error: no suite results parsed; see $OUT/teamcity.log (framework exit $FRAMEWORK_EXIT)" >&2
  exit 2
fi
python3 - "$OUT/meta.json" "$FRAMEWORK_EXIT" <<'PY'
import json, sys, datetime
path, code = sys.argv[1], int(sys.argv[2])
meta = json.load(open(path))
meta["framework_exit_code"] = code
meta["finished"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
json.dump(meta, open(path, "w"), indent=2)
PY
ln -sfn "$RUN_ID" "$ORACLE/results/$TARGET/latest"
echo "results: $OUT/results.json (framework exit $FRAMEWORK_EXIT)"
