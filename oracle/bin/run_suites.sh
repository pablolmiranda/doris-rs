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

RUN_ID="$(date +%Y%m%d-%H%M%S)"
OUT="$ORACLE/results/$TARGET/$RUN_ID"
mkdir -p "$OUT/conf"

JDBC="jdbc:mysql://${FE_HOST}:${FE_QUERY_PORT}/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"
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
  "started": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
JSON

echo "target=$TARGET fe=${FE_HOST}:${FE_QUERY_PORT} src=$DORIS_SRC_TAG@$SRC_COMMIT dirs=$DIRS"
echo "output: $OUT"

set +e
docker run --rm \
  --network "$NETWORK" \
  --add-host host.docker.internal:host-gateway \
  -v "$SRC:/doris" \
  -v "$OUT:/out" \
  -v doris-oracle-m2:/root/.m2 \
  -e DORIS_HOME=/doris \
  "$RUNNER_IMAGE" \
  "cd /doris && ./run-regression-test.sh --teamcity --conf /out/conf --run -d '$DIRS' -parallel $PARALLEL -suiteParallel $SUITE_PARALLEL ${EXTRA_ARGS[*]}" \
  2>&1 | tee "$OUT/teamcity.log"
FRAMEWORK_EXIT=${PIPESTATUS[0]}
set -e

python3 "$HERE/parse_teamcity.py" "$OUT/results.json" "$OUT/teamcity.log"
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
