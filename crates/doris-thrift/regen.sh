#!/usr/bin/env bash
# Regenerates src/gen/ from idl/ with the pinned Thrift compiler (0.24.0, the version upstream
# builds with), then applies the patches recorded in patches.md and writes the method table.
# The compiler only exists in the oracle runner image (`make runner` in oracle/).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
IMAGE="${RUNNER_IMAGE:-doris-rs/oracle-runner:17}"
THRIFT=/opt/doris-thirdparty/installed/bin/thrift

docker image inspect "$IMAGE" >/dev/null 2>&1 || {
  echo "runner image $IMAGE missing: run 'make runner' in oracle/" >&2
  exit 1
}

rm -rf "$HERE/src/gen"
mkdir -p "$HERE/src/gen"
# Same flags upstream passes to its cpp and java generators (gensrc/thrift/Makefile).
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$HERE/idl:/idl:ro" -v "$HERE/src/gen:/out" "$IMAGE" \
  "$THRIFT -version && for f in /idl/*.thrift; do $THRIFT -I /idl --gen rs --allow-64bit-consts -strict -out /out \"\$f\" 2>>/tmp/thrift.err || { cat /tmp/thrift.err >&2; exit 1; }; done; sort -u /tmp/thrift.err >&2"

python3 "$HERE/postprocess.py" "$HERE/src/gen"
echo "regenerated $(ls "$HERE/src/gen" | wc -l | tr -d ' ') files in src/gen"
