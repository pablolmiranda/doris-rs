#!/usr/bin/env bash
# Create (or verify) a git worktree of the upstream Doris source at the pinned release tag.
# The worktree lives in doris-rs/.cache/doris-src and is never committed.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORACLE="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$ORACLE/.." && pwd)"
# shellcheck disable=SC1091
source "$ORACLE/versions.env"

UPSTREAM="${DORIS_UPSTREAM:-$ROOT/../doris}"
SRC="${DORIS_SRC:-$ROOT/.cache/doris-src}"

if [[ ! -d "$UPSTREAM/.git" ]]; then
  echo "error: upstream Doris checkout not found at $UPSTREAM (set DORIS_UPSTREAM)" >&2
  exit 1
fi
if ! git -C "$UPSTREAM" rev-parse -q --verify "refs/tags/$DORIS_SRC_TAG" >/dev/null; then
  echo "error: tag $DORIS_SRC_TAG not present in $UPSTREAM; run: git -C $UPSTREAM fetch --tags" >&2
  exit 1
fi

if [[ -d "$SRC/.git" || -f "$SRC/.git" ]]; then
  have="$(git -C "$SRC" rev-parse HEAD)"
  want="$(git -C "$UPSTREAM" rev-parse "refs/tags/$DORIS_SRC_TAG^{commit}")"
  if [[ "$have" != "$want" ]]; then
    echo "worktree at $SRC is at $have, expected $want ($DORIS_SRC_TAG); re-checking out" >&2
    git -C "$SRC" checkout -q --detach "$want"
  fi
else
  mkdir -p "$(dirname "$SRC")"
  git -C "$UPSTREAM" worktree add -q --detach "$SRC" "refs/tags/$DORIS_SRC_TAG"
fi
echo "doris source: $SRC @ $(git -C "$SRC" rev-parse --short HEAD) ($DORIS_SRC_TAG)"
