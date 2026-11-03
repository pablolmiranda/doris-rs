#!/usr/bin/env bash
# Load corpus tables into database `golden` on the reference cluster from corpus/tables/*.sql.
#   load.sh                 recreate the whole database, every table file in name order
#   load.sh t1 t2 ...       (re)load only the named tables, leaving the rest of `golden` alone
# Fails on the first SQL error.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORACLE="$(cd "$HERE/../.." && pwd)"
COMPOSE=(docker compose -f "$ORACLE/compose/reference-single.yml")
MYSQL=("${COMPOSE[@]}" exec -T doris mysql -uroot -h127.0.0.1 -P9030)

if [[ $# -eq 0 ]]; then
  "${MYSQL[@]}" -e "DROP DATABASE IF EXISTS golden; CREATE DATABASE golden;"
  files=("$HERE"/../tables/*.sql)
else
  "${MYSQL[@]}" -e "CREATE DATABASE IF NOT EXISTS golden;"
  files=()
  for t in "$@"; do
    f=("$HERE"/../tables/*_"$t".sql)
    [[ -f "${f[0]}" ]] || { echo "error: no table file for $t" >&2; exit 1; }
    "${MYSQL[@]}" -e "DROP TABLE IF EXISTS golden.$t;"
    files+=("${f[0]}")
  done
fi
for sql in "${files[@]}"; do
  echo "loading $(basename "$sql")"
  { echo "USE golden;"; cat "$sql"; } | "${MYSQL[@]}"
done
"${MYSQL[@]}" -e "SHOW TABLES FROM golden;"
