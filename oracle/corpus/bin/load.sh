#!/usr/bin/env bash
# Recreate database `golden` on the reference cluster from corpus/tables/*.sql, in name order.
# Idempotent: drops the database first. Fails on the first SQL error.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORACLE="$(cd "$HERE/../.." && pwd)"
COMPOSE=(docker compose -f "$ORACLE/compose/reference-single.yml")
MYSQL=("${COMPOSE[@]}" exec -T doris mysql -uroot -h127.0.0.1 -P9030)

"${MYSQL[@]}" -e "DROP DATABASE IF EXISTS golden; CREATE DATABASE golden;"
for sql in "$HERE"/../tables/*.sql; do
  echo "loading $(basename "$sql")"
  { echo "USE golden;"; cat "$sql"; } | "${MYSQL[@]}"
done
"${MYSQL[@]}" -e "SHOW TABLES FROM golden;"
