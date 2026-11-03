#!/usr/bin/env bash
# Capture, from the reference cluster, the exact rows of every table in database `golden`:
#   expected/<table>.tsv      rows in SQL ORDER BY over every column, tab-separated, NULL as the word NULL
#   expected/<table>.count    row count
#   expected/<table>.ddl.sql  SHOW CREATE TABLE output
# Sorting by all columns makes the file independent of tablet layout. Strings in the corpus
# contain no tabs or newlines by construction, so --raw output is unambiguous.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORACLE="$(cd "$HERE/../.." && pwd)"
OUT="$HERE/../expected"
mkdir -p "$OUT"
MYSQL=(docker compose -f "$ORACLE/compose/reference-single.yml" exec -T doris mysql -uroot -h127.0.0.1 -P9030 -N --batch --raw)

tables="$("${MYSQL[@]}" -e "SHOW TABLES FROM golden")"
for t in $tables; do
  cols="$("${MYSQL[@]}" -e "SELECT column_name FROM information_schema.columns WHERE table_schema='golden' AND table_name='$t' ORDER BY ordinal_position" | paste -sd, -)"
  "${MYSQL[@]}" -e "SELECT $cols FROM golden.$t ORDER BY $cols" > "$OUT/$t.tsv"
  "${MYSQL[@]}" -e "SELECT count(*) FROM golden.$t" > "$OUT/$t.count"
  "${MYSQL[@]}" -e "SHOW CREATE TABLE golden.$t" | cut -f2- > "$OUT/$t.ddl.sql"
  printf "%-16s %6s rows  cols=%s\n" "$t" "$(cat "$OUT/$t.count")" "$cols"
done
