#!/usr/bin/env bash
# M2 fixture scenario (doris-rs BE: join the cluster and hold tablets). Runs inside the reference
# container while the capture sidecar records. It creates tablets of several kinds, saves the
# reference BE's own header for each (GET /api/meta/header/<id>) and the storage layout, then
# inserts one row into a separate table (the publish that follows makes the FE send
# UPDATE_VISIBLE_VERSION), drops a table, and waits for reports and a get_tablet_stat cycle.
# Files for the fixture directory go to /tmp/scenario-out; capture.sh copies them out.
set -euo pipefail
MYSQL=(mysql -uroot -h127.0.0.1 -P9030 --batch --skip-column-names)
ROOT=/opt/apache-doris/be/storage
OUT=/tmp/scenario-out
rm -rf "$OUT" && mkdir -p "$OUT/headers"

sleep 12 # let heartbeats and the first reports run

"${MYSQL[@]}" -e "
CREATE DATABASE IF NOT EXISTS m2;
CREATE TABLE m2.dup (
    k  INT NOT NULL,
    b  BIGINT,
    v  VARCHAR(32),
    ts DATETIMEV2(3),
    d  DECIMAL(12, 2)
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 2
PROPERTIES (\"replication_num\" = \"1\");
CREATE TABLE m2.mow (
    k INT NOT NULL,
    v VARCHAR(16)
) UNIQUE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES (\"replication_num\" = \"1\", \"enable_unique_key_merge_on_write\" = \"true\");
CREATE TABLE m2.ranged (
    dt DATE NOT NULL,
    k  INT
) DUPLICATE KEY(dt)
PARTITION BY RANGE(dt) (
    PARTITION p1 VALUES LESS THAN (\"2024-01-01\"),
    PARTITION p2 VALUES LESS THAN (\"2025-01-01\")
)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES (\"replication_num\" = \"1\");
ALTER TABLE m2.ranged ADD PARTITION p3 VALUES LESS THAN (\"2026-01-01\");
"

sleep 5 # finishTask for every CREATE

for t in dup mow ranged; do
  for id in $("${MYSQL[@]}" -e "SHOW TABLETS FROM m2.$t" | cut -f1); do
    curl -sf "http://127.0.0.1:8040/api/meta/header/$id?byte_to_base64=true" > "$OUT/headers/$id.json"
  done
done
find "$ROOT" -not -path "$ROOT/meta/*" | sort | sed "s#^$ROOT#\$ROOT#" > "$OUT/layout.txt"

"${MYSQL[@]}" -e "
CREATE TABLE m2.uvv (k INT NOT NULL) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\");
"
sleep 3
for id in $("${MYSQL[@]}" -e "SHOW TABLETS FROM m2.uvv" | cut -f1); do
  curl -sf "http://127.0.0.1:8040/api/meta/header/$id?byte_to_base64=true" > "$OUT/headers/$id.json"
done
"${MYSQL[@]}" -e "INSERT INTO m2.uvv VALUES (1);"

"${MYSQL[@]}" -e "DROP TABLE m2.mow FORCE;"

sleep 80 # tablet report (60 s), get_tablet_stat (60 s), trash move for the drop
{ echo "# after drop"; find "$ROOT" -not -path "$ROOT/meta/*" | sort | sed "s#^$ROOT#\$ROOT#"; } >> "$OUT/layout.txt"
echo "scenario complete"
