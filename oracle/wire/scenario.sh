#!/usr/bin/env bash
# Wire corpus scenario. Runs inside the reference container while the capture sidecar records:
# DDL that creates and drops tablets, INSERT, SELECT, a stream load, then a pause long enough
# for the periodic BE->FE reports. Every statement here shapes the corpus; change it only with
# a new capture.
set -euo pipefail
MYSQL=(mysql -uroot -h127.0.0.1 -P9030 --batch)

sleep 12 # let heartbeats and the first reports run

"${MYSQL[@]}" -e "
CREATE DATABASE IF NOT EXISTS wire;
CREATE TABLE wire.events (
    k  INT NOT NULL,
    v  VARCHAR(32),
    ts DATETIME
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 2
PROPERTIES (\"replication_num\" = \"1\");
INSERT INTO wire.events VALUES (1, 'a', '2024-01-01 00:00:00'), (2, 'b', '2024-01-01 00:00:01');
SELECT k, v, ts FROM wire.events ORDER BY k;
SELECT count(*), max(ts) FROM wire.events;
"

printf '3,c,2024-01-01 00:00:02\n4,d,2024-01-01 00:00:03\n5,e,2024-01-01 00:00:04\n' > /tmp/wire.csv
result="$(curl -s --location-trusted -u root: -H "column_separator:," -H "label:wire_corpus_load" \
  -H "Expect:100-continue" -T /tmp/wire.csv http://127.0.0.1:8030/api/wire/events/_stream_load)"
echo "$result" | grep -q '"Status": *"Success"' || { echo "stream load failed: $result" >&2; exit 1; }

"${MYSQL[@]}" -e "
SELECT count(*) FROM wire.events;
DROP TABLE wire.events FORCE;
DROP DATABASE wire FORCE;
"

sleep 25 # periodic reports, publish, finishTask for the drop
echo "scenario complete"
