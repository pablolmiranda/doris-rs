#!/usr/bin/env bash
# Settings every checker cluster gets after it starts, before any suite runs.
#   cluster_setup.sh FE_CONTAINER
# Fragment parallelism is pinned because by default the FE derives it from the cores the BE
# reports, which would make plans depend on the machine (Epic 1 spec §4.4).
set -euo pipefail
FE_CONTAINER="${1:?usage: cluster_setup.sh FE_CONTAINER}"
PARALLEL_PIPELINE_TASK_NUM=4
docker exec "$FE_CONTAINER" mysql -uroot -h127.0.0.1 -P9030 -N -e \
  "SET GLOBAL parallel_pipeline_task_num = ${PARALLEL_PIPELINE_TASK_NUM}; SHOW VARIABLES LIKE 'parallel_pipeline_task_num'"
