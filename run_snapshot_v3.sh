#!/bin/bash
# =============================================
# Snapshot Cassandra to MySQL v3
# =============================================

# Load shared config
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

# ================= LOG =================
echo "=============================================="
echo "Submitting Cassandra Snapshot v3 Job"
echo "----------------------------------------------"
echo "Cassandra : ${CASS_HOSTS} (${KEYSPACE}.${TABLE})"
echo "MySQL     : ${MYSQL_HOST} (${MYSQL_DB}.${TABLE})"
echo "BatchSize : ${BATCH_SIZE} (adaptive)"
echo "Parallel  : ${PARALLELISM}"
echo "=============================================="

# ================= SUBMIT =================
/opt/spark-3.3.0-bin-without-hadoop/bin/spark-submit \
  --master ${MASTER} \
  --deploy-mode ${DEPLOY_MODE} \
  --num-executors ${NUM_EXECUTORS} \
  --executor-cores ${EXECUTOR_CORES} \
  --executor-memory ${EXECUTOR_MEMORY} \
  --driver-memory ${DRIVER_MEMORY} \
  --name "Snapshot_Cassandra_${TABLE}_v3" \
  snapshot_cassandra_persistent_v3.py \
  "${ARGS_CASSANDRA}" \
  "${ARGS_MYSQL}" \
  "${TABLE}" \
  "${BATCH_SIZE}" \
  "${PARALLELISM}"
