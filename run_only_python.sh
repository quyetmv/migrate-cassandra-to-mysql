#!/bin/bash
# =============================================
# Snapshot Cassandra to MySQL - Pure Python
# (No Spark required)
# =============================================

# Load shared config
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

# ================= LOG =================
echo "=============================================="
echo "Running Pure Python Snapshot"
echo "----------------------------------------------"
echo "Cassandra : ${CASS_HOSTS} (${KEYSPACE}.${TABLE})"
echo "MySQL     : ${MYSQL_HOST} (${MYSQL_DB}.${TABLE})"
echo "BatchSize : ${BATCH_SIZE}"
echo "Parallel  : ${PARALLELISM} threads"
echo "=============================================="

# ================= RUN =================
python3 snapshot_only_python.py \
  "${ARGS_CASSANDRA}" \
  "${ARGS_MYSQL}" \
  "${TABLE}" \
  "${BATCH_SIZE}" \
  "${PARALLELISM}"
