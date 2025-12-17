#!/bin/bash
# =============================================
# Diagnostic: Find Missing IDs
# =============================================

# Load shared config
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

# ================= LOG =================
echo "=============================================="
echo "Running Missing IDs Diagnostic"
echo "----------------------------------------------"
echo "Cassandra : ${CASS_HOSTS} (${KEYSPACE}.${TABLE})"
echo "MySQL     : ${MYSQL_HOST} (${MYSQL_DB}.${TABLE})"
echo "Limit     : 100000 IDs"
echo "=============================================="

# ================= RUN DIAGNOSTIC =================
python3 find_missing_ids.py "${ARGS_CASSANDRA}" "${ARGS_MYSQL}" "${TABLE}" 100000