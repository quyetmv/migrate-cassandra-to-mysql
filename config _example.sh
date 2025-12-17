#!/bin/bash
# =============================================
# COMMON CONFIGURATION FILE
# All scripts should source this file:
#   source "$(dirname "$0")/config.sh"
# =============================================

# ================= CASSANDRA CONFIG =================
CASS_HOSTS="10.10.87.54,10.10.87.117,10.10.87.232"
CASS_PORT=9042
CASS_USER="cassandra"
CASS_PASS="cassandra"
KEYSPACE="storages"
TABLE="files"

# ================= MYSQL CONFIG =================
MYSQL_HOST="10.10.84.78"
MYSQL_PORT="3306"
MYSQL_DB="seaweedfs"
MYSQL_USER="cassandra2sql"
MYSQL_PASS="EW71N8apjeR8a^*^"

# ================= SPARK JOB CONFIG =================
BATCH_SIZE=5000          # adaptive – sẽ tự co giãn
PARALLELISM=20           # Spark parallelism

# ================= SPARK CLUSTER CONFIG =================
MASTER="yarn"
DEPLOY_MODE="client"
NUM_EXECUTORS=4
EXECUTOR_CORES=2
EXECUTOR_MEMORY="4g"
DRIVER_MEMORY="2g"

# ================= PYTHON ENV =================
export PYSPARK_PYTHON=/opt/miniconda3/envs/spark-cassandra-python3.7/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/miniconda3/envs/spark-cassandra-python3.7/bin/python

# ================= DERIVED ARGS =================
ARGS_CASSANDRA="${CASS_HOSTS};${CASS_PORT};${KEYSPACE};${CASS_USER};${CASS_PASS}"
ARGS_MYSQL="${MYSQL_HOST};${MYSQL_PORT};${MYSQL_DB};${MYSQL_USER};${MYSQL_PASS}"
