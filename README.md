# Cassandra to MySQL Snapshot Tool

A PySpark-based tool to reliably snapshot data from Apache Cassandra to MySQL with persistent checkpointing and automatic retry.

## Features

- **Full Token Ring Coverage**: Uses mathematical splitting of Murmur3 token range (-2^63 to 2^63-1) into 256 contiguous segments.
- **Persistent Checkpointing**: Progress is saved to MySQL, allowing job resume after failures.
- **Write-Ahead Log (WAL)**: Tracks batch processing status for debugging and audit.
- **Automatic Retry**: Handles MySQL deadlocks with exponential backoff.
- **Per-Range Verification**: Validates row counts to detect data loss during streaming.
- **NULL Handling**: Sanitizes NULL values from Cassandra to satisfy MySQL NOT NULL constraints.

## Prerequisites

- **Python 3.7+**
- **Apache Spark 3.3.0** (bin-without-hadoop)
- **Java 11**
- **Dependencies**:
  ```bash
  pip install pyspark cassandra-driver mysql-connector-python
  ```

## Quick Start

### 1. Configure

Copy the example config and edit with your credentials:

```bash
cp config_example.sh config.sh
vim config.sh
```

Key settings in `config.sh`:
- `CASS_HOSTS`: Comma-separated Cassandra hosts
- `MYSQL_HOST`: MySQL host
- `KEYSPACE`, `TABLE`: Source table
- `PARALLELISM`: Number of parallel workers

### 2. Choose Your Version

| Version | File | Use When |
|---------|------|----------|
| **PySpark** | `run_snapshot_v3.sh` | Large clusters, YARN/Mesos, distributed processing |
| **Pure Python** | `run_pure_python.sh` | Single machine, no Spark, simpler setup |

#### Option A: PySpark Version (Distributed)
```bash
chmod +x run_snapshot_v3.sh
./run_snapshot_v3.sh
```

#### Option B: Pure Python Version (No Spark)
```bash
chmod +x run_pure_python.sh
./run_pure_python.sh
```

### 3. Verify Results

Check the validation table in MySQL:
```sql
SELECT * FROM snapshot_validation;
```

Expected output (when successful):
```
+------------+-----------------+-------------+------+--------+---------------------+
| table_name | cassandra_count | mysql_count | diff | status | created_at          |
+------------+-----------------+-------------+------+--------+---------------------+
| files      |         3433970 |     3433970 |    0 | OK     | 2025-12-17 12:00:00 |
+------------+-----------------+-------------+------+--------+---------------------+
```

## Architecture

```
┌─────────────────┐     Token Ranges     ┌─────────────────┐
│  Cassandra      │◄────────────────────►│  Spark Driver   │
│  (Source)       │                      │  (Coordinator)  │
└────────┬────────┘                      └────────┬────────┘
         │                                        │
         │ SELECT ... WHERE token(id) > X        │ Parallelize
         │                AND token(id) <= Y     │ (256 ranges)
         ▼                                        ▼
┌─────────────────┐                      ┌─────────────────┐
│  Spark Executor │                      │  MySQL          │
│  (Worker)       │─────────────────────►│  (Destination)  │
│                 │   INSERT IGNORE      │                 │
└─────────────────┘                      └─────────────────┘
```

## MySQL Tables Created

| Table                  | Purpose                              |
|------------------------|--------------------------------------|
| `snapshot_checkpoints` | Tracks progress per token range      |
| `snapshot_wal`         | Write-ahead log for batch operations |
| `snapshot_validation`  | Post-migration count comparison      |
| `<your_table>`         | Destination data table               |

## Files

| File                                  | Description                          |
|---------------------------------------|--------------------------------------|
| `snapshot_cassandra_persistent_v3.py` | Main PySpark job                     |
| `snapshot_only_python.py`             | Pure Python version (no Spark)       |
| `config.sh`                           | Configuration (gitignored)           |
| `config_example.sh`                   | Example configuration                |
| `run_snapshot_v3.sh`                  | Runner for PySpark version           |
| `run_only_python.sh`                  | Runner for pure Python version       |
| `verify_missing_ids.sh`               | Diagnostic: find missing IDs         |
| `find_missing_ids.py`                 | Diagnostic script                    |
| `Dockerfile`                          | Container with Spark + Kerberos      |

## Troubleshooting

### Data Mismatch (diff > 0)

1. **Live Traffic**: If Cassandra receives writes during the job, the final count will differ.
2. **Run Diagnostic**:
   ```bash
   ./verify_missing_ids.sh
   ```
   This shows exactly which IDs are missing and their token values.

### MySQL Deadlocks

The script automatically retries (5 attempts with exponential backoff). If failures persist:
- Reduce `PARALLELISM` in config
- Check MySQL lock wait timeout settings

### Connection Issues

- Ensure all Cassandra hosts are reachable
- Verify MySQL credentials and network access

## License

MIT
