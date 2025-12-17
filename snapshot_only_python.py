#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cassandra to MySQL Snapshot Tool - Pure Python Version (No Spark)
Uses threading for parallelism instead of PySpark.
"""

import sys
import time
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

# =========================
# CONSTANTS
# =========================
MIN_TOKEN = -9223372036854775808
MAX_TOKEN = 9223372036854775807
NUM_SPLITS = 256

# =========================
# CONFIG
# =========================
class SnapshotConfig:
    def __init__(self, cass_args, mysql_args, table, batch_size, parallelism):
        c = cass_args.split(";")
        self.cass_hosts = c[0].split(",")
        self.cass_port = int(c[1])
        self.cass_keyspace = c[2]
        self.cass_user = c[3]
        self.cass_pass = c[4]

        m = mysql_args.split(";")
        self.mysql_host = m[0]
        self.mysql_port = int(m[1])
        self.mysql_db = m[2]
        self.mysql_user = m[3]
        self.mysql_pass = m[4]

        self.table = table
        self.batch_size = int(batch_size)
        self.parallelism = int(parallelism)

# =========================
# DATABASE CONNECTIONS
# =========================
def get_cassandra_session(cfg):
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    auth = PlainTextAuthProvider(cfg.cass_user, cfg.cass_pass)
    cluster = Cluster(cfg.cass_hosts, port=cfg.cass_port, auth_provider=auth)
    session = cluster.connect(cfg.cass_keyspace)
    return cluster, session

def get_mysql_connection(cfg):
    import mysql.connector
    conn = mysql.connector.connect(
        host=cfg.mysql_host,
        port=cfg.mysql_port,
        database=cfg.mysql_db,
        user=cfg.mysql_user,
        password=cfg.mysql_pass
    )
    return conn

# =========================
# MYSQL SETUP
# =========================
def ensure_mysql_tables(cfg):
    conn = get_mysql_connection(cfg)
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS snapshot_checkpoints (
        range_start BIGINT,
        range_end   BIGINT,
        checkpoint  BIGINT,
        PRIMARY KEY (range_start, range_end)
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS snapshot_validation (
        table_name VARCHAR(255),
        cassandra_count BIGINT,
        mysql_count BIGINT,
        diff BIGINT,
        status ENUM('OK','MISMATCH'),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    conn.commit()
    conn.close()

# =========================
# TOKEN SPLIT
# =========================
def generate_token_ranges() -> List[Dict]:
    total_tokens = (1 << 64)
    step = total_tokens // NUM_SPLITS
    
    ranges = []
    current = MIN_TOKEN
    for i in range(NUM_SPLITS):
        start = current
        if i == NUM_SPLITS - 1:
            end = MAX_TOKEN
        else:
            end = start + step
            if end > MAX_TOKEN:
                end = MAX_TOKEN
        
        ranges.append({
            "range_start": start,
            "range_end": end,
            "checkpoint": start
        })
        current = end
    
    return ranges

# =========================
# CHECKPOINT OPERATIONS
# =========================
def seed_ranges_if_empty(cfg, ranges):
    conn = get_mysql_connection(cfg)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM snapshot_checkpoints")
    if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO snapshot_checkpoints VALUES (%s,%s,%s)",
            [(r["range_start"], r["range_end"], r["checkpoint"]) for r in ranges]
        )
        conn.commit()
    conn.close()

def fetch_incomplete_ranges(cfg) -> List[Dict]:
    conn = get_mysql_connection(cfg)
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT range_start, range_end, checkpoint
        FROM snapshot_checkpoints
        WHERE checkpoint < range_end
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def update_checkpoint(cfg, range_start, range_end, new_checkpoint):
    conn = get_mysql_connection(cfg)
    cur = conn.cursor()
    cur.execute("""
        UPDATE snapshot_checkpoints
        SET checkpoint = %s
        WHERE range_start = %s AND range_end = %s
    """, (new_checkpoint, range_start, range_end))
    conn.commit()
    conn.close()

def truncate_control_tables(cfg):
    conn = get_mysql_connection(cfg)
    cur = conn.cursor()
    print("Truncating control tables...")
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    try:
        cur.execute("TRUNCATE TABLE snapshot_checkpoints")
        cur.execute("TRUNCATE TABLE snapshot_validation")
    except Exception as e:
        print(f"Truncate Warning: {e}")
    cur.execute("SET FOREIGN_KEY_CHECKS=1")
    conn.close()

# =========================
# WORKER FUNCTION
# =========================
def process_range(cfg, r, mapping, lock, progress_counter):
    """Process a single token range - runs in a thread."""
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.query import SimpleStatement
    from cassandra import ConsistencyLevel
    import mysql.connector
    
    start_range = r["range_start"]
    end_range = r["range_end"]
    
    # Cassandra connection
    auth = PlainTextAuthProvider(cfg.cass_user, cfg.cass_pass)
    cluster = Cluster(cfg.cass_hosts, port=cfg.cass_port, auth_provider=auth)
    cass_session = cluster.connect(cfg.cass_keyspace)
    
    # MySQL connection
    mysql_conn = mysql.connector.connect(
        host=cfg.mysql_host,
        port=cfg.mysql_port,
        database=cfg.mysql_db,
        user=cfg.mysql_user,
        password=cfg.mysql_pass
    )
    mysql_conn.cursor().execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    cur = mysql_conn.cursor()
    mysql_conn.autocommit = False
    
    try:
        # Query full range
        query = f"""
            SELECT {mapping['select_str']}
            FROM {cfg.cass_keyspace}.{cfg.table}
            WHERE token({mapping['pk']}) > {start_range}
              AND token({mapping['pk']}) <= {end_range}
        """
        stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.ONE, fetch_size=2000)
        rows = cass_session.execute(stmt)
        
        batch = []
        rows_processed = 0
        
        for row in rows:
            rows_processed += 1
            
            # Sanitize NULLs
            c_vals = []
            for col_name in mapping["cass_cols"]:
                val = getattr(row, col_name)
                if val is None:
                    if col_name in ["client_name", "client_zone", "fid", "name"]:
                        val = ""
                    elif col_name == "modified":
                        val = datetime.datetime.now()
                c_vals.append(val)
            
            batch.append(tuple(c_vals))
            
            if len(batch) >= cfg.batch_size:
                _insert_batch(cur, mysql_conn, mapping["insert_sql"], batch)
                batch = []
        
        # Insert remaining
        if batch:
            _insert_batch(cur, mysql_conn, mapping["insert_sql"], batch)
        
        # Update checkpoint
        update_checkpoint(cfg, start_range, end_range, end_range)
        
        with lock:
            progress_counter[0] += 1
            print(f"[{progress_counter[0]}/{NUM_SPLITS}] Range completed: {rows_processed} rows")
        
    except Exception as e:
        print(f"ERROR in range [{start_range}, {end_range}]: {e}")
        raise e
    finally:
        mysql_conn.close()
        cass_session.shutdown()
        cluster.shutdown()

def _insert_batch(cur, conn, insert_sql, batch, max_retries=5):
    """Insert batch with retry logic for deadlocks."""
    import mysql.connector
    
    retries = 0
    while retries < max_retries:
        try:
            cur.executemany(insert_sql, batch)
            conn.commit()
            return
        except mysql.connector.Error as err:
            if err.errno in (1213, 1205):  # Deadlock / Lock Wait
                conn.rollback()
                retries += 1
                time.sleep(0.5 * (2 ** retries))
            else:
                raise err
    raise RuntimeError(f"Failed after {max_retries} retries")

# =========================
# VALIDATION
# =========================
def validate_counts(cfg, mapping):
    print("Running validation...")
    
    # MySQL count
    mysql_conn = get_mysql_connection(cfg)
    cur = mysql_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {cfg.table}")
    mysql_count = cur.fetchone()[0]
    
    # Cassandra count
    cluster, cass_session = get_cassandra_session(cfg)
    from cassandra.query import SimpleStatement
    from cassandra import ConsistencyLevel
    
    cass_count = 0
    ranges = generate_token_ranges()
    for r in ranges:
        query = f"SELECT count(*) FROM {cfg.cass_keyspace}.{cfg.table} WHERE token({mapping['pk']}) > {r['range_start']} AND token({mapping['pk']}) <= {r['range_end']}"
        row = cass_session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ONE)).one()
        if row:
            cass_count += row[0]
    
    cluster.shutdown()
    
    # Compare
    diff = abs(cass_count - mysql_count)
    status = 'OK' if diff == 0 else 'MISMATCH'
    
    print(f"Validation: Cassandra={cass_count}, MySQL={mysql_count}, Diff={diff}, Status={status}")
    
    cur.execute("""
        INSERT INTO snapshot_validation
        (table_name, cassandra_count, mysql_count, diff, status, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """, (cfg.table, cass_count, mysql_count, diff, status))
    mysql_conn.commit()
    mysql_conn.close()

# =========================
# MAIN
# =========================
def main():
    if len(sys.argv) != 6:
        print("""
Usage:
  python snapshot_pure_python.py \\
    "<cass_hosts;port;keyspace;user;pass>" \\
    "<mysql_host;port;db;user;pass>" \\
    <table> <batch_size> <parallelism>
""")
        sys.exit(1)

    cfg = SnapshotConfig(*sys.argv[1:6])
    
    print(f"=== Cassandra to MySQL Snapshot (Pure Python) ===")
    print(f"Table: {cfg.cass_keyspace}.{cfg.table}")
    print(f"Parallelism: {cfg.parallelism} threads")
    print(f"Batch Size: {cfg.batch_size}")
    
    ensure_mysql_tables(cfg)
    truncate_control_tables(cfg)
    
    ranges = generate_token_ranges()
    seed_ranges_if_empty(cfg, ranges)
    
    # Column Mapping (customize for your table)
    mapping = {
        "cass_cols": ["id", "client_name", "client_zone", "cluster", "duration", "ext", "fid", "height", "mime", "name", "size", "type", "width", "modified"],
        "select_str": "id, client_name, client_zone, cluster, duration, ext, fid, height, mime, name, size, type, width, modified",
        "pk": "id",
        "insert_sql": f"""
            INSERT IGNORE INTO {cfg.table}(file_id,client_name,client_zone,cluster,duration,ext,fid,height,mime,name,size,type,width,modified)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
    }
    
    # Process with thread pool
    lock = threading.Lock()
    progress_counter = [0]
    
    incomplete = fetch_incomplete_ranges(cfg)
    print(f"Processing {len(incomplete)} ranges with {cfg.parallelism} threads...")
    
    with ThreadPoolExecutor(max_workers=cfg.parallelism) as executor:
        futures = [executor.submit(process_range, cfg, r, mapping, lock, progress_counter) for r in incomplete]
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Range failed: {e}")
    
    print("Snapshot complete. Running validation...")
    validate_counts(cfg, mapping)
    print("Done!")

if __name__ == "__main__":
    main()
