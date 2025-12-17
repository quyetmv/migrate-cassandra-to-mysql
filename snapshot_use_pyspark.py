#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time
from functools import partial
from typing import List, Dict, Tuple

from pyspark.sql import SparkSession

# =========================
# CONSTANTS
# =========================
MIN_TOKEN = -9223372036854775808
MAX_TOKEN =  9223372036854775807

TARGET_RANGE_MB = 512
LATENCY_THRESHOLD = 1.5
MIN_BATCH = 500
MAX_BATCH = 5000


# =========================
# CONFIG
# =========================
class SnapshotConfig:
    def __init__(self, cass_args, mysql_args, table, batch_size, parallelism):
        # cass_args = host1,host2;9042;keyspace;user;pass
        c = cass_args.split(";")
        self.cass_hosts = c[0].split(",")
        self.cass_port = int(c[1])
        self.cass_keyspace = c[2]
        self.cass_user = c[3]
        self.cass_pass = c[4]

        # mysql_args = host;3306;db;user;pass
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
# MYSQL
# =========================
def mysql_conn(cfg):
    import mysql.connector
    return mysql.connector.connect(
        host=cfg.mysql_host,
        port=cfg.mysql_port,
        database=cfg.mysql_db,
        user=cfg.mysql_user,
        password=cfg.mysql_pass
    )


def ensure_mysql_tables(cfg):
    conn = mysql_conn(cfg)
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
    CREATE TABLE IF NOT EXISTS snapshot_wal (
        range_start BIGINT,
        range_end   BIGINT,
        batch_id    BIGINT,
        token_from  BIGINT,
        token_to    BIGINT,
        status      ENUM('STARTED','COMMITTED'),
        updated_at  DATETIME,
        PRIMARY KEY (range_start, range_end, batch_id)
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
# GLOBAL TOKEN SPLIT (Guaranteed Full Coverage)
# =========================
def adaptive_token_split(cfg) -> List[Dict]:
    """
    Pure mathematical split of the Murmur3 token ring.
    Ignores size_estimates to guarantee no gaps.
    """
    MIN_TOKEN = -9223372036854775808
    MAX_TOKEN = 9223372036854775807
    split_count = 256
    total_tokens = (1 << 64)
    step = total_tokens // split_count
    
    print(f"Generating {split_count} token ranges (Global Math Split)...")
    
    ranges = []
    current = MIN_TOKEN
    for i in range(split_count):
        start = current
        if i == split_count - 1:
            end = MAX_TOKEN
        else:
            end = start + step
            if end > MAX_TOKEN: end = MAX_TOKEN
        
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
    conn = mysql_conn(cfg)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM snapshot_checkpoints")
    if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO snapshot_checkpoints VALUES (%s,%s,%s)",
            [(r["range_start"], r["range_end"], r["checkpoint"]) for r in ranges]
        )
        conn.commit()
    conn.close()


def fetch_incomplete_ranges(cfg):
    conn = mysql_conn(cfg)
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT range_start, range_end, checkpoint
        FROM snapshot_checkpoints
        WHERE checkpoint < range_end
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def update_checkpoints(cfg, updates):
    conn = mysql_conn(cfg)
    cur = conn.cursor()
    cur.executemany("""
        UPDATE snapshot_checkpoints
        SET checkpoint=%s
        WHERE range_start=%s AND range_end=%s
    """, updates)
    conn.commit()
    conn.close()


# =========================
# SPARK EXECUTOR
# =========================
def process_partition(iterator, cfg_bc, map_bc):
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.query import SimpleStatement
    from cassandra import ConsistencyLevel
    import mysql.connector

    cfg = cfg_bc.value
    m = map_bc.value

    auth = PlainTextAuthProvider(cfg["cass_user"], cfg["cass_pass"])
    cluster = Cluster(cfg["cass_hosts"], port=cfg["cass_port"], auth_provider=auth)
    session = cluster.connect(cfg["cass_keyspace"])

    mysql = mysql.connector.connect(
        host=cfg["mysql_host"],
        port=cfg["mysql_port"],
        database=cfg["mysql_db"],
        user=cfg["mysql_user"],
        password=cfg["mysql_pass"]
    )
    # Set Isolation Level to READ COMMITTED to reduce locking
    mysql.cursor().execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    cur = mysql.cursor()
    mysql.autocommit = False

    batch_size = cfg["batch_size"]
    results = []

    for r in iterator:
        # Remove manual LIMIT/Checkpoint loop. Use Driver Paging.
        # Query the FULL range and let the driver handle paging safely.
        query = f"""
            SELECT {m['select_str']}, token({m['pk']}) AS token_key
            FROM {cfg['cass_keyspace']}.{cfg['table']}
            WHERE token({m['pk']}) > {r['range_start']}
              AND token({m['pk']}) <= {r['range_end']}
        """
    
        # Use larger fetch_size for efficiency
        stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.ONE, fetch_size=2000)
        rows = session.execute(stmt)
        
        batch = []
        max_token = r['range_start'] # Track max token seen
        batch_count = 0
        rows_processed = 0 # Track actual rows seen
        
        start_range = r['range_start']
        end_range = r['range_end']

        for row in rows:
            rows_processed += 1
            # Sanitize Row to handle NULLs in NOT NULL columns
            # cass_cols: ["id", "client_name", "client_zone", "cluster", "duration", "ext", "fid", "height", "mime", "name", "size", "type", "width", "modified"]
            # MySQL NOT NULL: client_name, client_zone, fid, name, modified
            
            c_vals = []
            for i, col_name in enumerate(m["cass_cols"]):
                val = getattr(row, col_name)
                
                # Default mapping for None values
                if val is None:
                    if col_name in ["client_name", "client_zone", "fid", "name"]:
                        val = ""
                    elif col_name in ["duration", "size", "height", "width"]:
                        val = None # MySQL allows NULL
                    elif col_name == "modified":
                         import datetime
                         val = datetime.datetime.now()
                
                c_vals.append(val)
                
            batch.append(tuple(c_vals))
            max_token = row.token_key
            
            if len(batch) >= batch_size:
                batch_id = int(time.time() * 1000) + batch_count # unique ID approximation
                batch_count += 1
                
                # Insert Batch
                _insert_batch_safe(cur, m["insert_sql"], batch, start_range, end_range, batch_id, max_token, mysql)
                batch = []

        # Insert remaining rows
        if batch:
            batch_id = int(time.time() * 1000) + batch_count
            _insert_batch_safe(cur, m["insert_sql"], batch, start_range, end_range, batch_id, max_token, mysql)

        # ========= VERIFICATION: Compare expected vs actual =========
        # Query expected count for this range
        count_query = f"SELECT count(*) FROM {cfg['cass_keyspace']}.{cfg['table']} WHERE token({m['pk']}) > {start_range} AND token({m['pk']}) <= {end_range}"
        expected_count_row = session.execute(SimpleStatement(count_query, consistency_level=ConsistencyLevel.ONE)).one()
        expected_count = expected_count_row[0] if expected_count_row else 0
        
        if rows_processed != expected_count:
            print(f"WARNING: Range [{start_range}, {end_range}] expected {expected_count} rows, but processed {rows_processed}. Diff: {expected_count - rows_processed}")
            # Do NOT mark as complete. Raise to trigger Spark retry.
            raise RuntimeError(f"Data integrity failure: expected {expected_count}, got {rows_processed}")
        
        # Success! Mark range as fully done by setting checkpoint = match range_end
        # This prevents infinite loops if range was empty or max_token < range_end
        results.append((end_range, start_range, end_range))

    mysql.close()
    session.shutdown()
    cluster.shutdown()
    return iter(results)

def _insert_batch_safe(cur, insert_sql, batch, start, end, batch_id, max_token, mysql_conn):
    # Retry Helper
    import time
    import mysql.connector
    
    MAX_RETRIES = 5
    retries = 0
    success = False
    
    while retries < MAX_RETRIES and not success:
        try:
            # WAL START
            cur.execute("""
                INSERT INTO snapshot_wal
                VALUES (%s,%s,%s,%s,%s,'STARTED',NOW())
            """, (start, end, batch_id, start, max_token)) # Checkpoint in WAL is approx start of range or prev token? Just log start for now.
            
            # BATCH INSERT
            cur.executemany(insert_sql, batch)
            
            # WAL COMMIT
            cur.execute("""
                UPDATE snapshot_wal
                SET status='COMMITTED', updated_at=NOW()
                WHERE range_start=%s AND range_end=%s AND batch_id=%s
            """, (start, end, batch_id))
            
            mysql_conn.commit()
            success = True
            
        except mysql.connector.Error as err:
             if err.errno == 1213 or err.errno == 1205: # Deadlock / Lock Wait
                 mysql_conn.rollback()
                 retries += 1
                 sleep_time = 0.5 * (2 ** retries)
                 print(f"MySQL Deadlock/LockWait. Retrying {retries}/{MAX_RETRIES} in {sleep_time}s...")
                 time.sleep(sleep_time)
             else:
                 print(f"MySQL Error: {err}")
                 mysql_conn.rollback()
                 raise err
        except Exception as e:
            print(f"Unknown Error: {e}")
            mysql_conn.rollback()
            raise e
    
    if not success:
        raise RuntimeError(f"Failed to process batch after {MAX_RETRIES} retries.")


# =========================
# VALIDATION
# =========================
def count_cassandra_partition(iterator, cfg_bc, map_bc):
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.query import SimpleStatement
    from cassandra import ConsistencyLevel
    
    cfg = cfg_bc.value
    m = map_bc.value
    
    auth = PlainTextAuthProvider(cfg["cass_user"], cfg["cass_pass"])
    cluster = Cluster(cfg["cass_hosts"], port=cfg["cass_port"], auth_provider=auth)
    session = cluster.connect(cfg["cass_keyspace"])
    
    total = 0
    for r in iterator:
        start, end = r["range_start"], r["range_end"]
        # Use simple count(*) optimization if available or row scan
        query = f"SELECT count(*) FROM {cfg['cass_keyspace']}.{cfg['table']} WHERE token({m['pk']}) > {start} AND token({m['pk']}) <= {end}"
        row = session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ONE)).one()
        if row:
            total += row[0]
            
    session.shutdown()
    cluster.shutdown()
    return iter([total])

def validate_counts(sc, cfg, ranges):
    print("Starting Post-Migration Validation...")
    
    # 1. MySQL Count
    conn = mysql_conn(cfg)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {cfg.table}")
    mysql_count = cur.fetchone()[0]
    
    # 2. Cassandra Count (Distributed)
    # Re-use the ranges for splitting the work
    cfg_bc = sc.broadcast(cfg.__dict__)
    # Need minimal mapping for count
    map_bc = sc.broadcast({"pk": "id"}) # Assuming PK is known or derived. In V3 main it was hardcoded 'id'
    
    print("Counting Cassandra rows (Distributed)...")
    rdd = sc.parallelize(ranges, cfg.parallelism)
    cass_count = rdd.mapPartitions(partial(count_cassandra_partition, cfg_bc=cfg_bc, map_bc=map_bc)).reduce(lambda x, y: x + y)
    
    # 3. Compare
    diff = abs(cass_count - mysql_count)
    status = 'OK' if diff == 0 else 'MISMATCH'
    
    print(f"Validation Result: Cassandra={cass_count}, MySQL={mysql_count}, Diff={diff}, Status={status}")

    cur.execute("""
        INSERT INTO snapshot_validation
        (table_name, cassandra_count, mysql_count, diff, status, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """, (cfg.table, cass_count, mysql_count, diff, status))

    conn.commit()
    conn.close()


# =========================
# MAIN
# =========================
def main():
    if len(sys.argv) != 6:
        print("""
Usage:
  python snapshot_cassandra_to_mysql_v3.py \
    "<cass_hosts;port;keyspace;user;pass>" \
    "<mysql_host;port;db;user;pass>" \
    <table> <batch_size> <parallelism>
""")
        sys.exit(1)

    cfg = SnapshotConfig(*sys.argv[1:6])

    spark = SparkSession.builder.appName("Cassandra Snapshot v3").getOrCreate()
    sc = spark.sparkContext

    ensure_mysql_tables(cfg)
    
    # === FRESH START: Truncate control tables as requested ===
    def truncate_control_tables(cfg):
        conn = mysql_conn(cfg)
        cur = conn.cursor()
        print("Truncating control tables (snapshot_checkpoints, snapshot_wal)...")
        # Disable FK checks just in case, though not needed for these
        cur.execute("SET FOREIGN_KEY_CHECKS=0") 
        try:
            cur.execute("TRUNCATE TABLE snapshot_checkpoints")
            cur.execute("TRUNCATE TABLE snapshot_wal")
            cur.execute("TRUNCATE TABLE snapshot_validation")
        except Exception as e:
            print(f"Truncate Warning: {e}")
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.close()
        
    truncate_control_tables(cfg)
    # ========================================================

    ranges = adaptive_token_split(cfg)
    seed_ranges_if_empty(cfg, ranges)

    # ====== COLUMN MAPPING (SỬA Ở ĐÂY) ======
    mapping = {
        "cass_cols": ["id", "client_name", "client_zone", "cluster", "duration", "ext", "fid", "height", "mime", "name", "size", "type", "width", "modified"],
        "select_str": "id, client_name, client_zone, cluster, duration, ext, fid, height, mime, name, size, type, width, modified",
        "pk": "id",
        "insert_sql": f"""
            INSERT IGNORE INTO {cfg.table}(file_id,client_name,client_zone,cluster,duration,ext,fid,height,mime,name,size,type,width,modified)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
    }
    # =======================================

    cfg_bc = sc.broadcast(cfg.__dict__)
    map_bc = sc.broadcast(mapping)

    while True:
        ranges = fetch_incomplete_ranges(cfg)
        if not ranges:
            break

        rdd = sc.parallelize(ranges, cfg.parallelism)

        updates = (
            rdd
            .mapPartitions(partial(process_partition, cfg_bc=cfg_bc, map_bc=map_bc))
            .collect()
        )

        update_checkpoints(cfg, updates)
        
    # Re-fetch full ranges for validation or reuse initial ranges?
    # Initial ranges cover the whole ring, so they are safe for validation count.
    # Note: If ranges were adaptive, make sure we use the same definition (start, end)
    # The 'ranges' variable from adaptive_token_split(cfg) is what we need.
    # However, 'ranges' variable in the loop is overwritten by fetch_incomplete_ranges.
    # Effectively we need the 'original' complete list of ranges.
    
    # Recalculate original split for validation
    full_ranges = adaptive_token_split(cfg)
    validate_counts(sc, cfg, full_ranges)
    spark.stop()


if __name__ == "__main__":
    main()
