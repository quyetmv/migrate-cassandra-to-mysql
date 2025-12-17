#!/usr/bin/env python3
"""
Diagnostic Script: Find IDs present in Cassandra but missing in MySQL.
Usage: python find_missing_ids.py "<cass_conn>" "<mysql_conn>" <table> <limit>
"""
import sys

def main():
    if len(sys.argv) < 5:
        print("Usage: python find_missing_ids.py '<cass_host;port;ks;user;pass>' '<mysql_host;port;db;user;pass>' <table> <limit>")
        sys.exit(1)
    
    cass_args, mysql_args, table, limit = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
    
    # Parse Cassandra
    c = cass_args.split(";")
    cass_hosts, cass_port, cass_keyspace, cass_user, cass_pass = c[0].split(","), int(c[1]), c[2], c[3], c[4]
    
    # Parse MySQL
    m = mysql_args.split(";")
    mysql_host, mysql_port, mysql_db, mysql_user, mysql_pass = m[0], int(m[1]), m[2], m[3], m[4]
    
    # Connect Cassandra
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    auth = PlainTextAuthProvider(cass_user, cass_pass)
    cluster = Cluster(cass_hosts, port=cass_port, auth_provider=auth)
    cass_session = cluster.connect(cass_keyspace)
    print(f"Connected to Cassandra: {cass_keyspace}")
    
    # Connect MySQL
    import mysql.connector
    mysql_conn = mysql.connector.connect(host=mysql_host, port=mysql_port, database=mysql_db, user=mysql_user, password=mysql_pass)
    mysql_cursor = mysql_conn.cursor()
    print(f"Connected to MySQL: {mysql_db}")
    
    # 1. Get ALL Cassandra IDs (sampling if limit specified)
    print(f"Fetching Cassandra IDs (limit={limit})...")
    cass_ids = set()
    rows = cass_session.execute(f"SELECT id FROM {table} LIMIT {limit}")
    for row in rows:
        cass_ids.add(row.id)
    print(f"  Cassandra IDs fetched: {len(cass_ids)}")
    
    # 2. Check which exist in MySQL
    print("Checking against MySQL...")
    missing_ids = []
    for i, cid in enumerate(cass_ids):
        mysql_cursor.execute(f"SELECT 1 FROM {table} WHERE file_id = %s LIMIT 1", (cid,))
        if not mysql_cursor.fetchone():
            missing_ids.append(cid)
        if (i + 1) % 10000 == 0:
            print(f"  Checked {i+1}/{len(cass_ids)}, missing so far: {len(missing_ids)}")
    
    print(f"\n=== RESULTS ===")
    print(f"Total Cassandra IDs checked: {len(cass_ids)}")
    print(f"Missing in MySQL: {len(missing_ids)}")
    
    if missing_ids:
        print(f"\nSample Missing IDs (first 20):")
        for mid in missing_ids[:20]:
            # Get token for this ID
            token_row = cass_session.execute(f"SELECT token(id) FROM {table} WHERE id = %s", (mid,)).one()
            token_val = token_row[0] if token_row else "N/A"
            print(f"  ID: {mid}, Token: {token_val}")
    
    cluster.shutdown()
    mysql_conn.close()

if __name__ == "__main__":
    main()
