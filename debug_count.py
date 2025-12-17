
import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

# HARDCODED CONFIG - Adjust if needed
CASS_HOSTS = ["10.110.84.78", "10.110.84.79"] # Add all hosts if known, or just one
CASS_PORT = 9042
CASS_USER = "cassandra" # Default, change if needed? Wait, passing arguments to script is better.
CASS_PASS = "cassandra"
CASS_KEYSPACE = "seaweedfs"
CASS_TABLE = "files"

# ARGS: python debug_count.py <user> <pass> <host> <port> <keyspace> <table>
if len(sys.argv) >= 7:
    CASS_USER = sys.argv[1]
    CASS_PASS = sys.argv[2]
    CASS_HOSTS = sys.argv[3].split(',')
    CASS_PORT = int(sys.argv[4])
    CASS_KEYSPACE = sys.argv[5]
    CASS_TABLE = sys.argv[6]

print(f"Connecting to {CASS_HOSTS}...")

auth_provider = PlainTextAuthProvider(username=CASS_USER, password=CASS_PASS)
cluster = Cluster(contact_points=CASS_HOSTS, port=CASS_PORT, auth_provider=auth_provider)
session = cluster.connect(CASS_KEYSPACE)

try:
    # 1. Get Primary Key
    print("Fetching schema...")
    rows = session.execute(f"SELECT * FROM system_schema.columns WHERE keyspace_name = '{CASS_KEYSPACE}' AND table_name = '{CASS_TABLE}'")
    pk_parts = [r.column_name for r in rows if r.kind == 'partition_key']
    pk_col = ",".join(pk_parts)
    print(f"Primary Key: {pk_col}")

    # 2. Get One Range
    print("Fetching one token range...")
    rows = session.execute(f"SELECT range_start, range_end FROM system.size_estimates WHERE keyspace_name = '{CASS_KEYSPACE}' AND table_name = '{CASS_TABLE}' LIMIT 1")
    r = rows.one()
    
    if r:
        start = int(r.range_start)
        end = int(r.range_end)
        print(f"Testing Range: {start} -> {end}")
        
        # 3. Count
        print("Executing COUNT(*)...")
        # Ensure start < end for simple test (ignore wrap logic for this simple test or handle it)
        if start > end:
            print("Picked a wrapped range, using simple sub-range for test...")
            end = 9223372036854775807 # MAX
            
        query = f"SELECT count(*) FROM {CASS_KEYSPACE}.{CASS_TABLE} WHERE token({pk_col}) > {start} AND token({pk_col}) <= {end}"
        print(f"Query: {query}")
        
        stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.ONE, request_timeout=60)
        rows = session.execute(stmt)
        count = rows.one()[0]
        print(f"SUCCESS! Count: {count}")
    else:
        print("No ranges found in system.size_estimates.")

except Exception as e:
    print(f"ERROR: {e}")
finally:
    cluster.shutdown()
