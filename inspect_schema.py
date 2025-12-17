
import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def inspect(cass_args, table):
    c = cass_args.split(";")
    hosts = c[0].split(",")
    port = int(c[1])
    keyspace = c[2]
    user = c[3]
    password = c[4]

    auth = PlainTextAuthProvider(user, password)
    cluster = Cluster(hosts, port=port, auth_provider=auth)
    session = cluster.connect(keyspace)

    # Get Table Metadata
    t_meta = cluster.metadata.keyspaces[keyspace].tables[table]
    
    print(f"Table: {table}")
    print("Partition Key:", [c.name for c in t_meta.partition_key])
    print("Clustering Key:", [c.name for c in t_meta.clustering_key])
    
    cluster.shutdown()

if __name__ == "__main__":
    # Args: "<host;port;ks;user;pass>" table
    if len(sys.argv) < 3:
        print("Usage: python inspect_schema.py <conn_str> <table>")
        sys.exit(1)
    inspect(sys.argv[1], sys.argv[2])
