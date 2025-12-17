
import mysql.connector
import sys

# Config
MYSQL_HOST="10.110.84.78"
MYSQL_PORT=3306
MYSQL_DB="seaweedfs"
MYSQL_USER="cassandra2sql"
MYSQL_PASS="IqGEW71N8apjeR8a^*^"

def check_table():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DB,
            user=MYSQL_USER,
            password=MYSQL_PASS
        )
        cursor = conn.cursor()
        
        # 1. Count Total
        cursor.execute("SELECT count(*) FROM snapshot_checkpoints")
        total = cursor.fetchone()[0]
        
        # 2. Count Done
        cursor.execute("SELECT count(*) FROM snapshot_checkpoints WHERE checkpoint = range_end")
        done = cursor.fetchone()[0]
        
        # 3. Check for Wrapped
        cursor.execute("SELECT count(*) FROM snapshot_checkpoints WHERE range_start > range_end")
        wrapped = cursor.fetchone()[0]
        
        print(f"Total Ranges: {total}")
        print(f"Completed Ranges: {done}")
        print(f"Wrapped Ranges: {wrapped}")
        
        if total > 0 and total == done:
             print("CONCLUSION: All ranges are marked as COMPLETED. This confirms 'Poisoned Checkpoints' hypothesis.")
             print("You must DROP TABLE snapshot_checkpoints to rescan.")
             
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_table()
