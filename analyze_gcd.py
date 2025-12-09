import sqlite3

conn = sqlite3.connect(r"F:\apps\mdm_comics\assets\gcd_dump\2025-12-01.db")
cursor = conn.cursor()

# Get all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = cursor.fetchall()
print("=== TABLES ===")
for t in tables:
    print(t[0])

print("\n=== TABLE ROW COUNTS ===")
for t in tables:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM [{t[0]}]")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"{t[0]}: {count:,}")
    except Exception as e:
        print(f"{t[0]}: ERROR - {e}")

# Key tables for comics
print("\n=== KEY TABLE SCHEMAS ===")
key_tables = ['gcd_issue', 'gcd_series', 'gcd_publisher', 'gcd_story']
for table in key_tables:
    try:
        cursor.execute(f"PRAGMA table_info([{table}])")
        cols = cursor.fetchall()
        print(f"\n--- {table} ---")
        for col in cols:
            print(f"  {col[1]} ({col[2]})")
    except Exception as e:
        print(f"{table}: not found or error - {e}")

# Sample data from gcd_issue
print("\n=== SAMPLE ISSUE DATA ===")
try:
    cursor.execute("SELECT * FROM gcd_issue LIMIT 3")
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    for row in rows:
        print("\n--- Issue ---")
        for i, val in enumerate(row):
            if val:
                print(f"  {cols[i]}: {str(val)[:100]}")
except Exception as e:
    print(f"Error: {e}")

conn.close()
