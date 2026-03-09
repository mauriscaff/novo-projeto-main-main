import sqlite3
conn = sqlite3.connect('vmdk_scanner.db')
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cur.fetchall()]
print("Tables:", tables)

# Find scan job table
for t in tables:
    if 'scan' in t.lower() or 'zombie' in t.lower():
        print(f"\n--- {t} (last 3 rows) ---")
        try:
            cur.execute(f"SELECT * FROM {t} ORDER BY rowid DESC LIMIT 3")
            cols = [d[0] for d in cur.description]
            print("Columns:", cols)
            for row in cur.fetchall():
                for col, val in zip(cols, row):
                    print(f"  {col}: {val}")
                print("  ---")
        except Exception as e:
            print(f"Error: {e}")
conn.close()
