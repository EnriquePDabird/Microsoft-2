import sqlite3

db_path = "datamart/metadata.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("""
    SELECT id, title, authors
    FROM books
    WHERE subjects LIKE '%Romance%'
    ORDER BY title ASC
""")
rows = cursor.fetchall()

for row in rows:
    print(row)

conn.close()