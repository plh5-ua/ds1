import sqlite3

with open("schema.sql", "r", encoding="utf-8") as f:
    schema = f.read()

conn = sqlite3.connect("evcentral.db")
conn.executescript(schema)
conn.commit()
conn.close()

print("✅ Base de datos 'evcentral.db' creada correctamente.")
