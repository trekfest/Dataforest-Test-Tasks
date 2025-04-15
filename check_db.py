import sqlite3

conn = sqlite3.connect("vendr_products.db")
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM products")
count = cursor.fetchone()[0]
print(f"Всего продуктов в базе: {count}")

cursor.execute("SELECT name, category, price_range FROM products")
for row in cursor.fetchall():
    print(row)

conn.close()
