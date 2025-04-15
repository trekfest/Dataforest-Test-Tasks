import sqlite3
from threading import Lock

DB_NAME = "books.db"
db_lock = Lock()

def init_db():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS books (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                category TEXT,
                price TEXT,
                availability TEXT,
                rating TEXT,
                image_url TEXT,
                description TEXT,
                upc TEXT,
                product_type TEXT,
                price_excl_tax TEXT,
                price_incl_tax TEXT,
                tax TEXT,
                num_reviews INTEGER
            )
        ''')
        conn.commit()

def save_book(data: dict):
    with db_lock:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO books (
                    title, category, price, availability, rating,
                    image_url, description,
                    upc, product_type, price_excl_tax, price_incl_tax, tax, num_reviews
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data['title'], data['category'], data['price'], data['availability'], data['rating'],
                data['image_url'], data['description'],
                data['product_info'].get('UPC'),
                data['product_info'].get('Product Type'),
                data['product_info'].get('Price (excl. tax)'),
                data['product_info'].get('Price (incl. tax)'),
                data['product_info'].get('Tax'),
                int(data['product_info'].get('Number of reviews', 0))
            ))
            conn.commit()
