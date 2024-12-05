import os
import csv
import psycopg2
from urllib.parse import urlparse

# Fetch DATABASE_URL from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    print("Error: DATABASE_URL environment variable not set.")
    exit()

parsed_url = urlparse(DATABASE_URL)

# Connect to the database
try:
    # Connect to the database using extracted parameters
    conn = psycopg2.connect(
        dbname=parsed_url.path[1:],  # Remove leading '/' from the path
        user=parsed_url.username,
        password=parsed_url.password,
        host=parsed_url.hostname,
        port=parsed_url.port
    )
    print("Connected to the database.")
except psycopg2.OperationalError as e:
    print(f"Error connecting to the database: {e}")
    exit()

def setup_database(conn):
    """Create the necessary tables."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS authors (
                author_id SERIAL PRIMARY KEY,
                author_name VARCHAR(255),
                first_name VARCHAR(255),
                last_name VARCHAR(255)
            );
            CREATE TABLE IF NOT EXISTS categories (
                category_id SERIAL PRIMARY KEY,
                category_name VARCHAR(255)
            );
            CREATE TABLE IF NOT EXISTS publishers (
                publisher_id SERIAL PRIMARY KEY,
                publisher_name VARCHAR(255)
            );
            CREATE TABLE IF NOT EXISTS books (
                book_id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                author_id INTEGER REFERENCES authors(author_id),
                publisher_id INTEGER REFERENCES publishers(publisher_id),
                category_id INTEGER REFERENCES categories(category_id),
                description TEXT
            );
            CREATE TABLE IF NOT EXISTS prices (
                price_id SERIAL PRIMARY KEY,
                book_id INTEGER REFERENCES books(book_id),
                price DECIMAL(10, 2),
                publish_date DATE
            );
        """)
        conn.commit()
        print("Tables created successfully.")

def sanitize_value(value, max_length=255):
    """Sanitize and trim the value."""
    return value.strip()[:max_length] if value else None

def insert_data(conn, table_name, columns, data):
    """Generic function to insert data into a table."""
    with conn.cursor() as cur:
        try:
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join(columns)
            query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
            cur.executemany(query, data)
            conn.commit()
            print(f"Data inserted successfully into {table_name}.")
        except Exception as error:
            print(f"Error inserting data into {table_name}: {error}")
            conn.rollback()

def read_and_load_data(conn, file_path):
    """Read the data from the CSV file and load it into the database."""
    authors, categories, publishers = set(), set(), set()
    books, prices = [], []

    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        print("CSV Headers:", reader.fieldnames)

        for row in reader:
            try:
                author_name = sanitize_value(row['Authors'])
                category_name = sanitize_value(row['Category'])
                publisher_name = sanitize_value(row['Publisher'])
                title = sanitize_value(row['Title'])
                description = sanitize_value(row.get('Description', ''))
                price = row.get('Price Starting With ($)', '0')
                publish_date = row.get('Publish Date (Year)', None)

                price = float(price) if price else 0
                publish_date = f"{publish_date}-01-01" if publish_date else None

                if author_name:
                    authors.add((author_name,))
                if category_name:
                    categories.add((category_name,))
                if publisher_name:
                    publishers.add((publisher_name,))

                books.append((title, author_name, publisher_name, category_name, description))
                prices.append((title, price, publish_date))

            except KeyError as e:
                print(f"Missing column in CSV: {e}")
                continue

    insert_data(conn, "authors", ["author_name"], authors)
    insert_data(conn, "categories", ["category_name"], categories)
    insert_data(conn, "publishers", ["publisher_name"], publishers)

    with conn.cursor() as cur:
        for book in books:
            cur.execute("""
                INSERT INTO books (title, author_id, publisher_id, category_id, description) 
                VALUES (%s, 
                        (SELECT author_id FROM authors WHERE author_name = %s LIMIT 1),
                        (SELECT publisher_id FROM publishers WHERE publisher_name = %s LIMIT 1),
                        (SELECT category_id FROM categories WHERE category_name = %s LIMIT 1),
                        %s)
                ON CONFLICT DO NOTHING;
            """, book)
        conn.commit()
        print("Books inserted successfully.")

    with conn.cursor() as cur:
        for price in prices:
            cur.execute("""
                INSERT INTO prices (book_id, price, publish_date) 
                VALUES (
                    (SELECT book_id FROM books WHERE title = %s LIMIT 1), 
                    %s, %s)
                ON CONFLICT DO NOTHING;
            """, price)
        conn.commit()
        print("Prices inserted successfully.")

def main():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        print("Connected to the database.")
        setup_database(conn)
        read_and_load_data(conn, "C:/Users/Malempati Binduja/Desktop/dataset/BooksDatasetClean.csv")
        conn.close()
        print("Database connection closed.")
    except Exception as e:
        print("Error:", e)

if __name__ == '__main__':
    main()
