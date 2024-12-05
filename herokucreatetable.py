import os
import csv
import psycopg2
from urllib.parse import urlparse

# Fetch DATABASE_URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("Error: DATABASE_URL is not set.")
    print("Ensure the DATABASE_URL environment variable is set in Heroku.")
    exit()

# Parse DATABASE_URL and establish database connection
try:
    parsed_url = urlparse(DATABASE_URL)
    conn = psycopg2.connect(
        dbname=parsed_url.path[1:],  # Remove leading '/'
        user=parsed_url.username,
        password=parsed_url.password,
        host=parsed_url.hostname,
        port=parsed_url.port,
        sslmode='require'
    )
    print("Connected to the database.")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit()

# Function to create necessary tables
def setup_database(conn):
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

# Function to sanitize and trim data
def sanitize_value(value, max_length=255):
    return value.strip()[:max_length] if value else None

# Function to insert data into a table
def insert_data(conn, table_name, columns, data):
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

# Function to read data from CSV and load it into the database
# Function to read data from CSV and load it into the database
def read_and_load_data(conn, file_path):
    authors, categories, publishers = set(), set(), set()
    books, prices = [], []

    print(f"Current working directory: {os.getcwd()}")

    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        print("Ensure the file is deployed to Heroku and is in the correct directory.")
        return

    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        print("CSV Headers:", reader.fieldnames)

        # Validate CSV headers
        required_headers = ['Authors', 'Category', 'Publisher', 'Title', 'Description', 'Price Starting With ($)', 'Publish Date (Year)']
        if not all(header in reader.fieldnames for header in required_headers):
            print(f"Error: CSV file is missing required headers. Expected: {required_headers}")
            return

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

# Main function
def main():
    setup_database(conn)
    # Ensure the file path matches where BooksDatasetClean.csv is deployed
    print("Attempting to load data from BooksDatasetClean.csv...")
    read_and_load_data(conn, "BooksDatasetClean.csv")
    conn.close()
    print("Database setup and data loading complete.")

if __name__ == "__main__":
    main()
