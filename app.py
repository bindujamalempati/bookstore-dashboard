import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import os
from urllib.parse import urlparse, quote

st.set_page_config(page_title="Bookstore Dashboard", page_icon="📚", layout="wide")
st.title("📚 PostgreSQL Bookstore Database Viewer")

# Fetch DATABASE_URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in the environment variables.")

# Preprocess the URL to handle malformed parts
# Preprocess the URL to clean malformed segments
if DATABASE_URL.count(':') > 2:
    parts = DATABASE_URL.split(':')
    if len(parts) >= 3:
        # Reassemble URL assuming last part includes port and database
        username_host = ':'.join(parts[:-2])
        port_database = ':'.join(parts[-2:])
        if not port_database.startswith('//'):
            port_database = f"//{port_database}"  # Ensure proper format
        DATABASE_URL = f"{username_host}:{port_database}"

# Preprocess the DATABASE_URL to fix malformed components
try:
    # Remove invalid parts if URL is malformed
    if "://" in DATABASE_URL:
        url_parts = DATABASE_URL.split("://", 2)  # Split by the first two instances of `://`
        if len(url_parts) > 2:
            # Reconstruct a valid URL
            valid_prefix = url_parts[0] + "://"  # e.g., `postgres://`
            remaining_url = "@".join(url_parts[1:]).replace("://", "")  # Join and clean up remaining parts
            DATABASE_URL = valid_prefix + remaining_url

    parsed_url = urlparse(DATABASE_URL)
    DB_HOST = parsed_url.hostname
    DB_PORT = int(parsed_url.port) if parsed_url.port else 5432  # Default port is 5432
    DB_NAME = parsed_url.path[1:]  # Remove leading slash
    DB_USER = parsed_url.username
    DB_PASSWORD = parsed_url.password

    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        raise ValueError("Missing required database connection parameters.")

    print(f"Host: {DB_HOST}")
    print(f"Port: {DB_PORT}")
    print(f"Database: {DB_NAME}")
    print(f"User: {DB_USER}")

except Exception as e:
    print(f"Error processing DATABASE_URL: {e}")
    st.sidebar.error("Failed to process DATABASE_URL. Please check its format.")
    raise ValueError(f"Malformed DATABASE_URL: {DATABASE_URL}")


# Sidebar: Database connection
st.sidebar.header("Database Connection")
st.sidebar.text(f"Host: {DB_HOST}")
st.sidebar.text(f"Database: {DB_NAME}")
st.sidebar.text(f"User: {DB_USER}")

# Function to establish database connection
def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';")
        tables = cursor.fetchall()
        print("Tables in database:", tables)
        conn.close()
    except Exception as e:
        print("Database connection failed:", e)

def fetch_books_by_category(conn, category_name):
    query = """
    SELECT b.title AS book_title, pr.price, c.category_name
    FROM books b
    JOIN categories c ON b.category_id = c.category_id
    JOIN prices pr ON b.book_id = pr.book_id
    WHERE c.category_name ILIKE %s
    ORDER BY pr.price;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (f"%{' '.join(category_name.split())}%",))
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=["Book Title", "Price", "Category"])
    except Exception as e:
        st.error(f"Error fetching books by category: {e}")
        return None

def fetch_books_and_authors(conn):
    query = """
    SELECT b.book_id, b.title AS book_title, a.author_name, c.category_name, p.publisher_name, pr.price, pr.publish_date
    FROM books b
    JOIN authors a ON b.author_id = a.author_id
    JOIN categories c ON b.category_id = c.category_id
    JOIN publishers p ON b.publisher_id = p.publisher_id
    JOIN prices pr ON b.book_id = pr.book_id
    ORDER BY b.book_id;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=["Book ID", "Book Title", "Author Name", "Category", "Publisher", "Price", "Publish Date"])
    except Exception as e:
        st.error(f"Error fetching books and authors: {e}")
        return None

def fetch_summary_statistics(conn):
    query = """
    SELECT COUNT(DISTINCT b.book_id) AS total_books,
           AVG(pr.price) AS avg_price,
           COUNT(DISTINCT c.category_id) AS total_categories
    FROM books b
    JOIN prices pr ON b.book_id = pr.book_id
    JOIN categories c ON b.category_id = c.category_id;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            if result:
                return {
                    "Total Books": result[0],
                    "Average Price": round(result[1], 2) if result[1] else 0,
                    "Total Categories": result[2],
                }
            else:
                return None
    except Exception as e:
        st.error(f"Error fetching summary statistics: {e}")
        return None

def fetch_most_recent_book_by_category(conn, category_name):
    query = """
    SELECT b.title AS book_title, b.publication_year
    FROM books b
    JOIN categories c ON b.category_id = c.category_id
    WHERE c.category_name ILIKE %s
    ORDER BY b.publication_year DESC
    LIMIT 1;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (f"%{' '.join(category_name.split())}%",))
            result = cur.fetchone()
            if result:
                return {
                    "Book Title": result[0],
                    "Publication Year": result[1]
                }
            else:
                return None
    except Exception as e:
        st.error(f"Error fetching most recent book by category: {e}")
        return None

def fetch_books_by_price_and_category(conn, category_name, max_price):
    query = """
    SELECT b.title AS book_title, pr.price, c.category_name, b.publication_year
    FROM books b
    JOIN categories c ON b.category_id = c.category_id
    JOIN prices pr ON b.book_id = pr.book_id
    WHERE c.category_name ILIKE %s AND pr.price <= %s
    ORDER BY pr.price;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (f"%{' '.join(category_name.split())}%", max_price))
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=["Book Title", "Price", "Category", "Publication Year"])
    except Exception as e:
        st.error(f"Error fetching books by price and category: {e}")
        return None

# Streamlit App


# Initialize session state for connection
if "conn" not in st.session_state:
    st.session_state.conn = None

# Function to establish database connection
def connect_to_database():
    if st.session_state.conn is None:
        st.session_state.conn = get_db_connection()
        if st.session_state.conn:
            st.sidebar.success("Connected to the database successfully!")
        else:
            st.sidebar.error("Failed to connect to the database.")

# Add a button for connecting to the database
if st.sidebar.button("Connect to Database"):
    connect_to_database()

# Tabs for navigation
if st.session_state.conn:
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📖 All Books", "📊 Statistics", "🏆 Most Recent Books", "🔍 Search by Category", "🔎 Search by Price & Category"])

    with tab1:
        st.subheader("All Books and Authors")
        books_and_authors_df = fetch_books_and_authors(st.session_state.conn)
        if books_and_authors_df is not None and not books_and_authors_df.empty:
            st.dataframe(books_and_authors_df)
        else:
            st.warning("No data available for books and authors.")

    with tab2:
        st.subheader("📊 Database Statistics")
        stats = fetch_summary_statistics(st.session_state.conn)
        if stats:
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Books", stats["Total Books"])
            col2.metric("Average Price", f"${stats['Average Price']}")
            col3.metric("Total Categories", stats["Total Categories"])
        else:
            st.warning("No statistics available.")

    with tab3:
        st.subheader("🏆 Most Recent Book by Category")
        category_name = st.text_input("Enter Category Name:", "", key="most_recent_category")
        if category_name.strip():
            most_recent_book = fetch_most_recent_book_by_category(st.session_state.conn, category_name)
            if most_recent_book:
                st.success(f"Most Recent Book in Category `{category_name}`:")
                st.write(f"**Title:** {most_recent_book['Book Title']}")
                st.write(f"**Publication Year:** {most_recent_book['Publication Year']}")
            else:
                st.warning(f"No data found for category `{category_name}`.")
        else:
            st.info("Enter a category name to find the most recent book.")

    with tab4:
        st.subheader("🔍 Search Books by Category")
        category_name = st.text_input("Enter Category Name to search books:", "", key="search_category")
        if category_name.strip():
            books_by_category = fetch_books_by_category(st.session_state.conn, category_name)
            if books_by_category is not None and not books_by_category.empty:
                st.dataframe(books_by_category)
            
                # Add bar chart for price distribution
                fig = px.histogram(books_by_category, x="Price", nbins=50, 
                               title=f"Price Distribution for {category_name} Books")
                st.plotly_chart(fig)
            else:
                st.warning(f"No books found in the `{category_name}` category.")
        else:
            st.info("Enter a category name to search for books.")

    with tab5:
        st.subheader("🔎 Search Books by Price & Category")
        category_name = st.text_input("Enter Category Name:", "", key="price_category")
        max_price = st.number_input("Enter Maximum Price:", min_value=0.0, value=20.0, step=0.1)
        if category_name.strip():
            books_by_price_and_category = fetch_books_by_price_and_category(st.session_state.conn, category_name, max_price)
            if books_by_price_and_category is not None and not books_by_price_and_category.empty:
                st.dataframe(books_by_price_and_category)
            
                # Add scatter plot for price vs. publication year
                fig = px.scatter(books_by_price_and_category, x="Publication Year", y="Price",
                             hover_data=["Book Title"], 
                             title=f"Price vs. Publication Year for {category_name} Books")
                st.plotly_chart(fig)
            else:
                st.warning(f"No books found in the `{category_name}` category with a price less than or equal to {max_price}.")
        else:
            st.info("Enter a category name and price to search for books.")