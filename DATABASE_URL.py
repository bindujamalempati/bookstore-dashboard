import os
import psycopg2

# Fetch DATABASE_URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

try:
    # Establish connection
    conn = psycopg2.connect(DATABASE_URL)
    print("Connected to the database!")
except Exception as e:
    print(f"Failed to connect: {e}")
finally:
    if conn:
        conn.close()
