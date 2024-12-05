import psycopg2

# Use the DATABASE_URL from Heroku
DATABASE_URL = "postgres://u74dhij8uprqht:p52b027000f786d268ac0eb5aa82fedbe760b2861427634c146126e16b0148403@c97r84s7psuajm.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/dftinkvfbpeu11"

try:
    # Connect to the Heroku PostgreSQL database
    conn = psycopg2.connect(DATABASE_URL)
    print("Connected to the Heroku database!")
except Exception as e:
    print(f"Failed to connect to the database: {e}")
finally:
    conn.close()
