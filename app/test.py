import psycopg2

# Connection parameters (password is not needed due to 'trust' authentication)
db_params = {
    'dbname': 'postgres',  # Database to connect to
    'user': 'postgres',    # Username
    'host': 'localhost',   # Server (localhost if running locally)
    'port': 5432           # PostgreSQL default port
}

try:
    # Establish the connection (no password required)
    conn = psycopg2.connect(**db_params)

    # Create a cursor object to interact with the database
    cur = conn.cursor()

    # Example query: Get the current database name
    cur.execute("SELECT current_database();")
    result = cur.fetchone()
    print(f"Connected to database: {result[0]}")

    # Execute another query (e.g., list all tables in the public schema)
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tables = cur.fetchall()

    # Print the tables in the 'public' schema
    print("Tables in the 'public' schema:")
    for table in tables:
        print(table[0])

    # Close the cursor and the connection
    cur.close()
    conn.close()

except Exception as error:
    print(f"Error: {error}")
