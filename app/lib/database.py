from psycopg2.errors import DuplicateDatabase
from psycopg2 import sql
from .globals import *
import subprocess
import psycopg2
import time

class Database:
    def __init__(self, host: str, port: str, name: str, user: str, password: str, schema: str) -> None:
        self.host = host
        self.port = port
        self.name = name
        self.user = user
        self.password = password
        self.schema = schema
        self.__postgres_wait()
        self.__postgres_db_setup()
        self.__load_schema()

    def __repr__(self) -> str:
        return f"Database(host={self.host},port={self.port},name={self.name},user={self.user}, password={self.password})"
    
    def __postgres_wait(self) -> None:
        print("Waiting for PostgreSQL to start...")
        while True:
            try:
                conn = psycopg2.connect(
                    dbname=self.name,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port
                )
                conn.close()
                print("PostgreSQL is ready!")
                break
            except psycopg2.OperationalError:
                time.sleep(2)
    
    def __postgres_db_setup(self) -> None:
        try:
            conn = psycopg2.connect(
                dbname=self.name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            conn.autocommit = True 
            cursor = conn.cursor()

            try:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.name)))
                print(f"Database {self.name} created.")
            except DuplicateDatabase:
                print(f"Database {self.name} already exists.")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error setting up database: {e}")

    def insert_rows(self, table: Table):
        # Open cursor for insertion
        conn = psycopg2.connect(
            dbname=self.name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        cur = conn.cursor()
        
        columns = table.rows[0].keys()
        tup_str = ','.join(['%s' for _ in columns])
        args_str = ','.join(cur.mogrify("(%s)" % tup_str, tuple(row.values())).decode('utf-8') for row in table.rows)
        cur.execute(f"INSERT INTO {table.name} VALUES " + args_str) 

        conn.commit()

        cur.close()
        conn.close()

    def __load_schema(self) -> None:
        # Load schema
        subprocess.run(['psql', '-U', self.user, '-d', self.name, '-f', self.schema], check=True)

        # Verify schema is loaded in docker log
        subprocess.run(['psql', '-U', self.user, '-d', self.name, '-c', '\\dt'], check=True)
        
    
    def fetch_all(self, table_name: str, batch_size: int = 10000) -> list[dict]:
        try:
            conn = psycopg2.connect(
                dbname=self.name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            cursor = conn.cursor()
            
            # Fetch all rows
            query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
            cursor.execute(query)
            # rows = cursor.fetchall()
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break

                # Get column names
                if 'colnames' not in locals():
                    colnames = [desc[0] for desc in cursor.description]

                # Yield rows as dictionaries
                yield [dict(zip(colnames, map(str, list(row)))) for row in rows]
            
            # Get column names
            # colnames = [desc[0] for desc in cursor.description]
            
            # # Convert to list of dictionaries
            # result = [dict(zip(colnames, map(str, list(row)))) for row in rows]
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"Error fetching data from {table_name}: {e}")
            return []
