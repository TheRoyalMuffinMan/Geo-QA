import time
import subprocess
import psycopg2
from .globals import *
from psycopg2 import sql
from psycopg2.errors import DuplicateDatabase

class Database:
    def __init__(self, host: str, port: str, db_name: str, user: str, password: str, schema: str) -> None:
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self.schema = schema
        self.__postgres_wait()
        self.__postgres_setup()
        self.__load_schema()

    def __repr__(self) -> str:
        return f"Database(host={self.host},port={self.port},db_name={self.db_name},user={self.user}, password={self.password})"
    
    def __postgres_wait(self) -> None:
        print("Waiting for PostgreSQL to start...")
        while True:
            try:
                conn = psycopg2.connect(
                    dbname="postgres",
                    user="postgres",
                    password="postgres",
                    host=self.host,
                    port=self.port
                )
                conn.close()
                print("PostgreSQL is ready!")
                break
            except psycopg2.OperationalError:
                time.sleep(2)
    
    def __postgres_setup(self) -> None:
        try:
            conn = psycopg2.connect(
                dbname="postgres",  
                user="postgres",    
                password="postgres",
                host=self.host,
                port=self.port
            )
            conn.autocommit = True 
            cursor = conn.cursor()

            try:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.db_name)))
                print(f"Database {self.db_name} created.")
            except DuplicateDatabase:
                print(f"Database {self.db_name} already exists.")

            try:
                cursor.execute(
                    sql.SQL("CREATE USER {} WITH PASSWORD %s").format(sql.Identifier(self.user)),
                    [self.password]
                )
                print(f"User {self.user} created.")
            except Exception as e:
                print(f"Error creating user: {e}")

            cursor.execute(
                sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {}").format(
                    sql.Identifier(self.db_name),
                    sql.Identifier(self.user)
                )
            )
            print(f"Granted ALL privileges on {self.db_name} to {self.user}.")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error setting up database: {e}")

    
    def __load_schema(self) -> None:
        subprocess.run(['psql', '-U', 'postgres', '-d', self.db_name, '-f', self.schema], check=True)
        subprocess.run(['psql', '-U', 'postgres', '-d', self.db_name, '-c', '\\dt'], check=True)
