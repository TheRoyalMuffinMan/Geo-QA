from psycopg2.errors import DuplicateDatabase
from psycopg2 import sql
from .globals import *
import subprocess
import psycopg2
import time

class Database:
    def __init__(self, host: str, port: str, db_name: str, user: str, password: str, schema: str) -> None:
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self.schema = schema
        self.__postgres_wait()
        self.__postgres_db_setup()
        self.__load_schema()

    def __repr__(self) -> str:
        return f"Database(host={self.host},port={self.port},db_name={self.db_name},user={self.user}, password={self.password})"
    
    def __postgres_wait(self) -> None:
        print("Waiting for PostgreSQL to start...")
        while True:
            try:
                conn = psycopg2.connect(
                    dbname=self.db_name,
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
                dbname=self.db_name,
                user=self.user,
                password=self.password,
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

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error setting up database: {e}")

    
    def __load_schema(self) -> None:
        # Load schema
        subprocess.run(['psql', '-U', self.user, '-d', self.db_name, '-f', self.schema], check=True)

        # Verify schema is loaded in docker log
        subprocess.run(['psql', '-U', self.user, '-d', self.db_name, '-c', '\\dt'], check=True)
