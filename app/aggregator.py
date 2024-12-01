from flask import Flask, request, jsonify
from lib.globals import *
from lib.database import *
import os
import subprocess
import requests

app = Flask(__name__)
db = None
tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

WORKERS = ["http://worker_node_1:5001", "http://worker_node_2:5001"]

@app.route('/send_task', methods=['POST'])
def send_task():
    task = request.json.get('task', 'default task')
    results = {}
    for worker_url in WORKERS:
        response = requests.post(f"{worker_url}/process", json={"task": task})
        results[worker_url] = response.json()
    return jsonify(results)

def send_init():
    for worker_url in WORKERS:
        for table in tables:
            response = requests.post(f"{worker_url}/init", json={"name": table, "rows": db.fetch_all(table)})
            print(response)
            if response.status_code != 200:
                print("Error sending init to worker")

def init_aggregator() -> Database:
    global db
    
    # Get initial configurations
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', 5432)
    name = os.getenv('DB_NAME', 'postgres')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', 'postgres')
    schema = os.getenv('DB_SCHEMA', 'schema.sql')

    # Setup Database
    db = Database(host, port, name, user, password, schema)
    
    # Compile dbgen using subprocess
    subprocess.run(["cd TPC-H/dbgen && make"], check=True, shell=True)
    
    # Create .tbl files "./dbgen -s 1"
    subprocess.run(["cd TPC-H/dbgen && ./dbgen -s 1"], check=True, shell=True)
    
    # Data is in load.sql
    subprocess.run(["mv load.sql TPC-H/dbgen"], check=True, shell=True)
    subprocess.run([f"cd TPC-H/dbgen && psql -U {db.user} -d {db.name} -f load.sql"], check=True, shell=True)
    
    
def main():
    init_aggregator()
    send_init()
    app.run(host='0.0.0.0', port=5001)

if __name__ == '__main__':
    main()