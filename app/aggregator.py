from flask import Flask, request, jsonify
from lib.globals import *
from lib.database import *
import os
import subprocess
import requests

app = Flask(__name__)

WORKERS = ["http://worker_node_1:5001", "http://worker_node_2:5001"]

@app.route('/send_task', methods=['POST'])
def send_task():
    task = request.json.get('task', 'default task')
    results = {}
    for worker_url in WORKERS:
        response = requests.post(f"{worker_url}/process", json={"task": task})
        results[worker_url] = response.json()
    return jsonify(results)

def init_aggregator() -> None:
    # Get initial configurations
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', 5432)
    name = os.getenv('DB_NAME', 'mydb')
    user = os.getenv('DB_USER', 'myuser')
    password = os.getenv('DB_PASSWORD', 'mypassword')
    schema = os.getenv('DB_SCHEMA', 'schema.sql')

    # Setup Database
    db = Database(host, port, name, user, password, schema)
    
    # Compile dbgen using subprocess
    subprocess.run(["cd TPC-H/dbgen && make"], check=True, shell=True)
    
    # # Create .tbl files "./dbgen -s 1"
    subprocess.run(["cd TPC-H/dbgen && ./dbgen -s 1"], check=True, shell=True)
    
    # Copy data into database
    # psql -U postgres -d tpch_db <<EOF
    # \copy nation FROM 'nation.tbl' DELIMITER '|' CSV;
    # \copy region FROM 'region.tbl' DELIMITER '|' CSV;
    # \copy customer FROM 'customer.tbl' DELIMITER '|' CSV;
    # \copy orders FROM 'orders.tbl' DELIMITER '|' CSV;
    # \copy lineitem FROM 'lineitem.tbl' DELIMITER '|' CSV;
    # \copy part FROM 'part.tbl' DELIMITER '|' CSV;
    # \copy partsupp FROM 'partsupp.tbl' DELIMITER '|' CSV;
    # \copy supplier FROM 'supplier.tbl' DELIMITER '|' CSV;
    # EOF
    
    # Data is in load.sql
    subprocess.run(["mv load.sql TPC-H/dbgen"], check=True, shell=True)
    subprocess.run(["cd TPC-H/dbgen && psql -U postgres -d db -f load.sql"], check=True, shell=True)
    

if __name__ == '__main__':
    init_aggregator()
    app.run(host='0.0.0.0', port=5001)