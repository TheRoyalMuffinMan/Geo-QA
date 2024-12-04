from flask import Flask, request, jsonify, make_response
from lib.globals import *
from lib.database import *
import os
import subprocess
import requests
import configparser
import json

app = Flask(__name__)

app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024

db = None
workers = []
worker_ids = []
tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

@app.route('/send_task', methods=['POST'])
def send_task():
    query = request.json.get('query')
    tables = request.json.get('tables')
    type = request.json.get('type')
    query_id = request.json.get('query_id')
    results = {}
    for worker_url in workers:
        if type == "ResponseType.DATA":
            response = requests.post(f"{worker_url}/process_data", 
                                     json={
                                        "tables": tables,
                                        "agg_url": f"http://aggregator:5001/receive_data"
                                        })
        else:
            response = requests.post(f"{worker_url}/process_query", 
                                     json={
                                        "query": query,
                                        "agg_url": f"http://aggregator:5001/receive_result",
                                        "query_id": query_id,
                                        "worker_id": worker_ids[workers.index(worker_url)]
                                        })
    if type == "ResponseType.DATA":
        # Run the query on the aggregator
        results = db.execute_query(query)
        
        # Write results to json file with query_id
        with open(f"query-results/{query_id}_aggregator.json", "w") as f:
            f.write(json.dumps(results))
        
    return jsonify(results)

@app.route('/receive_result', methods=['POST'])
def receive_result():
    data = request.json
    results = data["results"]
    query_id = data["query_id"]
    worker_id = data["worker_id"]
    
    # write results to json file with query_id
    with open(f"query-results/{query_id}_worker_{worker_id}.json", "w") as f:
        f.write(json.dumps(results))
    
    return make_response("Success", 200)

@app.route('/receive_data', methods=['POST'])
def receive_data():
    table = request.json.get('name')
    rows = request.json.get('rows')
    # Insert data into database
    db.insert_rows(Table(table, rows))
    return make_response("Success", 200)

def send_init():
    worker_id = 0
    for table in tables:
        if table == "lineitem":
            for batch in db.fetch_all(table):
                response = requests.post(
                    f"{workers[worker_id]}/init", 
                    json={"name": table, "rows": batch}
                )
                print(response)
                if response.status_code != 200:
                    print("Error sending init to worker")
                worker_id = (worker_id + 1) % len(workers)
        else:
            for worker_url in workers:
                for batch in db.fetch_all(table):
                    response = requests.post(
                        f"{worker_url}/init", 
                        json={"name": table, "rows": batch}
                    )
                    if response.status_code != 200:
                        print("Error sending init to worker")
                        
    # Remove rows from database
    db.delete_rows("lineitem")

def init_aggregator() -> Database:
    global db, workers, worker_ids
    
    # Get initial configurations
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', 5432)
    name = os.getenv('DB_NAME', 'postgres')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', 'postgres')
    schema = os.getenv('DB_SCHEMA', 'schema.sql')

    # Determines how make workers were generated with system-nodes.sh
    # References the config.ini file to figure this out
    config = configparser.ConfigParser()
    config.read('config.ini')
    number_of_workers = int(config['AGGREGATOR']['number_of_workers'])
    worker_port = int(config['AGGREGATOR']['port'])
    for w in range(1, number_of_workers + 1):
        workers.append(DEFAULT_WORKER_NAME + f"{w}:{worker_port}")
        worker_ids.append(str(w))

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