from flask import Flask, request, jsonify, make_response, Response
from lib.globals import *
from lib.database import *
import os
import requests
import configparser

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024
worker = None
db = None

@app.route('/process_query', methods=['POST'])
def process_query() -> Response:
    query = request.json.get('query')
    agg_url = request.json.get('agg_url')
    query_id = request.json.get('query_id')
    worker_id = request.json.get('worker_id')
    try: 
        results = db.execute_query(query)
        response = requests.post(agg_url, json={"results": results,
                                                "query_id": query_id,
                                                "worker_id": worker_id})
        return make_response("Success", 200)
    
    except Exception as e:
        return make_response(str(e), 500)
        
    
@app.route('/process_data', methods=['POST'])
def process_data() -> Response:
    tables = request.json.get('tables')
    agg_url = request.json.get('agg_url')
    try:
        for table in tables:
            for batch in db.fetch_all(table):
                response = requests.post(
                    agg_url, 
                    json={"name": table, "rows": batch}
                )
                if response.status_code != 200:
                    print("Error sending data to aggregator")
                    
        return make_response("Success", 200)
    
    except Exception as e:
        return make_response(str(e), 500)
    

@app.route('/receive_init', methods=['POST'])
def receive_init() -> Response:
    global worker

    # Initialize worker instance
    worker.worker_type = WorkerType(request.json.get("worker_type"))
    files = request.json.get("files")
    worker.leader_address = request.json.get("leader_address")
    
    # Get mountpoint
    config = configparser.ConfigParser()
    config.read('config.ini')
    mount_point = config["SHARED"]["mount_point"]

    # Generate a load file for all the tables
    file = open(f'{mount_point}/load.sql', 'w')
    for table in files:
        file.write(f"\copy {table} FROM '{files[table]}' DELIMITER '|' CSV;\n")
    file.close()

    # Load tables
    subprocess.run([f"cd {mount_point} && psql -U {db.user} -d {db.name} -f load.sql"], check=True, shell=True)

    return make_response("Success", 200)


def init_worker() -> None:
    global worker, db

    # Get initial configurations
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', 5432)
    name = os.getenv('DB_NAME', 'postgres')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', 'postgres')
    schema = os.getenv('DB_SCHEMA', 'schema.sql')

    # Initialize Database
    db = Database(host, port, name, user, password, schema)

    # Initialize worker with basic init information
    worker = Worker()

def main() -> None:
    init_worker()
    app.run(host='0.0.0.0', port=5001)

if __name__ == '__main__':
    main()