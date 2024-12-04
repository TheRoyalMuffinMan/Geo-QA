from flask import Flask, request, jsonify, make_response, Response
from lib.globals import *
from lib.database import *
import os
import requests

app = Flask(__name__)
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
    

@app.route('/init', methods=['POST'])
def recieve_init() -> Response:
    data = request.json
    table = Table(data["name"], data["rows"])
    db.insert_rows(table)
    print(table.name)
    return make_response("Success", 200)


def init_worker() -> None:
    global db

    # Get initial configurations
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', 5432)
    name = os.getenv('DB_NAME', 'postgres')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', 'postgres')
    schema = os.getenv('DB_SCHEMA', 'schema.sql')

    # Initialize Database
    db = Database(host, port, name, user, password, schema)

def main() -> None:
    init_worker()
    app.run(host='0.0.0.0', port=5001)

if __name__ == '__main__':
    main()