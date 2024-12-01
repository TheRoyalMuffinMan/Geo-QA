from flask import Flask, request, jsonify, make_response, Response
from lib.globals import *
from lib.database import *
import os

app = Flask(__name__)
db = None

@app.route('/process', methods=['POST'])
def process_task() -> Response:
    task = request.json.get('task')
    result = f"Processed task: {task}"  # Simulated processing logic
    return make_response("Success", 200)

@app.route('/init', methods=['POST'])
def recieve_init() -> Response:
    data = request.json.get('table')
    table = Table(data.name, data.rows)
    db.insert_rows(table)

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