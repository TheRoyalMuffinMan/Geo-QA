from flask import Flask, request, jsonify
from lib.globals import *
from lib.database import *
import subprocess
import os

app = Flask(__name__)

@app.route('/process', methods=['POST'])
def process_task():
    task = request.json.get('task')
    result = f"Processed task: {task}"  # Simulated processing logic
    return jsonify({"result": result})

@app.route('/setup', methods=['POST'])
def setup():
    table = request.json.get('rows')

def init_worker() -> None:
    # Get initial configurations
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', 5432)
    name = os.getenv('DB_NAME', 'mydb')
    user = os.getenv('DB_USER', 'myuser')
    password = os.getenv('DB_PASSWORD', 'mypassword')
    schema = os.getenv('DB_SCHEMA', 'schema.sql')

    # Setup Database
    db = Database(host, port, name, user, password, schema)

def main() -> None:
    init_worker()
    app.run(host='0.0.0.0', port=5001)

if __name__ == '__main__':
    main()