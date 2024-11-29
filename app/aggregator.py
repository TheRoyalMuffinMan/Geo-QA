from flask import Flask, request, jsonify
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)