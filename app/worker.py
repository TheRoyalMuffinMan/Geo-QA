from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/process', methods=['POST'])
def process_task():
    task = request.json.get('task')
    result = f"Processed task: {task}"  # Simulated processing logic
    return jsonify({"result": result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)