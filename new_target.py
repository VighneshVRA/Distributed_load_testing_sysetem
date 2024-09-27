#!/usr/bin/env python3
from flask import Flask, jsonify

app = Flask(__name__)

# Initialize request and response counters
requests_sent = 0
responses_received = 0

# Update the target server URL for testing with Google
target_server_url = 'http://localhost:5000/endpoint_to_test'  # Corrected URL

@app.route('/endpoint_to_test', methods=['GET'])
def test_endpoint():
    global requests_sent, responses_received
    requests_sent += 1

    # Simulate the target server's response
    responses_received += 1
    return jsonify({"message": "This is a test endpoint"})

@app.route('/metrics', methods=['GET'])
def metrics_endpoint():
    global requests_sent, responses_received

    metrics = {
        "requests_sent": requests_sent,
        "responses_received": responses_received
    }

    return jsonify(metrics)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

