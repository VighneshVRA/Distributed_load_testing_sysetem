#!/usr/bin/env python3
from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
import json
import uuid
from collections import defaultdict
import time

app = Flask(__name__)

# Kafka producer for sending messages
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Kafka consumer for receiving metrics
metrics_consumer = KafkaConsumer('metrics_reporting', bootstrap_servers='localhost:9092',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Maintain a dictionary to store test statistics
test_statistics = defaultdict(lambda: defaultdict(list))

@app.route('/start_test', methods=['POST'])
def start_test():
    data = request.json
    test_id = str(uuid.uuid4())
    test_type = data.get('test_type')

    if not test_type or test_type not in ["AVALANCHE", "TSUNAMI"]:
        return jsonify({"error": "Invalid test_type"}), 400

    # Additional parameters for testing modes
    test_message_delay = data.get('test_message_delay', 0)  # Default delay is 0

    # Trigger the load test based on the selected mode
    trigger_message = {
        "test_id": test_id,
        "trigger": "YES",
        "test_type": test_type,
        "test_message_delay": test_message_delay
    }
    producer.send('test_trigger', json.dumps(trigger_message).encode('utf-8'))

    return jsonify({"test_id": test_id, "message": "Load test triggered"})

@app.route('/get_statistics', methods=['GET'])
def get_statistics():
    test_id = request.args.get('test_id')

    if not test_id:
        return jsonify({"error": "Test ID is missing"}), 400

    # Retrieve and return test statistics
    stats = test_statistics.get(test_id, {})

    return jsonify(stats)

if __name__ == '__main__':
    # Runtime controller for receiving metrics
    for message in metrics_consumer:
        data = message.value
        test_id = data.get('test_id')
        metrics = data.get('metrics')

        if test_id:
            # Store metrics data
            for node_id, node_metrics in metrics.items():
                test_statistics[test_id][node_id].append(node_metrics)

            # Simulate some processing time before updating statistics
            time.sleep(1)

