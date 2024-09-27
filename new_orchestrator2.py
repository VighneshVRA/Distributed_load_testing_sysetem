from kafka import KafkaConsumer, KafkaProducer
import threading
import json
import statistics
import streamlit as st
import time
# Kafka broker settings
bootstrap_servers = 'localhost:9092'

# Consumer settings
consumer_config = {
    'bootstrap_servers': bootstrap_servers, 'group_id': 'your_consumer_group', 'auto_offset_reset': 'earliest'
}

# Producer settings
producer_config = {
    'bootstrap_servers': bootstrap_servers
}

# Create consumer and producer instances
consumer = KafkaConsumer('heartbeat', 'register', **consumer_config)
consumer_2 = KafkaConsumer('metrics', 'metrics2', 'metrics3','metrics4','metrics5','metrics6','metrics7','metrics8', **consumer_config)
producer = KafkaProducer(**producer_config)
k = 1
metric_list = []


def Consumer_registration(consumer):
    while True:
        for msg in consumer:

            if msg.topic == "register":
                topic = msg.topic
                value = msg.value.decode('utf-8')
                st.text("Orchestrator received message from topic '{}': {}".format(topic, value))

            if msg.topic == "heartbeat":
                topic = msg.topic
                value = msg.value.decode('utf-8')
                st.text("Orchestrator received message from topic '{}': {}".format(topic, value))
            



def test_config(producer, test_id, test_type, delay, number_of_requests):
    message = {
        'test_id': test_id,
        'test_type': test_type,
        'delay': delay,
        'number_of_requests': number_of_requests
    }
    # Convert the dictionary to a JSON string
    json_message = json.dumps(message)

    # Send the JSON string as the value for the 'test_data' topic
    producer.send('test_config', value=json_message.encode('utf-8'))

    # Flush to make sure all messages are sent
    producer.flush()


def trigger(producer, trig_id):
    if trig_id != 0:
        test_id = trig_id
        message = {
            'test_id': test_id,
            'trigger': "YES"
        }
        json_message = json.dumps(message)

        producer.send('trigger', value=json_message.encode('utf-8'))

        # Flush to make sure all messages are sent
        producer.flush()


def metric_calculations(consumer):
    # Initialize an empty list to store the values
        metric_list = []

        for msg in consumer:
            if msg.topic == 'metrics':
                topic = msg.topic
                values = [float(num) for num in msg.value.decode('utf-8').strip(' []').split(',')]
                print("Orchestrator received message from topic '{}': {}".format(topic, values))
                # Extend the metric_list with the values from the current message
                metric_list.extend(values)

            if msg.topic == 'metrics2':
                topic = msg.topic
                values = [float(num) for num in msg.value.decode('utf-8').strip(' []').split(',')]
                print("Orchestrator received message from topic and driver 2 '{}': {}".format(topic, values))
                # Extend the metric_list with the values from the current message
                metric_list.extend(values)

            if msg.topic == 'metrics3':
                topic = msg.topic
                values = [float(num) for num in msg.value.decode('utf-8').strip(' []').split(',')]
                print("Orchestrator received message from topic and driver 3 '{}': {}".format(topic, values))
                # Extend the metric_list with the values from the current message
                metric_list.extend(values)
            if msg.topic == 'metrics4':
                topic = msg.topic
                values = [float(num) for num in msg.value.decode('utf-8').strip(' []').split(',') if num.strip()]
                print("Orchestrator received message from topic and driver 3 '{}': {}".format(topic, values))
                # Extend the metric_list with the values from the current message
                metric_list.extend(values)
            if msg.topic == 'metrics5':
                topic = msg.topic
                values = [float(num) for num in msg.value.decode('utf-8').strip(' []').split(',') if num.strip()]
                print("Orchestrator received message from topic and driver 3 '{}': {}".format(topic, values))
                # Extend the metric_list with the values from the current message
                metric_list.extend(values)
            if msg.topic == 'metrics6':
                topic = msg.topic
                values = [float(num) for num in msg.value.decode('utf-8').strip(' []').split(',') if num.strip()]
                print("Orchestrator received message from topic and driver 3 '{}': {}".format(topic, values))
                # Extend the metric_list with the values from the current message
                metric_list.extend(values)
            if msg.topic == 'metrics7':
                topic = msg.topic
                values = [float(num) for num in msg.value.decode('utf-8').strip(' []').split(',') if num.strip()]
                print("Orchestrator received message from topic and driver 3 '{}': {}".format(topic, values))
                # Extend the metric_list with the values from the current message
                metric_list.extend(values)
            if msg.topic == 'metrics8':
                topic = msg.topic
                values = [float(num) for num in msg.value.decode('utf-8').strip(' []').split(',') if num.strip()]
                print("Orchestrator received message from topic and driver 3 '{}': {}".format(topic, values))
                # Extend the metric_list with the values from the current message
                metric_list.extend(values)
            print("Complete list of metrics:", metric_list)
            mean_latency = statistics.mean(metric_list)
            median_latency = statistics.median(metric_list)
            min_latency = min(metric_list)
            max_latency = max(metric_list)

            metrics_dict = {
                'mean_latency': mean_latency,
                'median_latency': median_latency,
                'min_latency': min_latency,
                'max_latency': max_latency
            }
            st.write("final list: ", metrics_dict)
            #display_metrics(metrics_dict)


def display_metrics(metrics_dict):
    metrics_placeholder.text("Mean Latency: {:.2f}".format(metrics_dict['mean_latency']))
    metrics_placeholder.text("Median Latency: {:.2f}".format(metrics_dict['median_latency']))
    metrics_placeholder.text("Min Latency: {:.2f}".format(metrics_dict['min_latency']))
    metrics_placeholder.text("Max Latency: {:.2f}".format(metrics_dict['max_latency']))


# Streamlit UI
st.title("Kafka Test Orchestrator")

# Add test details
st.header("Add Test Details")
test_id = st.text_input("Test ID")
test_type = st.text_input("Test Type")
delay = st.text_input("Delay")
number_of_requests = st.text_input("Number of Requests")

add_button = st.button("Add Test", key="add_test")
if add_button:
    test_config(producer, test_id, test_type, delay, number_of_requests)
    st.success("Test details added successfully!")

# Trigger test
st.header("Trigger Test")
trig_id = st.text_input("Test ID to Trigger")
trigger_button = st.button("Trigger Test", key="trigger_test")
if trigger_button:
    trigger(producer, trig_id)
    metric_calculations(consumer)
    st.success(f"Test {trig_id} triggered successfully!")

# Display Outputs
Connect = st.button("Connect", key="connect_test")
if Connect:
    Consumer_registration(consumer)
    
'''METRICS = st.button("Metrics", key="metrics_test")
if METRICS:
    metric_calculations(consumer)'''

st.header("Outputs")
# You can add the code to display outputs from the metrics here
metrics_placeholder = st.empty()

# Close producer
producer.close()

# Uncomment the following code to run the Kafka consumer threads
#consumer_thread = threading.Thread(target=Consumer_registration, args=(consumer,))
#test_config_thread = threading.Thread(target=test_config, args=(producer, test_id, test_type, delay, number_of_requests))
# trigger_thread = threading.Thread(target=trigger, args=(producer, trig_id,))
#metrics_thread = threading.Thread(target=metric_calculations, args=(consumer_2,))
#
#consumer_thread.start()
# test_config_thread.start()
# trigger_thread.start()
#metrics_thread.start()
#
#consumer_thread.join()
# test_config_thread.join()
# trigger_thread.join()
#metrics_thread.join()
# consumer.close()
