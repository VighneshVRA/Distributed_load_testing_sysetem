import time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import requests
import threading
import json
import uuid
import sqlite3
import statistics



node_id = str(uuid.uuid4())

# Kafka broker settings
bootstrap_servers = 'localhost:9092'

# Consumer settings
consumer_config = {
    'bootstrap_servers': bootstrap_servers,
    'group_id': 'driver_group_8',
    'auto_offset_reset': 'earliest'
}

# Producer settings
producer_config = {
    'bootstrap_servers': bootstrap_servers
}

target_server_url = 'http://localhost:5000/endpoint_to_test'

latency_list=[]
stored_test_data = {}

# Create consumer and producer instances for the driver
driver_consumer = KafkaConsumer('test_data','test_config',**consumer_config)
driver_consumer_2 = KafkaConsumer('trigger',**consumer_config)
driver_producer = KafkaProducer(**producer_config)
driver_producer_2 = KafkaProducer(**producer_config)

def create_database():
    conn = sqlite3.connect('test_data8.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS test_data (
            test_id TEXT PRIMARY KEY,
            test_type TEXT,
            delay INTEGER,
            number_of_requests INTEGER
        )
    ''')
    conn.commit()

    return conn

def register():
     data = {
    "node_id": node_id,
    "message_type": "DRIVER_NODE_8"
}
     json_data = json.dumps(data).encode('utf-8')
     driver_producer.send('register', value=json_data)


     heartbeat_thread.start()
     storage_thread.start()
     trigger_thread.start()
     
     #trigger_thread.start()
     storage_thread.join()
     trigger_thread.join()
     heartbeat_thread.join()
     


# Function to produce heartbeat and metrics messages
def produce_heartbeat():
    while True:
        # Produce heartbeat message
        heartbeat_data = {
        "node_id": node_id,
        "heartbeat": "YES",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}
        heartbeat_json = json.dumps(heartbeat_data)

# Send the heartbeat message
        driver_producer.send('heartbeat', value=heartbeat_json.encode('utf-8'))

        time.sleep(10)

def storage(consumer):
    conn = create_database()
    cursor = conn.cursor()

    for msg in consumer:
        if msg.topic == "test_config":
            topic = msg.topic
            value = msg.value.decode('utf-8')
            print("Driver received message from topic '{}': {}".format(topic, value))
            test_data = json.loads(value)
            if 'test_id' in test_data:
                test_id = test_data['test_id']
                stored_test_data[test_id] = test_data
                print(f"Stored test data with test_id={test_id}:", stored_test_data)

                # Insert or replace the test data into the SQLite database
                cursor.execute('''
                    INSERT OR REPLACE INTO test_data (test_id, test_type, delay, number_of_requests)
                    VALUES (?, ?, ?, ?)
                ''', (test_id, test_data['test_type'], test_data['delay'], test_data['number_of_requests']))

                conn.commit()


    conn.close()

def trigger_and_metrics(consumer):
    conn = create_database()
    cursor = conn.cursor()

    while True:
        for msg in consumer:
            if msg.topic == "trigger":
                topic = msg.topic
                value = msg.value.decode('utf-8')
                print("Driver received message from topic '{}': {}".format(topic, value))
                trigger_data = json.loads(value)
                
                if 'test_id' in trigger_data:
                    t_i = trigger_data.get('test_id')
                    print(t_i)

                    # Fetch test data from the SQLite database
                    cursor.execute('''
                        SELECT test_id, test_type, delay, number_of_requests
                        FROM test_data
                        WHERE test_id = ?
                    ''', (t_i,))

                    testing_value = cursor.fetchone()
                    print("value to be tested: ", testing_value)

                    if testing_value:
                        # Create a dictionary with column names as keys
                        # and values corresponding to the test_id
                        result_dict = {
                            'test_id': testing_value[0],
                            'test_type': testing_value[1],
                            'delay': testing_value[2],
                            'number_of_requests': testing_value[3]
                            # Add more columns as needed
                        }
                        
                        print("Resulting dictionary:", result_dict)
                    else:
                        print("Test data with test_id={} not found.".format(t_i))
                    testing_type = result_dict.get('test_type')
                    iterations = result_dict.get('number_of_requests')
                    delay = result_dict.get('delay')
                    if testing_type == 'AVALANCHE':
                        for i in range(iterations):
                            send_http_request(target_server_url,latency_list,testing_type)
                    if testing_type == 'TSUNAMI':
                         for i in range(iterations):
                            send_http_request(target_server_url,latency_list,testing_type)
                            time.sleep(delay)
                         
                    print(latency_list)
                    produce_metrics(latency_list)
                    
        

    conn.close()

def produce_metrics(latency_list):

            metrics_json = json.dumps(latency_list)

# Send the JSON string to the Kafka topic
            driver_producer_2.send('metrics8', value=metrics_json.encode('utf-8'))
            driver_producer_2.flush()

'''send_http_request(target_server_url,latency_list,testing_type)

                    print(latency_list)
                    value_to_send = str(latency_list[0]).encode('utf-8') 
                    driver_producer.send('metrics', value=value_to_send)
                    driver_producer.flush()'''



def send_http_request( url, latency_list, test_type):
            start_time = time.time()
            response = requests.get(url)
            response.raise_for_status()  # Check for HTTP request errors
            end_time = time.time()
            response_time = end_time - start_time
            # Record response time for each request
            latency_list.append(response_time)
            return latency_list
            communication+=1



'''
def produce_metrics(latency_list):
              
            value_to_send = str(latency_list[0]).encode('utf-8')  
            driver_producer.send('metrics', value=value_to_send)
            driver_producer.flush()
   '''         

'''
def consumer_working(consumer):
    for msg in consumer:
        if msg.topic == "test_data":
            topic = msg.topic
            value = msg.value.decode('utf-8')
            print("Driver received message from topic '{}': {}".format(topic, value))
            test_data = json.loads(value)
            if 'test_id' in test_data:
                test_id = test_data['test_id']
                stored_test_data[test_id] = test_data
            print(f"Stored test data with test_id={test_id}:", stored_test_data)

            print("test data stored")
            #send_http_request( target_server_url,latency_list, "test_data")
            
        
        if msg.topic == "trigger":
            topic = msg.topic
            value = msg.value.decode('utf-8')
            print("Driver received message from topic '{}': {}".format(topic, value))
            trig_data = json.loads(value)

            if 'test_id' in trig_data:
                data =  stored_test_data
                print(data)
            print("triggered")
'''
               
               


# Start a separate thread for producing continuous heartbeat and metrics messages
storage_thread = threading.Thread(target=storage, args=(driver_consumer,))
trigger_thread = threading.Thread(target=trigger_and_metrics, args=(driver_consumer_2,))
heartbeat_thread = threading.Thread(target=produce_heartbeat)
heartbeat_thread.daemon = True  # Set the thread as daemon


#heartbeat_thread.start()



register()
    # Close down driver consumer
driver_consumer.close()
    # Wait for the heartbeat thread to finish before exiting

