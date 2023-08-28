import csv
import json
import time
import random
from confluent_kafka import Producer

# Define your Kafka broker and topic
kafka_broker = 'localhost:9092'
kafka_topic = 'aws-cloudwatch-network'

# Function to send data to Kafka
def send_to_kafka(data):
    producer.produce(kafka_topic, key=None, value=data)
    producer.flush()

# Initialize Kafka producer
producer_config = {
    'bootstrap.servers': kafka_broker
}
producer = Producer(producer_config)

# Read and send CSV data
with open('data/sample_network_data.csv', 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        # Convert the row to JSON
        json_data = json.dumps(row)
        print(json_data)
        # Send JSON data to Kafka
        send_to_kafka(json_data.encode('utf-8'))
        time.sleep(1)

# Close Kafka producer
producer.close()