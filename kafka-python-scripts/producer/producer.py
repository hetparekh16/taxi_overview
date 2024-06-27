import os
import glob
import pandas as pd
from confluent_kafka import Producer
import json

# Configure Kafka Producer
conf = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'taxi-producer'
}
producer = Producer(conf)

# Function to read and parse files
def read_and_parse_files(file_pattern):
    all_records = []
    for file_name in glob.glob(file_pattern):
        taxi_id = os.path.splitext(os.path.basename(file_name))[0]
        df = pd.read_csv(file_name, names=['timestamp', 'latitude', 'longitude'])
        df['taxi_id'] = taxi_id
        all_records.extend(df.to_dict('records'))
    return all_records

# Merge and sort records by timestamp
def merge_and_sort_records(records):
    sorted_records = sorted(records, key=lambda x: x['timestamp'])
    return sorted_records

# Produce records to Kafka with proper error handling and flushing
def produce_to_kafka(records, topic):
    for record in records:
        try:
            producer.produce(topic, key=record['taxi_id'], value=json.dumps(record), callback=delivery_report)
        except BufferError:
            print(f'Local producer queue is full ({len(producer)} messages awaiting delivery): try again')
            producer.poll(1)
    
    # Wait for all messages to be delivered
    producer.flush()

# Delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Main function
def main():
    file_pattern = 'data/*.txt'  # Make sure this path is correct
    topic = 'taxi-locations'
    
    # Step 1: Read and parse files
    records = read_and_parse_files(file_pattern)
    
    # Step 2: Merge and sort records by timestamp
    sorted_records = merge_and_sort_records(records)
    
    # Step 3: Produce sorted records to Kafka
    produce_to_kafka(sorted_records, topic)

if __name__ == "__main__":
    main()