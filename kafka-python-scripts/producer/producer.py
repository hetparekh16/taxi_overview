import os
import glob
import pandas as pd
from confluent_kafka import Producer, KafkaException
import json
import logging
from confluent_kafka.admin import AdminClient, NewTopic

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Kafka Producer
conf = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'taxi-producer'
}
producer = Producer(conf)
admin_client = AdminClient({'bootstrap.servers': 'kafka:29092'})

def create_topic(topic):
    new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info(f"Topic {topic} created")
        except KafkaException as e:
            logger.error(f"Failed to create topic {topic}: {e}")
            # Topic might already exist, in which case, it's okay to proceed

# Function to read and parse files
def read_and_parse_files(file_pattern):
    all_records = []
    for file_name in glob.glob(file_pattern):
        try:
            taxi_id = os.path.splitext(os.path.basename(file_name))[0]
            df = pd.read_csv(file_name, names=['timestamp', 'longitude', 'latitude'])
            df['taxi_id'] = taxi_id
            all_records.extend(df.to_dict('records'))
            logger.info(f'Successfully read and parsed file: {file_name}')
        except Exception as e:
            logger.error(f'Error reading file {file_name}: {e}')
    return all_records

# Merge and sort records by timestamp
def merge_and_sort_records(records):
    try:
        sorted_records = sorted(records, key=lambda x: x['timestamp'])
        return sorted_records
    except Exception as e:
        logger.error(f'Error sorting records: {e}')
        return []

# Produce records to Kafka with proper error handling and flushing
def produce_to_kafka(records, topic):
    for record in records:
        try:
            producer.produce(topic, key=record['taxi_id'], value=json.dumps(record), callback=delivery_report)
            producer.poll(0)
        except BufferError:
            logger.warning(f'Local producer queue is full ({len(producer)} messages awaiting delivery): try again')
            producer.poll(1)
        except Exception as e:
            logger.error(f'Error producing record to Kafka: {e}')

    # Wait for all messages to be delivered
    producer.flush()

# Delivery report callback
def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Main function
def main():
    file_pattern = 'data/*.txt'  # Make sure this path is correct
    topic = 'taxi-locations'
    
    # Step 1: Create Kafka topic if not exists
    create_topic(topic)
    
    # Step 2: Read and parse files
    records = read_and_parse_files(file_pattern)
    
    # Step 3: Merge and sort records by timestamp
    sorted_records = merge_and_sort_records(records)
    
    # Step 4: Produce sorted records to Kafka
    produce_to_kafka(sorted_records, topic)

if __name__ == "__main__":
    main()
