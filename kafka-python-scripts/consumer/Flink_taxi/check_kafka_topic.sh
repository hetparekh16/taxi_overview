#!/bin/sh

TOPIC="taxi-locations"
BROKER="kafka:29092"

while ! kafka-topics --bootstrap-server $BROKER --list | grep -w $TOPIC > /dev/null; do
    echo "Kafka topic not ready yet..."
    sleep 5
done

echo "Kafka topic is ready."
exit 0
