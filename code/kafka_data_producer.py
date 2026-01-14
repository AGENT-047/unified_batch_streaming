import time
import json
import random
import uuid
import os
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_TOPIC = "events_topic"
KAFKA_SERVER = "kafka:9092"

def get_producer():
    # Wait for Kafka to start up
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("Connected to Kafka!")
            return producer
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            time.sleep(5)

def generate_event():
    current_time = datetime.now().isoformat()
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1000, 99999)}",
        "event_type": random.choice(["view", "click", "purchase", "login"]),
        "event_timestamp": current_time
    }

if __name__ == "__main__":
    producer = get_producer()
    
    try:
        while True:
            event = generate_event()
            producer.send(KAFKA_TOPIC, value=event)
            print(f"Sent: {event}")
            time.sleep(1) # Send every 1 second
    except KeyboardInterrupt:
        print("Stopping producer...")
        producer.close()