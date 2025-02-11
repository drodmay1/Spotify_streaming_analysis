from kafka import KafkaConsumer
import json

# Define Kafka settings
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "spotify_streams"

# Create a Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Consumer is listening for messages...")

# Process incoming messages
for message in consumer:
    data = message.value
    print(f"Received: {data}")
