from kafka import KafkaConsumer
import json

# Kafka settings
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'spotify_streams'

# Create the Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    group_id='spotify-group',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Listening for messages
print("Consumer is listening for messages...")

for message in consumer:
    print(f"Received: {message.value}")  # Print the message from the topic
