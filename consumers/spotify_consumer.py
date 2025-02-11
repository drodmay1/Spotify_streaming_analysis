import sys
import os
import json
import time
from kafka import KafkaConsumer

# Get the absolute path of the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Add the project root to Python's module path BEFORE importing db_utils
sys.path.append(project_root)

# Import SQLite functions
from utils.db_utils import create_table, insert_stream  


# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'spotify_streams',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)

# Ensure the database table exists
create_table()

# Function to compute sentiment score from valence_% (if available)
def compute_sentiment(valence):
    return round(valence / 100, 2)  # Normalize between 0 and 1

print("📥 Listening for new messages...")
for message in consumer:
    data = message.value
    
    track = data['track']
    artist = data['artist']
    streams = data['streams']
    genre = data['genre']
    timestamp = data['timestamp']
    
    # Compute sentiment score (placeholder for now)
    sentiment_score = compute_sentiment(50)  # Replace 50 with valence_% if available

    # Store in SQLite database
    insert_stream(track, artist, streams, genre, sentiment_score, timestamp)

    print(f"✅ Stored in DB: {track} | {artist} | Streams: {streams} | Genre: {genre}")

    time.sleep(0.5)  # Simulating processing time
