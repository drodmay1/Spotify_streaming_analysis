import sys
import os
import json
import time
from kafka import KafkaConsumer
from utils.db_utils import create_table, insert_stream

# Get the absolute path of the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'spotify_streams',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)

# Ensure the database table exists
create_table()

# Function to compute sentiment score using valence (if available)
def compute_sentiment(valence, energy=0.5, danceability=0.5):
    if valence is not None:
        return round((valence * 0.5) + (energy * 0.3) + (danceability * 0.2), 2)
    return 0.5  # Default neutral sentiment if valence is missing

print("📥 Listening for new messages...")
for message in consumer:
    data = message.value
    
    track = data['track']
    artist = data['artist']
    streams = data['streams']
    genre = data['genre']
    timestamp = data['timestamp']
    
    # Check if valence is available in the dataset
    valence = data.get('valence', 0.5)  # Default to 0.5 if missing
    energy = data.get('energy', 0.5)  # Default to 0.5
    danceability = data.get('danceability', 0.5)  # Default to 0.5

    # Compute sentiment score
    sentiment_score = compute_sentiment(valence, energy, danceability)

    # Store in SQLite database
    insert_stream(track, artist, streams, genre, sentiment_score, timestamp)

    print(f"✅ Stored in DB: {track} | {artist} | Streams: {streams} | Sentiment: {sentiment_score}")

    time.sleep(0.5)  # Simulating processing time
