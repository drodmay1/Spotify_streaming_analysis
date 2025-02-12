import sys
import os
import json
import time
from datetime import datetime
from kafka import KafkaConsumer

# Ensure the script finds the project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../utils")))

from utils.db_utils import create_table, insert_stream

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'spotify_streams',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Ensure correct deserialization
    auto_offset_reset='earliest'
)

# Ensure the database table exists
create_table()

# Function to compute sentiment score
def compute_sentiment(valence, energy, danceability):
    return round((valence * 0.5) + (energy * 0.3) + (danceability * 0.2), 2)

print("📥 Listening for new messages...")
for message in consumer:
    # Read the message as a dictionary (already deserialized)
    data = message.value

    # DEBUG: Print the entire message
    print(f"DEBUG: Full received message: {data}")

    # Extract values
    track = data.get('track', 'Unknown')
    artist = data.get('artist', 'Unknown')
    streams = data.get('streams', 0)
    genre = data.get('genre', 'Unknown')

    # Extract and validate timestamp
    raw_timestamp = data.get('timestamp', 'Unknown')

    try:
        timestamp = datetime.fromisoformat(raw_timestamp).isoformat()  # Ensure correct format
    except ValueError:
        print(f"⚠️ WARNING: Invalid timestamp format received: {raw_timestamp}")
        timestamp = raw_timestamp  # Store the original timestamp for debugging

    print(f"DEBUG: Final timestamp stored: {timestamp}")  # Debugging

    # Extract valence, energy, and danceability properly
    valence = float(data.get("valence", 0.5))
    energy = float(data.get("energy", 0.5))
    danceability = float(data.get("danceability", 0.5))

    # Debugging: Print extracted sentiment values
    print(f"DEBUG: Raw values -> Valence: {valence}, Energy: {energy}, Danceability: {danceability}")

    # Compute sentiment score
    sentiment_score = compute_sentiment(valence, energy, danceability)

    # Debugging: Print computed sentiment score
    print(f"DEBUG: Computed Sentiment Score: {sentiment_score}")

    # Store in SQLite database (make sure the correct timestamp is inserted)
    insert_stream(track, artist, streams, genre, sentiment_score, timestamp)

    print(f"✅ Stored in DB: {track} | {artist} | Streams: {streams} | Sentiment: {sentiment_score} | Timestamp: {timestamp}")


    time.sleep(0.5)  # Simulating processing time
