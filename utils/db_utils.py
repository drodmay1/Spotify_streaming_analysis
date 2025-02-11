import sqlite3
import os

# Database file path (creates `spotify.db` in the data folder)
DB_PATH = os.path.join(os.path.dirname(__file__), "../data/spotify.db")

def connect_db():
    """Connect to SQLite database."""
    return sqlite3.connect(DB_PATH)

def create_table():
    """Create the database table if it doesn't exist."""
    query = """
    CREATE TABLE IF NOT EXISTS spotify_streams (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        track TEXT,
        artist TEXT,
        streams INTEGER,
        genre TEXT,
        sentiment_score FLOAT,
        timestamp TEXT
    );
    """
    with connect_db() as conn:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()

def insert_stream(track, artist, streams, genre, sentiment_score, timestamp):
    """Insert streaming data into SQLite database."""
    query = """
    INSERT INTO spotify_streams (track, artist, streams, genre, sentiment_score, timestamp)
    VALUES (?, ?, ?, ?, ?, ?);
    """
    with connect_db() as conn:
        cur = conn.cursor()
        cur.execute(query, (track, artist, streams, genre, sentiment_score, timestamp))
        conn.commit()

# Ensure the database table exists when the module is imported
create_table()
