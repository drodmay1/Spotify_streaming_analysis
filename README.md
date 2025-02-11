# 🎵 Spotify Streaming Data Pipeline by David Rodriguez-Mayorquin
## https://www.linkedin.com/in/david-rodriguez-mayorquin-94808117/

## **Overview**
This project is a real-time streaming data pipeline built using Kafka, SQLite, and Streamlit to analyze Spotify music trends. 
It allows real-time tracking of streamed songs, sentiment analysis, and genre distribution, and displays interactive dashboards.

## **Features**
- **Kafka-based Streaming Pipeline**: Streams data from a Spotify dataset in real-time.  
- **SQLite Database Storage**: Processes and stores data dynamically.  
- **Sentiment Analysis**: Computes sentiment scores based on song characteristics.  
- **Interactive Dashboards**:  
   - `dashboard.py` **Top Songs, Genre Trends, Streaming Trends**   
   - `sentiment_dashboard.py` **Sentiment Analysis by Genre & Time** 
**Auto-Refreshing Dashboard**: Updates data every 5 seconds.  

## **Dependencies**
- **Python 3.11**
- **Kafka** (for real-time streaming)
- **SQLite** (for storing data)
- **Pandas** (for data processing)
- **Matplotlib** (for charts)
- **Streamlit** (for interactive dashboards)

## **Installation & Setup**
### **Clone the Repository**
```sh
git clone https://github.com/drodmay1/Spotify_streaming_analysis
```

### **Create and active a virtual environment**
```
python -m venv .venv
source .venv/bin/activate  # Mac/Linux
.venv\Scripts\activate     # Windows
```

### **Install Dependencies**
```
pip install -r requirements.txt
```

### **Start Kafka and Zookeeper**
```
bash zookeeper-server-start.sh ../config/zookeeper.properties
bash kafka-server-start.sh ../config/server.properties
```

### **R**un Kafka Producer**
```
python producers/spotify_producer.py
```

## **Run Kafka Consumer**
```
python consumers/spotify_consumer.py
```

## **Start the dashboards**
```
streamlit run dashboard.py
```

## **Run Sentiment Analysis Dashboard**
```
streamlit run sentiment_dashboard.py
```

## **Save and Push it to GitHub!
```
git add README.md
git commit -m "Added project README"
git push origin main
```





