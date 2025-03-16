from confluent_kafka import Producer, Consumer, KafkaError
import json
import time
import requests
import snowflake.connector
import logging
import os
from datetime import datetime
import threading
import signal
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
API_KEY = "D2IQ7X0XWT7YE8RM"
STOCK_SYMBOLS = ["AAPL", "MSFT", "AMZN", "GOOGL", "META"]  # List of stock symbols to track
KAFKA_TOPIC = "stock_prices"
KAFKA_BROKER = "localhost:9092"

# Streaming configuration
FETCH_INTERVAL = 60  # Seconds between data fetches (be mindful of API rate limits)
CONSUMER_INTERVAL = 5  # Seconds between consumer poll operations

# Snowflake connection details
SNOWFLAKE_USER = "Rukudeva"
SNOWFLAKE_PASSWORD = "Harrypotter@12"
SNOWFLAKE_ACCOUNT = "UJJESBQ-UFB16712"
SNOWFLAKE_DATABASE = "STOCK_STREAM"
SNOWFLAKE_SCHEMA = "APPLE_TEST"
SNOWFLAKE_TABLE = "STOCK_PRICES"  # Changed to a more generic table name

# Flags for controlling the streaming processes
running = True

def fetch_stock_data(symbol):
    """Fetch stock data from Alpha Vantage API for a specific symbol"""
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Check for API rate limit error
        if "Note" in data and "Thank you for using Alpha Vantage" in data["Note"]:
            logger.warning(f"API rate limit exceeded for {symbol}. Please wait before making another request.")
            return None
            
        if "Time Series (1min)" in data:
            latest_time = list(data["Time Series (1min)"].keys())[0]
            stock_info = data["Time Series (1min)"][latest_time]
            stock_record = {
                "symbol": symbol,
                "timestamp": latest_time,
                "open": stock_info["1. open"],
                "high": stock_info["2. high"],
                "low": stock_info["3. low"],
                "close": stock_info["4. close"],
                "volume": stock_info["5. volume"]
            }
            return stock_record
        else:
            logger.error(f"Unexpected API response format for {symbol}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response for {symbol}: {e}")
        return None
    except (KeyError, IndexError) as e:
        logger.error(f"Error extracting data for {symbol}: {e}")
        return None

def produce_to_kafka(stock_data):
    """Send stock data to Kafka topic"""
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    try:
        producer.produce(KAFKA_TOPIC, json.dumps(stock_data).encode("utf-8"))
        producer.flush()
        logger.info(f"Sent data to Kafka: {stock_data['symbol']} at {stock_data['timestamp']}")
        return True
    except Exception as e:
        logger.error(f"Error sending data to Kafka: {e}")
        return False

def continuous_producer():
    """Continuously fetch stock data and send to Kafka"""
    global running
    symbol_index = 0
    
    while running:
        # Process one symbol at a time in a round-robin fashion
        symbol = STOCK_SYMBOLS[symbol_index]
        logger.info(f"Fetching data for {symbol}...")
        
        # Fetch and send data
        stock_data = fetch_stock_data(symbol)
        if stock_data:
            produce_to_kafka(stock_data)
        
        # Move to next symbol
        symbol_index = (symbol_index + 1) % len(STOCK_SYMBOLS)
        
        # If we've completed a full cycle, wait to avoid API rate limits
        if symbol_index == 0:
            logger.info(f"Completed full symbol cycle, waiting {FETCH_INTERVAL} seconds...")
            time.sleep(FETCH_INTERVAL)
        else:
            # Add small delay between symbols to avoid rapid API calls
            time.sleep(12)  # Alpha Vantage free tier limit (5 calls per minute)

def continuous_consumer():
    """Continuously consume from Kafka and load into Snowflake"""
    global running
    
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'stock-price-loader',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    
    consumer.subscribe([KAFKA_TOPIC])
    
    try:
        while running:
            # Poll for messages with a timeout
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # No message received, wait briefly then try again
                time.sleep(CONSUMER_INTERVAL)
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("Reached end of partition")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue
                
            # Process the message
            try:
                stock_data = json.loads(msg.value().decode("utf-8"))
                logger.info(f"Received data for {stock_data['symbol']}")
                
                # Insert into Snowflake
                if insert_into_snowflake(stock_data):
                    # Manually commit the offset after successful processing
                    consumer.commit(msg)
                    logger.info(f"Successfully processed {stock_data['symbol']} and committed offset")
                else:
                    logger.error(f"Failed to insert {stock_data['symbol']} into Snowflake, not committing offset")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        consumer.close()
        logger.info("Consumer closed")

def insert_into_snowflake(stock_data):
    """Insert stock data into Snowflake with a unique processing timestamp"""
    try:
        # Add current processing timestamp to make each record unique
        processing_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cursor = conn.cursor()
        
        # Insert data with processing timestamp
        insert_query = f"""
        INSERT INTO {SNOWFLAKE_TABLE} (symbol, timestamp, open, high, low, close, volume, processing_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            stock_data["symbol"],
            stock_data["timestamp"],
            stock_data["open"],
            stock_data["high"],
            stock_data["low"],
            stock_data["close"],
            stock_data["volume"],
            processing_time
        ))
        conn.commit()
        logger.info(f"Data for {stock_data['symbol']} inserted into Snowflake")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error inserting {stock_data['symbol']} into Snowflake: {e}")
        return False

def create_snowflake_table_if_not_exists():
    """Create the Snowflake table if it doesn't exist"""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
            id INT AUTOINCREMENT PRIMARY KEY,
            symbol VARCHAR(10),
            timestamp TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INT,
            processing_time TIMESTAMP,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info(f"Ensured table {SNOWFLAKE_TABLE} exists")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error creating Snowflake table: {e}")
        return False

def handle_shutdown(signum, frame):
    """Handle shutdown signals gracefully"""
    global running
    logger.info("Shutdown signal received, stopping threads...")
    running = False

if __name__ == "__main__":
    logger.info("Starting streaming stock price pipeline")
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    # Ensure the Snowflake table exists
    if not create_snowflake_table_if_not_exists():
        logger.error("Failed to ensure Snowflake table exists. Exiting.")
        sys.exit(1)
    
    # Start producer and consumer threads
    producer_thread = threading.Thread(target=continuous_producer)
    consumer_thread = threading.Thread(target=continuous_consumer)
    
    producer_thread.start()
    consumer_thread.start()
    
    logger.info("Streaming pipeline started. Press Ctrl+C to stop.")
    
    # Wait for threads to complete
    try:
        producer_thread.join()
        consumer_thread.join()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        running = False
        
    logger.info("Pipeline shutdown complete")