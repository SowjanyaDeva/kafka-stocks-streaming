from confluent_kafka import Producer, Consumer, KafkaError
import json
import time
import requests
import snowflake.connector
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
API_KEY = "D2IQ7X0XWT7YE8RM"
STOCK_SYMBOL = "AAPL"
KAFKA_TOPIC = "stock_prices"
KAFKA_BROKER = "localhost:9092"

# Snowflake connection details
SNOWFLAKE_USER = "********"
SNOWFLAKE_PASSWORD = "*********"
SNOWFLAKE_ACCOUNT = "**************"
SNOWFLAKE_DATABASE = "STOCK_STREAM"
SNOWFLAKE_SCHEMA = "APPLE_TEST"
SNOWFLAKE_TABLE = "STOCK_PRICES_APPLE"

def fetch_stock_data():
    """Fetch a single stock data record from Alpha Vantage API"""
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={STOCK_SYMBOL}&interval=1min&apikey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Check for API rate limit error
        if "Note" in data and "Thank you for using Alpha Vantage" in data["Note"]:
            logger.warning("API rate limit exceeded. Please wait before making another request.")
            return None
            
        if "Time Series (1min)" in data:
            latest_time = list(data["Time Series (1min)"].keys())[0]
            stock_info = data["Time Series (1min)"][latest_time]
            stock_record = {
                "symbol": STOCK_SYMBOL,
                "timestamp": latest_time,
                "open": stock_info["1. open"],
                "high": stock_info["2. high"],
                "low": stock_info["3. low"],
                "close": stock_info["4. close"],
                "volume": stock_info["5. volume"]
            }
            return stock_record
        else:
            logger.error("Unexpected API response format")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response: {e}")
        return None

def produce_to_kafka(stock_data):
    """Send a single stock data record to Kafka topic"""
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    try:
        producer.produce(KAFKA_TOPIC, json.dumps(stock_data).encode("utf-8"))
        producer.flush()
        logger.info(f"Sent data to Kafka: {stock_data}")
        return True
    except Exception as e:
        logger.error(f"Error sending data to Kafka: {e}")
        return False

def consume_single_message():
    """Consume a single message from Kafka"""
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'stock-price-loader-test',  # Using a unique group ID for testing
        'auto.offset.reset': 'earliest',
    })
    
    consumer.subscribe([KAFKA_TOPIC])
    
    try:
        # Poll for a single message with a 10-second timeout
        msg = consumer.poll(timeout=10.0)
        
        if msg is None:
            logger.warning("No messages received within timeout period")
            return None
            
        if msg.error():
            logger.error(f"Kafka error: {msg.error()}")
            return None
            
        # Process the message
        stock_data = json.loads(msg.value().decode("utf-8"))
        logger.info(f"Received data: {stock_data}")
        return stock_data
                
    except Exception as e:
        logger.error(f"Error consuming message: {e}")
        return None
    finally:
        consumer.close()
        logger.info("Consumer closed")

def insert_into_snowflake(stock_data):
    """Insert a single stock data record into Snowflake"""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cursor = conn.cursor()
        
        query = f"""
        INSERT INTO {SNOWFLAKE_TABLE} (symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            stock_data["symbol"],
            stock_data["timestamp"],
            stock_data["open"],
            stock_data["high"],
            stock_data["low"],
            stock_data["close"],
            stock_data["volume"]
        ))
        conn.commit()
        logger.info(f"Data inserted into Snowflake: {stock_data}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error inserting into Snowflake: {e}")
        return False

def test_single_record():
    """Test the pipeline with a single record"""
    logger.info("=== STAGE 1: FETCHING DATA ===")
    stock_data = fetch_stock_data()
    if not stock_data:
        logger.error("Failed to fetch stock data")
        return False
    
    logger.info("=== STAGE 2: PRODUCING TO KAFKA ===")
    if not produce_to_kafka(stock_data):
        logger.error("Failed to produce to Kafka")
        return False
    
    logger.info("=== STAGE 3: CONSUMING FROM KAFKA ===")
    # Wait a moment to ensure data is available in Kafka
    time.sleep(2)
    consumed_data = consume_single_message()
    if not consumed_data:
        logger.error("Failed to consume data from Kafka")
        return False
    
    logger.info("=== STAGE 4: INSERTING INTO SNOWFLAKE ===")
    if not insert_into_snowflake(consumed_data):
        logger.error("Failed to insert data into Snowflake")
        return False
    
    logger.info("=== TEST COMPLETED SUCCESSFULLY ===")
    return True

if __name__ == "__main__":
    logger.info("Starting single record test for Kafka-Snowflake pipeline")
    if test_single_record():
        logger.info("Test completed successfully!")
    else:
        logger.error("Test failed")
