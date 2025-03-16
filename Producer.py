from confluent_kafka import Producer
import json
import time
import requests
import os

# Alpha Vantage API Key (Set as environment variable)
API_KEY = "D2IQ7X0XWT7YE8RM"
STOCK_SYMBOL = "AAPL"
KAFKA_TOPIC = "stock_prices"
KAFKA_BROKER = "localhost:9092"

# Initialize Kafka Producer
producer = Producer({
    'bootstrap.servers': KAFKA_BROKER
})

def fetch_stock_data():
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={STOCK_SYMBOL}&interval=1min&apikey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None

    # Check for API rate limit error
    if "Note" in data and "Thank you for using Alpha Vantage" in data["Note"]:
        print("API rate limit exceeded. Please wait before making another request.")
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
    return None


stock_data = fetch_stock_data()
if stock_data:
    producer.produce(KAFKA_TOPIC, json.dumps(stock_data).encode("utf-8"))
    producer.flush()
    print(f"Sent data: {stock_data}")
else:
    print("No data fetched.")