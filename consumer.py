from confluent_kafka import Consumer, KafkaError
import json

KAFKA_TOPIC = "stock_prices"
KAFKA_BROKER = "localhost:9092"

# Initialize Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'stock-price-consumer',
    'auto.offset.reset': 'earliest'  # Start from the beginning
})

consumer.subscribe([KAFKA_TOPIC])

# Poll once for a new message
msg = consumer.poll(timeout=5.0)  # Wait for 5 seconds to fetch a message

if msg is None:
    print("No new messages in topic.")
elif msg.error():
    if msg.error().code() == KafkaError._PARTITION_EOF:
        print("Reached end of partition.")
    else:
        print(f"Kafka error: {msg.error()}")
else:
    # Decode and print the message
    stock_data = json.loads(msg.value().decode("utf-8"))
    print(f"Received data: {stock_data}")

# Close the consumer after pulling one message
#consumer.close()
