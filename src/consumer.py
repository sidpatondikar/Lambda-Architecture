import json
from kafka import KafkaConsumer
from data_process import DataProcessor
import helper
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import math

spark = SparkSession.builder.appName("TransactionConsumer").getOrCreate()

def create_consumer(topic_name):
    """Create and return a Kafka consumer instance."""
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="user-data-consumer-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )



def process_transactions(tx):
    """Process received transactions."""
    # In a real application, this could include data transformation,
    # saving to database, triggering other events, etc.
    txn_id = tx.get("transaction_id")
    card_id = tx.get("card_id")
    merchant_name = tx.get("merchant_name")
    timestamp = tx.get("timestamp")
    amount = tx.get("amount")
    location = tx.get("location")
    transaction_type = tx.get("transaction_type")

    print(f"Processing transaction: (ID: {txn_id})")
    print(f" card_id:{card_id}, merchant name:{merchant_name}, timestamp:{timestamp}, amount:{amount}")
    print(f" type: {transaction_type}")

    print("status : ")
    print("")
    print("-"*50)


def main():
    """Main function to consume messages from Kafka topic."""
    topic_name = "transactions"
    consumer = create_consumer(topic_name)

    print(f"Starting to consume messages from topic '{topic_name}'...")
    print("Waiting for messages... (Press Ctrl+C to stop)")

    try:
        for txn in consumer:
            print(f"\nReceived transaction at offset {txn.offset}:")
            user_data = txn.value
            process_transactions(user_data)
    except KeyboardInterrupt:
        print("\nConsumer stopped by user")
    finally:
        consumer.close()
        print("Consumer closed")


if __name__ == "__main__":
    main()