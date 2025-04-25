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

def import_files() -> Tuple[DataFrame, DataFrame, DataFrame]:
    cards_updated= spark.read.csv("data/cards.csv", header=True, inferSchema=True).fillna({"pending_balance": 0})
    customers= spark.read.csv("data/customers.csv", header=True, inferSchema=True)
    stream_transactions = spark.read.csv("data/transactions.csv", header=True, inferSchema=True, fillna({"status":"", "pending_balance":0}))
    return cards_updated, customers, stream_transactions


def card_limit_check(card_id, amount, cards_updated) --> bool:
    credit_limit = (
        cards_updated
        .filter(col("card_id") == card_id)
        .select(col("credit_limit"))
        .collect()
    )

    if amount >= 0.5*credit_limit:
        return False

    return True


def balance_check(txn_id, card_id, amount, cards_updated) --> bool:
    credit_limit = (
        cards_updated
        .filter(col("card_id") == card_id)
        .select("credit_limit")
        .collect()
    )

    pending_bal = (
        stream_transactions
        .filter(col("transaction_id") == txn_id)
        .select("pending_balance")
        .collect()
    )

    current_bal = (
        cards_updated
        .filter(col("card_id") == card_id)
        .select("current_balance")
        .collect()
    )

    total_bal = amount + pending_bal + current_bal

    if total_bal >= credit_limit:
        return False
    
    return True

def location_check(location,card_id, customers, cards_updated) --> bool:
    customer_id = (
        cards_udpated
        .filter(col("card_id") == card_id)
        .select("customer_id")
        .collect()
    )

    address = (
        customers
        .filter(col("customer_id") == customer_id)
        .select("address")
        .collect()
    )

    return helper.is_location_close_enough(location, address)



def test_all_checks(tx_id, card_id, amount, location, cards_udpated, customers):
    if all([
    card_limit_check(card_id, amount, cards_updated)
    and balance_check(txn_id, card_id, amount, cards_updated)
    and location_check(location, card_id, customers, cards_updated)
    ]):
        return True
    
    return False


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