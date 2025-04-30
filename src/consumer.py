import json
from kafka import KafkaConsumer
import helper
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.functions import sum as spark_sum
import math
from data_processor import DataProcessor

spark = SparkSession.builder.appName("TransactionConsumer").getOrCreate()


def create_consumer(topic_name):
    """Create and return a Kafka consumer instance."""
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="user-data-consumer-group-test6",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def import_files():
    cards_updated = (
        spark.read.csv("data/cards.csv", header=True, inferSchema=True)
        .withColumn("pending_balance", lit(0))
    )
    customers = spark.read.csv("data/customers.csv", header=True, inferSchema=True)
    stream_transactions = (
        spark.read.csv("data/transactions.csv", header=True, inferSchema=True)
        .withColumn("status", lit(""))
        .withColumn("pending_balance", lit(0))
    )
    return cards_updated, customers, stream_transactions

def build_lookup_dicts(cards_df, customers_df, transactions_df):
    cards_dict = {
        str(row["card_id"]): {
            "credit_limit": row["credit_limit"],
            "current_balance": row["current_balance"],
            "customer_id": row["customer_id"]
        }
        for row in cards_df.collect()
    }

    customers_dict = {
        str(row["customer_id"]): row["address"]
        for row in customers_df.collect()
    }

    transactions_dict = {
        str(row["transaction_id"]): row["pending_balance"]
        for row in transactions_df.collect()
    }

    return cards_dict, customers_dict, transactions_dict

def card_limit_check(transaction_info, cards_dict):
    card_id = transaction_info["card_id"]
    amount = float(transaction_info["amount"])
    card = cards_dict.get(card_id)
    if not card:
        return False, "Card not found"
    if amount >= 0.5 * float(card["credit_limit"]):
        return False, "Transaction exceeds 50% of credit limit"
    
    return True, None

def balance_check(transaction_info, cards_dict, transactions_dict):
    txn_id = transaction_info["txn_id"]
    card_id = transaction_info["card_id"]
    amount = float(transaction_info["amount"])
    card = cards_dict.get(card_id)

    if not card:
        return False, "Card not found"
    
    credit_limit = float(card["credit_limit"])
    current_bal = float(card["current_balance"])
    pending_bal = float(transactions_dict.get(txn_id, 0))

    if amount + current_bal + pending_bal >= credit_limit:
        return False, "Total balance exceeds credit limit"
    
    return True, None

def location_check(transaction_info, cards_dict, customers_dict):
    card_id = transaction_info["card_id"]
    location = transaction_info["location"]
    card = cards_dict.get(card_id)

    if not card:
        return False, "Card not found"
    
    customer_id = str(card["customer_id"])
    address = customers_dict.get(customer_id)
    
    if not address:
        return False, "Customer address not found"
    
    try:
        customer_zip = address.strip()[-5:]
        transaction_zip = location.strip()[-5:]
    except Exception as e:
        return False, f"Error extracting zip codes: {str(e)}"
    if not helper.is_location_close_enough(customer_zip, transaction_zip):
        return False, "Merchant location too far"
    
    return True, None

def test_all_checks(transaction_info, cards_dict, customers_dict, transactions_dict):
    checks = [
        card_limit_check(transaction_info, cards_dict),
        balance_check(transaction_info, cards_dict, transactions_dict),
        location_check(transaction_info, cards_dict, customers_dict)
    ]
    for passed, reason in checks:
        if not passed:
            return False, reason
    return True, None

def update_txn(transaction_info, stream_transactions, cards_dict, customers_dict, transactions_dict):
    txn_id = transaction_info["txn_id"]
    card_id = transaction_info["card_id"]
    amount = float(transaction_info["amount"])

    prev_pending_balance = float(transactions_dict.get(txn_id, 0))
    approved, reason = test_all_checks(transaction_info, cards_dict, customers_dict, transactions_dict)
    status = True
    if approved:
        new_pending_balance = prev_pending_balance + amount
        stream_transactions = stream_transactions.withColumn(
            "pending_balance",
            when(col("transaction_id") == txn_id, lit(new_pending_balance)).otherwise(col("pending_balance"))
        ).withColumn(
            "status",
            when(col("transaction_id") == txn_id, lit("pending")).otherwise(col("status"))
        )
    else:
        stream_transactions = stream_transactions.withColumn(
            "status",
            when(col("transaction_id") == txn_id, lit("declined")).otherwise(col("status"))
        )
        status = False

    return stream_transactions, status, reason

def generate_cards_updated(cards_df, stream_transactions):
    # 1. Aggregate pending balances per card from transactions
    pending_df = (
        stream_transactions
        .groupBy("card_id")
        .agg(spark_sum("pending_balance").alias("total_pending"))
    )

    # 2. Join original cards_df with aggregated pending balances
    updated_cards_df = (
        cards_df
        .join(pending_df, on="card_id", how="left")
        .withColumn("pending_balance", col("current_balance") + col("total_pending"))
        .drop("total_pending")  # optional
    )

    return updated_cards_df

def process_transactions(tx, stream_transactions, cards_dict, customers_dict, transactions_dict):
    txn_id = tx.get("transaction_id")
    transaction_info = {
        "txn_id": txn_id,
        "card_id": tx.get("card_id"),
        "merchant_name": tx.get("merchant_name"),
        "timestamp": tx.get("timestamp"),
        "amount": tx.get("amount"),
        "location": tx.get("location"),
        "transaction_type": tx.get("transaction_type"),
    }

    print(f"Processing transaction: (ID: {txn_id})")

    stream_transactions, status, reason = update_txn(
        transaction_info, stream_transactions, cards_dict, customers_dict, transactions_dict
    )

    if not status:
        print(f"Transaction Declined. Reason: {reason}")

    return stream_transactions



def main():
    """Main function to consume messages from Kafka topic."""
    topic_name = "transactions"
    consumer = create_consumer(topic_name)

    cards_df, customers_df, stream_transactions = import_files()
    cards_dict, customers_dict, transactions_dict = build_lookup_dicts(cards_df, customers_df, stream_transactions)
    processor = DataProcessor(spark)

    print(f"Starting to consume messages from topic '{topic_name}'...")
    print("Waiting for messages... (Press Ctrl+C to stop)")

    MAX_TXNS = 401
    count = 0

    try:
        for txn in consumer:
            user_data = txn.value
            stream_transactions = process_transactions(
                user_data, stream_transactions, cards_dict, customers_dict, transactions_dict
            )
            count += 1
            if count >= MAX_TXNS:
                print(f"✅ Processed {MAX_TXNS} transactions. Stopping consumer.")
                break
    except KeyboardInterrupt:
        print("\nConsumer stopped by user")
    finally:
        cards_updated_final = generate_cards_updated(cards_df, stream_transactions)
        processor.save_to_csv(stream_transactions, "data/results/", "stream_transactions.csv")
        processor.save_to_csv(cards_updated_final, "data/results/","cards_updated.csv")
        consumer.close()
        print("Consumer closed")
    


if __name__ == "__main__":
    main()