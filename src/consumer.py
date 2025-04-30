import json
from kafka import KafkaConsumer
import helper
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
import math
from data_processor import DataProcessor

spark = SparkSession.builder.appName("TransactionConsumer").getOrCreate()

load_dotenv()

def load_config() -> Dict:
    """Load configuration from environment variables"""
    return{
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "mysql_db": os.getenv("MYSQL_DB"),
        "cards_table": os.getenv("CARDS_TABLE"),
        "customers_table": os.getenv("CUSTOMERS_TABLE"),
        "transactions_table": os.getenv("TRANSACTIONS_TABLE"),
        "cc_types_table": os.getenv("CC_TYPES_TABLE"),
        "output_path": os.getenv("OUTPUT_PATH")
    }

def setup_configuration() -> Dict:
    load_dotenv()
    config = load_config()
    return config

def create_consumer(topic_name):
    """Create and return a Kafka consumer instance."""
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="user-data-consumer-group-test2",
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

def card_limit_check(transaction_info, cards_updated):
    card_id = transaction_info["card_id"]
    amount = float(transaction_info["amount"])
    
    credit_limit_row = (
        cards_updated
        .filter(col("card_id") == card_id)
        .select("credit_limit")
        .collect()
    )

    if not credit_limit_row:
        return False, "Credit limit info missing"

    credit_limit = float(credit_limit_row[0]["credit_limit"])

    if amount >= 0.5 * credit_limit:
        return False, "Transaction exceeds 50% of credit limit"

    return True, None


def balance_check(transaction_info, cards_updated, stream_transactions):
    txn_id = transaction_info["txn_id"]
    card_id = transaction_info["card_id"]
    amount = float(transaction_info["amount"])
    
    credit_limit_row = (
        cards_updated
        .filter(col("card_id") == card_id)
        .select("credit_limit")
        .collect()
    )

    pending_bal_row = (
        stream_transactions
        .filter(col("transaction_id") == txn_id)
        .select("pending_balance")
        .collect()
    )

    current_bal_row = (
        cards_updated
        .filter(col("card_id") == card_id)
        .select("current_balance")
        .collect()
    )

    if not credit_limit_row or not pending_bal_row or not current_bal_row:
        return False, "Missing balance info"

    credit_limit = float(credit_limit_row[0]["credit_limit"])
    pending_bal = float(pending_bal_row[0]["pending_balance"])
    current_bal = float(current_bal_row[0]["current_balance"])

    total_bal = amount + pending_bal + current_bal

    if total_bal >= credit_limit:
        return False, "Total balance exceeds credit limit"

    return True, None


def location_check(transaction_info, customers, cards_updated):
    card_id = transaction_info["card_id"]
    location = transaction_info["location"]

    customer_id_row = (
        cards_updated
        .filter(col("card_id") == card_id)
        .select("customer_id")
        .collect()
    )

    if not customer_id_row:
        return False, "Customer ID not found for card"

    customer_id = customer_id_row[0]["customer_id"]

    address_row = (
        customers
        .filter(col("customer_id") == customer_id)
        .select("address")
        .collect()
    )

    if not address_row:
        return False, "Customer address not found"

    customer_address = address_row[0]["address"]
    transaction_location = location

    # ✅ Extract ZIPCODE
    try:
        customer_zip = customer_address.strip()[-5:]
        transaction_zip = transaction_location.strip()[-5:]
    except Exception as e:
        return False, f"Error extracting zip codes: {str(e)}"

    if not helper.is_location_close_enough(customer_zip, transaction_zip):
        return False, "Merchant location too far"

    return True, None


def test_all_checks(transaction_info, cards_updated, customers, stream_transactions):
    checks = [
        card_limit_check(transaction_info, cards_updated),
        balance_check(transaction_info, cards_updated, stream_transactions),
        location_check(transaction_info, customers, cards_updated)
    ]

    for passed, reason in checks:
        if not passed:
            return False, reason

    return True, None



def update_txn(transaction_info, cards_updated, customers, stream_transactions):
    txn_id = transaction_info["txn_id"]
    card_id = transaction_info["card_id"]
    amount = float(transaction_info["amount"])

    previous_txns = (
        stream_transactions
        .filter(col("card_id") == card_id)
        .filter(col("transaction_id") < txn_id)
        .orderBy(col("transaction_id").desc())
        .limit(1)
        .collect()
    )

    if previous_txns:
        prev_pending_balance = float(previous_txns[0]["pending_balance"])
    else:
        prev_pending_balance = 0.0

    approved, reason = test_all_checks(transaction_info, cards_updated, customers, stream_transactions)
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



def process_transactions(tx, stream_transactions, cards_updated, customers):
    txn_id = tx.get("transaction_id")
    card_id = tx.get("card_id")
    merchant_name = tx.get("merchant_name")
    timestamp = tx.get("timestamp")
    amount = tx.get("amount")
    location = tx.get("location")
    transaction_type = tx.get("transaction_type")

    transaction_info = {
        "txn_id": txn_id,
        "card_id": card_id,
        "merchant_name": merchant_name,
        "timestamp": timestamp,
        "amount": amount,
        "location": location,
        "transaction_type": transaction_type,
    }

    print(f"Processing transaction: (ID: {txn_id})")
    # print(f" card_id:{card_id}, merchant name:{merchant_name}, timestamp:{timestamp}, amount:{amount}")
    # print(f" type: {transaction_type}")

    stream_transactions, status, reason = update_txn(transaction_info, cards_updated, customers, stream_transactions)

    if not status:
        print(f"Transaction Declined. Reason: {reason}")

    # print("")
    # print("-" * 50)

    return stream_transactions



def main():
    """Main function to consume messages from Kafka topic."""
    config = setup_configuration()
    topic_name = "transactions"
    consumer = create_consumer(topic_name)

    cards_updated, customers, stream_transactions = import_files()

    print(f"Starting to consume messages from topic '{topic_name}'...")
    print("Waiting for messages... (Press Ctrl+C to stop)")

    try:
        for txn in consumer:
            #print(f"\nReceived transaction at offset {txn.offset}:")
            user_data = txn.value
            stream_transactions = process_transactions(user_data, stream_transactions, cards_updated, customers)
    except KeyboardInterrupt:
        print("\nConsumer stopped by user")
    finally:
        DataProcessor.save_to_csv(stream_transactions, config['output_path'])
        consumer.close()
        print("Consumer closed")
    


if __name__ == "__main__":
    main()