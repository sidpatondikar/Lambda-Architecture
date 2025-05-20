import json
import os
from kafka import KafkaConsumer
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, when, coalesce
from dotenv import load_dotenv
from data_processor import DataProcessor
import helper
from pyspark.sql.functions import to_timestamp, date_format


def create_consumer(topic_name):
    """
    creates consumer in kafka
    """
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="user-data-consumer-group-jkl",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def create_spark_session(app_name: str = "LambdaArchitectureConsumer") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def build_lookup_dicts(cards_df, customers_df, transactions_df):
    """
    Builds Python dictionaries for fast lookups from cards, customers, and transactions DataFrames.
    """
    cards_dict = {
        str(row["card_id"]): {
            "credit_limit": row["credit_limit"],
            "current_balance": row["current_balance"],
            "customer_id": row["customer_id"],
        }
        for row in cards_df.toLocalIterator()
    }
    customers_dict = {
        str(row["customer_id"]): row["address"]
        for row in customers_df.toLocalIterator()
    }
    transactions_dict = {
        str(row["transaction_id"]): row["pending_balance"]
        for row in transactions_df.select(
            "transaction_id", "pending_balance"
        ).toLocalIterator()
    }
    return cards_dict, customers_dict, transactions_dict


def prepare_transaction_info(user_data):

    return {
        "txn_id": user_data.get("transaction_id"),
        "card_id": user_data.get("card_id"),
        "merchant_name": user_data.get("merchant_name"),
        "timestamp": user_data.get("timestamp"),
        "amount": user_data.get("amount"),
        "location": user_data.get("location"),
        "transaction_type": user_data.get("transaction_type"),
    }


def test_all_checks(
    transaction_info,
    cards_dict,
    customers_dict,
    transactions_dict,
    card_pending_balances,
    transaction_history,
):
    """
    Applies card validation checks (limit, balance, location) to determine if a transaction is valid.
    Returns (True, None) if valid, else (False, reason).
    """
    card_id = transaction_info["card_id"]
    txn_id = transaction_info["txn_id"]
    amount = float(transaction_info["amount"])
    location = transaction_info["location"]

    card = cards_dict.get(card_id)
    if not card:
        return False, "Card not found"
    if amount >= 0.5 * float(card["credit_limit"]):
        return False, "Transaction exceeds 50% of credit limit"

    credit_limit = float(card["credit_limit"])
    current_bal = float(card["current_balance"])
    previous_pending = 0.0
    if card_id in transaction_history:
        last_txns = transaction_history[card_id]
        for tx_id in reversed(last_txns):
            if int(tx_id.split(":")[-1]) < int(txn_id.split(":")[-1]):
                previous_pending = card_pending_balances.get(card_id, 0.0)
                break

    if amount + current_bal + previous_pending >= credit_limit:
        return False, "Total balance exceeds credit limit"

    address = customers_dict.get(str(card["customer_id"]))
    if not address:
        return False, "Customer address not found"
    try:
        if not helper.is_location_close_enough(
            address.strip()[-5:], location.strip()[-5:]
        ):
            return False, "Merchant location too far"
    except Exception as e:
        return False, f"Error extracting zip codes: {str(e)}"

    return True, None


def process_transaction(
    user_data,
    cards_dict,
    customers_dict,
    transactions_dict,
    card_pending_balances,
    transaction_history,
):
    """
    Processes a single transaction: validates, updates running pending balance, and returns a Spark Row for output.
    """

    txn_id = user_data.get("transaction_id")
    transaction_info = prepare_transaction_info(user_data)
    approved, reason = test_all_checks(
        transaction_info,
        cards_dict,
        customers_dict,
        transactions_dict,
        card_pending_balances,
        transaction_history,
    )
    amount = float(transaction_info["amount"])
    card_id = transaction_info["card_id"]
    card_pending_balances.setdefault(card_id, 0.0)

    if approved:
        card_pending_balances[card_id] += amount
        status = "pending"
        print(f"Processed transaction_id {txn_id} - PROCESSED")
    else:
        status = "declined"
        print(f"Processed transaction_id {txn_id} - DECLINED | Reason: {reason}")

    transaction_history.setdefault(card_id, []).append(txn_id)
    return Row(
        transaction_id=txn_id,
        status=status,
        pending_balance=card_pending_balances[card_id],
    )


def finalize_output(updates, stream_transactions, spark, processor):
    """
    Applies processed transaction updates to the stream_transactions DataFrame, reformats timestamps,
    and writes the result to CSV.
    """

    updates.sort(key=lambda row: int(row["transaction_id"].split(":")[-1]))
    updates_df = spark.createDataFrame(updates)
    updates_df = updates_df.withColumnRenamed("status", "new_status").withColumnRenamed(
        "pending_balance", "new_pending_balance"
    )

    final_df = (
        stream_transactions.join(
            updates_df.orderBy(col("transaction_id")), on="transaction_id", how="left"
        )
        .withColumn("status", coalesce(col("new_status"), col("status")))
        .withColumn(
            "pending_balance",
            coalesce(col("new_pending_balance"), col("pending_balance")),
        )
        .drop("new_status", "new_pending_balance")
    )

    final_df = final_df.withColumn(
        "timestamp",
        date_format(
            to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            "dd-MM-yyyy HH:mm:ss",
        ),
    )

    processor.save_to_csv(
        final_df.orderBy(col("transaction_id")),
        "data/results/",
        "stream_transactions.csv",
    )


def main():
    """
    Main execution logic: initializes Spark and Kafka, consumes transactions, processes them,
    and outputs results to CSV.
    """

    load_dotenv()
    topic_name = "transactions"
    consumer = create_consumer(topic_name)
    spark = create_spark_session()
    processor = DataProcessor(spark)
    processor.config = processor.setup_configuration()

    cards_df = processor.load_from_mysql(processor.config["cards_table"]).cache()
    customers_df = processor.load_from_mysql(
        processor.config["customers_table"]
    ).cache()
    stream_transactions = (
        processor.load_from_mysql(processor.config["transactions_table"])
        .withColumn("pending_balance", lit(0))
        .withColumn("status", lit(""))
        .cache()
    )

    cards_dict, customers_dict, transactions_dict = build_lookup_dicts(
        cards_df, customers_df, stream_transactions
    )

    seen_txn_ids = set()
    updates, card_pending_balances, transaction_history = [], {}, {}
    total_expected_txns = len(transactions_dict)

    try:
        for txn in consumer:
            user_data = txn.value
            txn_id = user_data.get("transaction_id")
            if txn_id in seen_txn_ids:
                continue
            updates.append(
                process_transaction(
                    user_data,
                    cards_dict,
                    customers_dict,
                    transactions_dict,
                    card_pending_balances,
                    transaction_history,
                )
            )
            seen_txn_ids.add(txn_id)
            if len(seen_txn_ids) >= total_expected_txns:
                print(f"✅ Processed all {total_expected_txns} transactions.")
                break
    except KeyboardInterrupt:
        print("Consumer manually stopped.")
    finally:
        if updates:
            finalize_output(updates, stream_transactions, spark, processor)
        consumer.close()
        print("✅ Consumer closed.")


if __name__ == "__main__":
    main()
