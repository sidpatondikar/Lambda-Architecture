from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode,
    col,
    round as spark_round,
    sum as spark_sum,
    max,
    count,
    abs as spark_abs,
    countDistinct,
    when,
    broadcast,
    lit,
    to_date,
)
from typing import Dict, Tuple
from collections import defaultdict
import mysql.connector

# from pymongo import MongoClient
import os
import glob
import shutil
import decimal
import numpy as np
import csv
import json
from datetime import datetime, timedelta
from pyspark.sql.types import DoubleType, DecimalType, FloatType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql import Row
from dotenv import load_dotenv
from helper import calculate_credit_score_adjustment, calculate_new_credit_limit
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType
from pyspark.sql.functions import coalesce, lit


class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = None
        self.customers = None
        self.cards = None
        self.transactions = None
        self.credit_card_types = None

    def preprocess_transaction_row(self, header, row):
        new_row = []
        for h, value in zip(header, row):
            if h == "related_transaction_id" and value.strip() == "":
                new_row.append(None)
            else:
                new_row.append(value)
        return new_row

    def load_config(self) -> Dict:
        """Load configuration from environment variables"""
        return {
            "mysql_url": os.getenv("MYSQL_URL"),
            "mysql_user": os.getenv("MYSQL_USER"),
            "mysql_password": os.getenv("MYSQL_PASSWORD"),
            "mysql_db": os.getenv("MYSQL_DB"),
            "cards_table": os.getenv("CARDS_TABLE"),
            "customers_table": os.getenv("CUSTOMERS_TABLE"),
            "transactions_table": os.getenv("TRANSACTIONS_TABLE"),
            "cc_types_table": os.getenv("CC_TYPES_TABLE"),
            "output_path": os.getenv("OUTPUT_PATH"),
        }

    def setup_configuration(self) -> Dict:
        load_dotenv()
        config = self.load_config()
        return config

    def load_into_mysql(self, csv_file: str, table_name: str):
        """
        Load a CSV file into a MySQL table, truncating the table first to avoid duplicates.
        """
        connection = mysql.connector.connect(
            host="localhost",
            user=self.config["mysql_user"],
            password=self.config["mysql_password"],
            database=self.config["mysql_db"],
        )
        cursor = connection.cursor()

        # Truncate the table before inserting new data
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        print(f"Truncated table '{table_name}' before inserting new data.")

        with open(csv_file, "r", newline="") as file:
            reader = csv.reader(file)
            header = next(reader)
            placeholders = ", ".join(["%s"] * len(header))
            insert_query = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES ({placeholders})"

            for line_num, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    print(
                        f"Skipping line {line_num}: column count mismatch ({len(row)} vs {len(header)})"
                    )
                    continue

                if table_name == "transactions":  # apply only for that table
                    row = self.preprocess_transaction_row(header, row)

                cursor.execute(insert_query, row)

        connection.commit()
        cursor.close()
        connection.close()
        print(f"✅ Inserted data from '{csv_file}' into MySQL table '{table_name}'.")

    def load_from_mysql(self, table_name: str) -> DataFrame:
        """
        Load MySQL table into a Spark DataFrame.
        """
        df = (
            self.spark.read.format("jdbc")
            .option("url", self.config["mysql_url"])
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", table_name)
            .option("user", self.config["mysql_user"])
            .option("password", self.config["mysql_password"])
            .load()
        )
        print(f"Loaded MySQL table {table_name} into Spark DataFrame.")
        return df

    def save_to_csv(self, df: DataFrame, output_path: str, filename: str) -> None:
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")
        print(f"Temporary directory: {temp_dir}")  # Debugging output

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)

    def generate_cards_updated(self, cards_df, stream_transactions):
        """
        Updates cards balance with card_balance + pending balance
        for each card_id
        """
        window_spec = Window.partitionBy("card_id").orderBy(
            col("transaction_id").cast("int").desc()
        )

        # Selecting the most recent transaction per card_id with its pending_balance
        latest_pending = (
            stream_transactions.withColumn("txn_rank", row_number().over(window_spec))
            .filter(col("txn_rank") == 1)
            .select("card_id", "pending_balance")
        )

        updated_cards_df = (
            cards_df.withColumn(
                "current_balance", col("current_balance").cast(FloatType())
            )
            .join(latest_pending, on="card_id", how="left")
            .withColumn(
                "pending_balance",
                when(col("pending_balance").isNull(), 0).otherwise(
                    col("pending_balance")
                ),
            )
            .withColumn(
                "current_balance", col("current_balance") + col("pending_balance")
            )
            .drop("pending_balance")
        )

        return updated_cards_df

    def update_credit_score(self, cards_df, customers_df):
        """
        Updates credit score for each customer using helper function calculate_credit_score_adjustment
        """

        usage_df = cards_df.groupBy("customer_id").agg(
            (spark_sum("current_balance") / spark_sum("credit_limit") * 100).alias(
                "usage_percent"
            )
        )

        customer_usage_df = usage_df.join(
            customers_df.select("customer_id", "credit_score"),
            on="customer_id",
            how="inner",
        )

        usage_data = customer_usage_df.select(
            "customer_id", "credit_score", "usage_percent"
        ).collect()
        updated_rows = []

        for row in usage_data:
            cid = int(row["customer_id"])
            old_score = int(row["credit_score"])
            usage = float(row["usage_percent"])

            score_change = calculate_credit_score_adjustment(usage)
            new_score = old_score + score_change

            updated_rows.append(
                Row(
                    customer_id=cid,
                    new_credit_score=new_score,
                    credit_score_change=score_change,
                )
            )

        spark = customer_usage_df.sparkSession
        updated_credit_df = spark.createDataFrame(updated_rows)

        customers_df = (
            customers_df.join(updated_credit_df, on="customer_id", how="left")
            .withColumn(
                "credit_score", coalesce(col("new_credit_score"), col("credit_score"))
            )
            .withColumn(
                "credit_score_change", coalesce(col("credit_score_change"), lit(0))
            )
            .drop("new_credit_score")
        )

        return customers_df.orderBy("customer_id")

    def update_credit_limit(self, cards_df, customers_df):
        """
        Updates credit limit for cards using helper function calculate_new_credit_limit
        """
        joined_df = cards_df.join(
            customers_df.select("customer_id", "credit_score_change"),
            on="customer_id",
            how="left",
        )

        updated_rows = []
        for row in joined_df.select(
            "card_id", "customer_id", "credit_limit", "credit_score_change"
        ).collect():
            card_id = int(row["card_id"])
            customer_id = int(row["customer_id"])
            old_limit = float(row["credit_limit"])
            score_change = (
                float(row["credit_score_change"])
                if row["credit_score_change"] is not None
                else 0.0
            )

            new_limit = calculate_new_credit_limit(old_limit, score_change)
            updated_rows.append(
                Row(card_id=card_id, customer_id=customer_id, credit_limit=new_limit)
            )

        spark = cards_df.sparkSession
        updated_cards_df = spark.createDataFrame(updated_rows)

        cards_df = (
            cards_df.join(
                updated_cards_df.select("card_id", "credit_limit").withColumnRenamed(
                    "credit_limit", "new_credit_limit"
                ),
                on="card_id",
                how="left",
            )
            .withColumn(
                "credit_limit",
                when(
                    col("new_credit_limit").isNotNull(), col("new_credit_limit")
                ).otherwise(col("credit_limit")),
            )
            .drop("new_credit_limit")
        )

        updated_customers_df = customers_df.drop("credit_score_change")

        return cards_df.orderBy("card_id"), updated_customers_df.orderBy("customer_id")

    def batch_processing(self, cards_df, customers_df):
        """
        reads stream_transactions and create batch_transactions
        also creates updated cards and customers df
        as well as update them in MySQL
        """
        stream_df = self.spark.read.option("header", True).csv(
            "data/results/stream_transactions.csv"
        )

        batch_transactions = stream_df.withColumn(
            "status",
            when(col("status") == "pending", "approved").otherwise(col("status")),
        )

        self.cards = self.generate_cards_updated(cards_df, batch_transactions)
        self.save_to_csv(
            batch_transactions, self.config["output_path"], "batch_transactions.csv"
        )

        self.customers = self.update_credit_score(self.cards, customers_df)
        self.cards, self.customers = self.update_credit_limit(
            self.cards, self.customers
        )

        self.save_to_csv(
            self.customers, self.config["output_path"], "customers_updated.csv"
        )
        self.save_to_csv(self.cards, self.config["output_path"], "cards_updated.csv")
