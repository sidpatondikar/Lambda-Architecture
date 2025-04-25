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
from pyspark.sql.types import DoubleType, DecimalType

class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = None
        self.customers = None
        self.cards = None
        self.transactions = None
        self.credit_card_types = None
    
    def preprocess_transaction_row(self,header, row):
        new_row = []
        for h, value in zip(header, row):
            if h == "related_transaction_id" and value.strip() == "":
                new_row.append(None)
            else:
                new_row.append(value)
        return new_row

    
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
        print(f"🧹 Truncated table '{table_name}' before inserting new data.")

        with open(csv_file, 'r', newline='') as file:
            reader = csv.reader(file)
            header = next(reader)
            placeholders = ", ".join(["%s"] * len(header))
            insert_query = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES ({placeholders})"

            for line_num, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    print(f"⚠️ Skipping line {line_num}: column count mismatch ({len(row)} vs {len(header)})")
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
