from pyspark.sql import SparkSession, DataFrame
from data_processor import DataProcessor
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
from pyspark.sql.functions import col, sum as spark_sum
from typing import Dict, Tuple
import traceback
import shutil
import glob


def create_spark_session(app_name: str = "LambdaArchitecture") -> SparkSession:
    """
    Create and configure Spark session with MySQL connectors
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.driver.memory", "12g")
        .getOrCreate()
    )


def main():
    """
    Loads data from csv to MySQL and then MySQL to Spark
    """
    load_dotenv()
    spark = create_spark_session()
    data_processor = DataProcessor(spark)
    data_processor.config = data_processor.setup_configuration()

    # Loading csv data into mysql
    data_processor.load_into_mysql(
        "data/cards.csv", data_processor.config["cards_table"]
    )
    data_processor.load_into_mysql(
        "data/customers.csv", data_processor.config["customers_table"]
    )
    # data_processor.load_into_mysql("data/credit_card_types.csv", data_processor.config["cc_types_table"])
    # data_processor.load_into_mysql("data/transactions.csv", data_processor.config["transactions_table"])

    data_processor.cards = data_processor.load_from_mysql(
        data_processor.config["cards_table"]
    )
    data_processor.customers = data_processor.load_from_mysql(
        data_processor.config["customers_table"]
    )
    data_processor.credit_card_types = data_processor.load_from_mysql(
        data_processor.config["cc_types_table"]
    )
    data_processor.transactions = data_processor.load_from_mysql(
        data_processor.config["transactions_table"]
    )

    print("Loaded data from MySQL to Spark")


if __name__ == "__main__":
    main()
