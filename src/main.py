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

def create_spark_session(app_name: str = "LambdaArchitecture") -> SparkSession:
    """
    Create and configure Spark session with MySQL connectors
    """
    return(
        SparkSession.builder.appName(app_name)
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.driver.memory","12g")
        .getOrCreate()
)

def main():
    config = setup_configuration()

    spark = create_spark_session()
    data_processor = DataProcessor(spark)
    data_processor.config = config

    #Loading csv data into mysql
    # data_processor.load_into_mysql("data/cards.csv", config["cards_table"])
    # data_processor.load_into_mysql("data/customers.csv", config["customers_table"])
    # data_processor.load_into_mysql("data/credit_card_types.csv", config["cc_types_table"])
    # data_processor.load_into_mysql("data/transactions.csv", config["transactions_table"])

    data_processor.cards = data_processor.load_from_mysql(data_processor.config["cards_table"])
    data_processor.customers = data_processor.load_from_mysql(data_processor.config["customers_table"])
    data_processor.credit_card_types = data_processor.load_from_mysql(data_processor.config["cc_types_table"])
    data_processor.transactions = data_processor.load_from_mysql(data_processor.config["transactions_table"])

    



if __name__ == "__main__":
    main()

