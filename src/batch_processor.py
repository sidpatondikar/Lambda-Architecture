from pyspark.sql import SparkSession
from data_processor import DataProcessor
from dotenv import load_dotenv
import os


def create_spark_session(app_name: str = "BatchProcessor") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def main():
    # Load environment variables
    load_dotenv()

    spark = create_spark_session()
    data_processor = DataProcessor(spark)
    data_processor.config = data_processor.setup_configuration()

    # Load necessary data from MySQL
    cards_df = data_processor.load_from_mysql(data_processor.config["cards_table"])
    customers_df = data_processor.load_from_mysql(
        data_processor.config["customers_table"]
    )

    # Perform batch processing
    data_processor.batch_processing(cards_df, customers_df)
    print("✅ Batch processing complete.")

    data_processor.load_into_mysql(
        "data/results/cards_updated.csv", data_processor.config["cards_table"]
    )
    data_processor.load_into_mysql(
        "data/results/customers_updated.csv", data_processor.config["customers_table"]
    )


if __name__ == "__main__":
    main()
