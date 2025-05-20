import csv
import json
import time
from kafka import KafkaProducer
from data_processor import DataProcessor
from collections import defaultdict
from datetime import datetime
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os


def create_producer():
    """Create and return a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )


def create_spark_session(app_name: str = "LambdaArchitectureProdcer") -> SparkSession:
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
    """Send MySQL-loaded transactions to Kafka."""
    from collections import defaultdict
    from datetime import datetime

    load_dotenv()
    spark = create_spark_session()
    processor = DataProcessor(spark)
    processor.config = processor.setup_configuration()
    transactions_df = processor.load_from_mysql(processor.config["transactions_table"])
    producer = create_producer()

    # Collect and group records by date
    data = transactions_df.collect()
    grouped = defaultdict(list)
    for record in data:
        ts = record["timestamp"]
        dt = (
            ts.date()
            if isinstance(ts, datetime)
            else datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").date()
        )
        grouped[dt].append(record)

    for date in sorted(grouped.keys()):
        for record in grouped[date]:
            rec = record.asDict()
            rec = {
                k: (v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else v)
                for k, v in rec.items()
            }
            producer.send("transactions", value=rec)
            print(f"Send: {rec}")
            time.sleep(0.1)
        print(f"Finished sending transactions for {date}\n")
    producer.flush()
    print("All transactions sent successfully!")


if __name__ == "__main__":
    main()
