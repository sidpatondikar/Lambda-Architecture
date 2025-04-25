import csv
import json
import time
from kafka import KafkaProducer
from collections import defaultdict
from datetime import datetime

def create_producer():
    """Create and return a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

def read_csv(filepath):
    """Read data from a CSV file and return a list of dictionaries."""
    data = []
    with open(filepath, "r") as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            data.append(row)
    return data


def main():
    """Main function to send data to Kafka topic."""
    producer = create_producer()
    data = read_csv('data/transactions.csv')

    grouped_date = defaultdict(list)

    for record in data:
        try:
            parsed_date = datetime.strptime(record['timestamp'], '%Y-%m-%d %H:%M:%S').date()
        except ValueError:
            parsed_date = datetime.strptime(record['timestamp'], '%Y-%m-%d %H:%M:%S').date()
        grouped_date[parsed_date].append(record)


    print("Starting to send transactions to Kafka topic 'transactions' ...\n")


    sorted_dates = sorted(grouped_date.keys())

    for date in sorted_dates:
        print(f"Sending transactions for date: {date} \n")
        for record in grouped_date[date]:
            producer.send("transactions", value=record)
            print(f"Sent: {record}")
            time.sleep(0.2)  
        time.sleep(2) 
        print(f"Finished sending transactions for {date}\n")

    producer.flush()
    print("All transactions sent successfully!")


if __name__ == "__main__":
    main()