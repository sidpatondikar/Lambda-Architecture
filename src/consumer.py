import json
from kafka import KafkaConsumer

def create_consumer(topic_name):
    """Create and return a Kafka consumer instance."""
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="user-data-consumer-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def process_transactions(tx):
    """Process received transactions."""
    # In a real application, this could include data transformation,
    # saving to database, triggering other events, etc.
    