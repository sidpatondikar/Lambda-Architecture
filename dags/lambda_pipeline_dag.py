# File: dags/lambda_pipeline_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lambda_pipeline_dag",
    default_args=default_args,
    description="Orchestrate Lambda Architecture for credit card transactions",
    schedule_interval=None,  # Trigger manually or via API
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Load data from CSV to MySQL
    task_load_data = BashOperator(
        task_id="load_data",
        bash_command=(
            "cd /home/sidx129/project-root/project-4 && "
            "python3 src/main.py"
        ),
        env={
            "MYSQL_URL": "jdbc:mysql://localhost:3306/transactions_cc",
            "MYSQL_USER": "sidx129",
            "MYSQL_PASSWORD": "12999",
            "MYSQL_DB": "transactions_cc",
            "CARDS_TABLE": "cards",
            "CUSTOMERS_TABLE": "customers",
            "TRANSACTIONS_TABLE": "transactions",
            "CC_TYPES_TABLE": "credit_card_types",
            "OUTPUT_PATH": "data/results",
            "MYSQL_CONNECTOR_PATH": "/home/sidx129/lib/mysql/mysql-connector-j-9.2.0.jar",
        },
    )

    # Task 2: Run producer and consumer in parallel in a single task
    task_stream_processing = BashOperator(
        task_id="stream_processing",
        bash_command=(
            "python3 /home/sidx129/project-root/project-4/src/producer.py & "
            "python3 /home/sidx129/project-root/project-4/src/consumer.py; wait"
        ),
                env={
            "MYSQL_URL": "jdbc:mysql://localhost:3306/transactions_cc",
            "MYSQL_USER": "sidx129",
            "MYSQL_PASSWORD": "12999",
            "MYSQL_DB": "transactions_cc",
            "CARDS_TABLE": "cards",
            "CUSTOMERS_TABLE": "customers",
            "TRANSACTIONS_TABLE": "transactions",
            "CC_TYPES_TABLE": "credit_card_types",
            "OUTPUT_PATH": "data/results",
            "MYSQL_CONNECTOR_PATH": "/home/sidx129/lib/mysql/mysql-connector-j-9.2.0.jar",
        },
    )

    # Task 3: Perform batch processing
    task_batch_processing = BashOperator(
        task_id="batch_processing",
        bash_command=(
            "cd /home/sidx129/project-root/project-4 && "
            "python3 src/batch_processor.py"
        ),
        env={
            "MYSQL_URL": "jdbc:mysql://localhost:3306/transactions_cc",
            "MYSQL_USER": "sidx129",
            "MYSQL_PASSWORD": "12999",
            "MYSQL_DB": "transactions_cc",
            "CARDS_TABLE": "cards",
            "CUSTOMERS_TABLE": "customers",
            "TRANSACTIONS_TABLE": "transactions",
            "CC_TYPES_TABLE": "credit_card_types",
            "OUTPUT_PATH": "data/results",
            "MYSQL_CONNECTOR_PATH": "/home/sidx129/lib/mysql/mysql-connector-j-9.2.0.jar",
        },
    )

    # Orchestration dependencies
    task_load_data >> task_stream_processing >> task_batch_processing
