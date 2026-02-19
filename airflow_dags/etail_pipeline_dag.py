# File: airflow_dags/retail_pipeline_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# -----------------------
# Default DAG arguments
# -----------------------
default_args = {
    'owner': 'sunbeam',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# -----------------------
# DAG Definition
# -----------------------
dag = DAG(
    'retail_analytics_pipeline',
    default_args=default_args,
    description='Daily batch retail analytics pipeline',
    schedule_interval='@daily',  # runs every day
    catchup=False
)

# -----------------------
# Task 1: Generate Dimension CSVs
# -----------------------
generate_dimensions = BashOperator(
    task_id='generate_dimensions',
    bash_command=(
        'python3 /home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data_generator/generate_dim.py'
    ),
    dag=dag
)

# -----------------------
# Task 2: Generate Bronze Orders (JSON)
# -----------------------
generate_bronze_orders = BashOperator(
    task_id='generate_bronze_orders',
    bash_command=(
        'python3 /home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data_generator/bronze_generate_orders.py'
    ),
    dag=dag
)

# -----------------------
# Task 3: Silver ETL (Spark job)
# -----------------------
silver_etl = BashOperator(
    task_id='silver_etl',
    bash_command=(
        'spark-submit '
        '/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/scripts/silver_etl.py'
    ),
    dag=dag
)

# -----------------------
# Task 4: Gold Aggregation (Spark job)
# -----------------------
gold_aggregation = BashOperator(
    task_id='gold_aggregation',
    bash_command=(
        'spark-submit '
        '/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/scripts/gold_aggregation.py'
    ),
    dag=dag
)

# -----------------------
# Task 5 (Optional): Email report (example)
# -----------------------
# from airflow.operators.email import EmailOperator
# email_report = EmailOperator(
#     task_id='send_email_report',
#     to='management@example.com',
#     subject='Daily Retail Analytics Report',
#     html_content='Please find the Gold layer reports attached.',
#     files=['/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/gold/daily_product_sales.csv'],
#     dag=dag
# )

# -----------------------
# Task Dependencies
# -----------------------
generate_dimensions >> generate_bronze_orders >> silver_etl >> gold_aggregation
# >> email_report  # uncomment if using EmailOperator

