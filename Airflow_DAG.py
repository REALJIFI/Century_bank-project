from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import sys
import os

# Assuming these scripts are in a directory relative to the DAG file
sys.path.insert(0, os.path.abspath(r"C:Users/back2/Desktop/DESORTED_FILEZ/CENTURYBANKPROJECT/dags/modules"))

from Extract import extract_csv_to_dataframe
from transform import transform_dataframe
from create_dataframe_table import create_dataframe_tables
from load import get_db_connection, create_database_schema, load_dataframe_to_postgres


default_args = {
    'owner': 'century_bank',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['ifigeorgeifi@yahoo.com'],
    'retries': None,
    'retry_delay': None,
}

# Instantiate the Dag
with DAG(
    dag_id='century_bank_etl_dag',
    default_args=default_args,
    description='ETL pipeline for Century Bank data to PostgreSQL',
    schedule_interval='@monthly',  # Or use None for manual trigger
    start_date=datetime(2018, 7, 28),
    catchup=False,
    tags=['pyspark', 'postgresql', 'etl'],
) as dag:

    # Task 1
    start_task = DummyOperator(
        task_id='start_pipeline'
    )

    # Task 2
    extract_task = PythonOperator(
        task_id='extract_raw_data',
        python_callable=extract_csv_to_dataframe,
        op_kwargs={'csv_filepath': './dag/Raw_data/Century_bank_transactions.csv'} # Pass the file path as an argument
    )

    # Task 3
    transform_task = PythonOperator(
        task_id='transform_dataframe',
        python_callable=transform_dataframe,
        # You'll likely need to pass the output of the extract task here using XComs
    )

    # Task 4: Create Database Tables
    create_df_task = PythonOperator(
        task_id='create_dataframe_tables',
        python_callable=create_dataframe_tables,
    )

    # Task 5: Load to Database
    db_connect_task = PythonOperator(
        task_id='initiate_database connection',
        python_callable=get_db_connection,
    )

     # Task 6: Load to Database
    create_table_task = PythonOperator(
        task_id='load_to_database',
        python_callable=create_database_schema,
    )

     # Task 6: Load to Database
    load_task = PythonOperator(
        task_id='load_to_database',
        python_callable=load_dataframe_to_postgres,
    )

    End_task = DummyOperator(task_id='end_pipeline')
    # Define task dependencies
    start_task >> extract_task >> transform_task >> create_df_task >> db_connect_task >> create_table_task >> load_task >> End_task