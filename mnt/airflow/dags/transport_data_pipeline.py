from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable


from datetime import datetime, timedelta
import pandas as pd
import csv
import requests
import json

# Set the input and output file paths as variables
Variable.set("input_csv_file", '/opt/airflow/dags/files/transport_details.csv')
Variable.set("output_weekday_json", '/opt/airflow/dags/files/weekday_data.json')
Variable.set("output_weekend_json", '/opt/airflow/dags/files/weekend_data.json')

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "hkituyi@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def split_csv_by_weekday_weekend(**kwargs):
    input_csv_file = Variable.get("input_csv_file")
    output_weekday_json = Variable.get("output_weekday_json")
    output_weekend_json = Variable.get("output_weekend_json")
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_csv_file, parse_dates=['datetime'])

    # Extract the day of the week (0 = Monday, 6 = Sunday)
    df['day_of_week'] = df['datetime'].dt.dayofweek

    # Split the data into weekday and weekend
    weekday_df = df[df['day_of_week'] < 5]  # 0-4 are weekdays (Mon-Fri)
    weekend_df = df[df['day_of_week'] >= 5]  # 5-6 are weekends (Sat-Sun)

    # Convert the 'vel' column to numeric, handling non-numeric values as NaN
    weekday_df['vel'] = pd.to_numeric(weekday_df['vel'], errors='coerce')
    weekend_df['vel'] = pd.to_numeric(weekend_df['vel'], errors='coerce')

    # Drop rows with missing 'vel' values
    weekday_df.dropna(subset=['vel'], inplace=True)
    weekend_df.dropna(subset=['vel'], inplace=True)

    # Round the 'vel' field to 2 decimal points
    weekday_df['vel'] = weekday_df['vel'].round(2)
    weekend_df['vel'] = weekend_df['vel'].round(2)

    # Drop the 'day_of_week' column
    weekday_df = weekday_df.drop(columns=['day_of_week'])
    weekend_df = weekend_df.drop(columns=['day_of_week'])

    # Write the filtered DataFrames to separate JSON files
    weekday_df.to_json(output_weekday_json, orient='records', lines=True)
    weekend_df.to_json(output_weekend_json, orient='records', lines=True)

# usage:

#input_csv_file = '/opt/airflow/dags/files/transport_details.csv'
#output_weekday_json = '/opt/airflow/dags/files/weekday_data.json'
#output_weekend_json = '/opt/airflow/dags/files/weekend_data.json'

#split_csv_by_weekday_weekend(input_csv_file, output_weekday_json, output_weekend_json)


def _get_message() -> str:
    return "Hi from transport_data_pipeline"

with DAG("transport_data_pipeline", start_date=datetime(2023, 1, 1), 
    schedule_interval='0 0 1 1,4,7,10 *', default_args=default_args, catchup=False) as dag:

    is_transport_data_available = HttpSensor (
        task_id="is_transport_data_available",
        http_conn_id="transport_api",
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "data" in response.text,
        poke_interval=5,
        timeout=20
    )


    is_transport_data_file_available = FileSensor(
        task_id="is_transport_data_file_available",
        fs_conn_id="transport_path",
        filepath="transport_details.csv",
        poke_interval=5,
        timeout=20
    )

   

    downloading_transport_data = PythonOperator(
        task_id="downloading_transport_data",
        python_callable=split_csv_by_weekday_weekend,
        provide_context=True
    )
        
     

    # Define a TaskGroup for saving tasks (saving_weekday and saving_weekend)
    with TaskGroup("saving_tasks") as saving_tasks:
        saving_weekday = BashOperator(
            task_id="saving_weekday",
            bash_command="""
                hdfs dfs -mkdir -p /weekday && \
                hdfs dfs -put -f $AIRFLOW_HOME/dags/files/weekday_data.json /weekday
            """
        )

        saving_weekend = BashOperator(
            task_id="saving_weekend",
            bash_command="""
                hdfs dfs -mkdir -p /weekend && \
                hdfs dfs -put -f $AIRFLOW_HOME/dags/files/weekend_data.json /weekend
            """
        )

    # Define a TaskGroup for creating tables tasks (creating_weekday_table and creating_weekend_table)
    with TaskGroup("creating_tables_tasks") as creating_tables_tasks:
        creating_weekday_table = HiveOperator(
            task_id="creating_weekday_table",
            hive_cli_conn_id="hive_conn",
            hql="""
                CREATE EXTERNAL TABLE IF NOT EXISTS weekday_traffic(
                    datetime TIMESTAMP,
                    street_id DECIMAL(10, 2),
                    count INT,
                    vel DECIMAL(10, 2)
                    )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
            """
        )

        creating_weekend_table = HiveOperator(
            task_id="creating_weekend_table",
            hive_cli_conn_id="hive_conn",
            hql="""
                CREATE EXTERNAL TABLE IF NOT EXISTS weekend_traffic(
                    datetime TIMESTAMP,
                    street_id DECIMAL(10, 2),
                    count INT,
                    vel DECIMAL(10, 2)
                    )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
            """
        )

    # Define the remaining tasks (weekday_processing, weekend_processing, send_email_notification)
    weekday_processing = SparkSubmitOperator(
        task_id="weekday_processing",
        application="/opt/airflow/dags/scripts/weekday_processing.py",
        conn_id="spark_conn",
        verbose=False
    )

    weekend_processing = SparkSubmitOperator(
        task_id="weekend_processing",
        application="/opt/airflow/dags/scripts/weekend_processing.py",
        conn_id="spark_conn",
        verbose=False
    )

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="hegilc@yahoo.com",
        subject="belgium_traffic_pipeline",
        html_content="<h3>belgium_traffic_pipeline</h3>"
    )

    # Set up task dependencies using TaskGroups
    is_transport_data_available >> is_transport_data_file_available >> downloading_transport_data
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from datetime import datetime, timedelta
import pandas as pd
import csv
import json

# Set the input and output file paths as variables
Variable.set("input_csv_file", '/opt/airflow/dags/files/transport_details.csv')
Variable.set("output_weekday_json", '/opt/airflow/dags/files/weekday_data.json')
Variable.set("output_weekend_json", '/opt/airflow/dags/files/weekend_data.json')

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "hkituyi@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def split_csv_by_weekday_weekend(**kwargs):
    input_csv_file = Variable.get("input_csv_file")
    output_weekday_json = Variable.get("output_weekday_json")
    output_weekend_json = Variable.get("output_weekend_json")
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_csv_file, parse_dates=['datetime'])

    # Extract the day of the week (0 = Monday, 6 = Sunday)
    df['day_of_week'] = df['datetime'].dt.dayofweek

    # Split the data into weekday and weekend
    weekday_df = df[df['day_of_week'] < 5]  # 0-4 are weekdays (Mon-Fri)
    weekend_df = df[df['day_of_week'] >= 5]  # 5-6 are weekends (Sat-Sun)

    # Convert the 'vel' column to numeric, handling non-numeric values as NaN
    weekday_df['vel'] = pd.to_numeric(weekday_df['vel'], errors='coerce')
    weekend_df['vel'] = pd.to_numeric(weekend_df['vel'], errors='coerce')

    # Drop rows with missing 'vel' values
    weekday_df.dropna(subset=['vel'], inplace=True)
    weekend_df.dropna(subset=['vel'], inplace=True)

    # Round the 'vel' field to 2 decimal points
    weekday_df['vel'] = weekday_df['vel'].round(2)
    weekend_df['vel'] = weekend_df['vel'].round(2)

    # Drop the 'day_of_week' column
    weekday_df = weekday_df.drop(columns=['day_of_week'])
    weekend_df = weekend_df.drop(columns=['day_of_week'])

    # Write the filtered DataFrames to separate JSON files
    weekday_df.to_json(output_weekday_json, orient='records', lines=True)
    weekend_df.to_json(output_weekend_json, orient='records', lines=True)

with DAG("transport_data_pipeline", start_date=datetime(2023, 1, 1), 
    schedule_interval='0 0 1 1,4,7,10 *', default_args=default_args, catchup=False) as dag:

    is_transport_data_available = HttpSensor (
        task_id="is_transport_data_available",
        http_conn_id="transport_api",
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "data" in response.text,
        poke_interval=5,
        timeout=20
    )

    is_transport_data_file_available = FileSensor(
        task_id="is_transport_data_file_available",
        fs_conn_id="transport_path",
        filepath="transport_details.csv",
        poke_interval=5,
        timeout=20
    )

    downloading_transport_data = PythonOperator(
        task_id="downloading_transport_data",
        python_callable=split_csv_by_weekday_weekend,
        provide_context=True
    )

    # Define a TaskGroup for saving tasks (saving_weekday and saving_weekend)
    with TaskGroup("saving_tasks") as saving_tasks:
        saving_weekday = BashOperator(
            task_id="saving_weekday",
            bash_command="""
                hdfs dfs -mkdir -p /weekday && \
                hdfs dfs -put -f $AIRFLOW_HOME/dags/files/weekday_data.json /weekday
            """
        )

        saving_weekend = BashOperator(
            task_id="saving_weekend",
            bash_command="""
                hdfs dfs -mkdir -p /weekend && \
                hdfs dfs -put -f $AIRFLOW_HOME/dags/files/weekend_data.json /weekend
            """
        )

    # Define a TaskGroup for creating tables tasks (creating_weekday_table and creating_weekend_table)
    with TaskGroup("creating_tables_tasks") as creating_tables_tasks:
        creating_weekday_table = HiveOperator(
            task_id="creating_weekday_table",
            hive_cli_conn_id="hive_conn",
            hql="""
                CREATE EXTERNAL TABLE IF NOT EXISTS weekday_traffic(
                    datetime TIMESTAMP,
                    street_id DECIMAL(10, 2),
                    count INT,
                    vel DECIMAL(10, 2)
                    )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
            """
        )

        creating_weekend_table = HiveOperator(
            task_id="creating_weekend_table",
            hive_cli_conn_id="hive_conn",
            hql="""
                CREATE EXTERNAL TABLE IF NOT EXISTS weekend_traffic(
                    datetime TIMESTAMP,
                    street_id DECIMAL(10, 2),
                    count INT,
                    vel DECIMAL(10, 2)
                    )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
            """
        )

    # Define the remaining tasks (weekday_processing, weekend_processing, send_email_notification)
    weekday_processing = PythonOperator(
        task_id="weekday_processing",
        python_callable=_get_message
    )

    weekend_processing = PythonOperator(
        task_id="weekend_processing",
        python_callable=_get_message
    )

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="hegilc@yahoo.com",
        subject="belgium_traffic_pipeline",
        html_content="<h3>belgium_traffic_pipeline</h3>"
    )

    # Set up task dependencies using TaskGroups
    is_transport_data_available >> is_transport_data_file_available >> downloading_transport_data

    downloading_transport_data >> saving_tasks  # saving_weekday and saving_weekend run in parallel

    saving_tasks >> creating_tables_tasks  # creating_weekday_table and creating_weekend_table run in parallel

    creating_tables_tasks >> [weekday_processing, weekend_processing] >> send_email_notification

    downloading_transport_data >> saving_tasks  # saving_weekday and saving_weekend run in parallel

    saving_tasks >> creating_tables_tasks  # creating_weekday_table and creating_weekend_table run in parallel

    creating_tables_tasks >> weekday_processing >> send_email_notification
    creating_tables_tasks >> weekend_processing >> send_email_notification
    send_email_notification