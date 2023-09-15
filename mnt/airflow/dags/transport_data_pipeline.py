from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


from datetime import datetime, timedelta
import pandas as pd
import csv
import requests
import json

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "hkituyi@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def split_csv_by_weekday_weekend(input_csv_file, output_weekday_json, output_weekend_json):
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

# Example usage:

input_csv_file = '/opt/airflow/dags/files/transport_details.csv'
output_weekday_json = '/opt/airflow/dags/files/weekday_data.json'
output_weekend_json = '/opt/airflow/dags/files/weekend_data.json'

#split_csv_by_weekday_weekend(input_csv_file, output_weekday_json, output_weekend_json)


def _get_message() -> str:
    return "Hi from transport_data_pipeline"

with DAG("transport_data_pipeline", start_date=datetime(2023, 11, 11), 
    schedule_interval="@once", default_args=default_args, catchup=False) as dag:

    is_transport_data_available = HttpSensor (
        task_id="is_transport_data_available",
        http_conn_id="transport_api",
        endpoint="gateway",
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
        python_callable= split_csv_by_weekday_weekend
        
    )
    split_csv_by_weekday_weekend(input_csv_file, output_weekday_json, output_weekend_json)

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

    


    is_transport_data_available >> is_transport_data_file_available >> downloading_transport_data >> saving_weekday 
    saving_weekday >> saving_weekend >> creating_weekday_table >> creating_weekend_table >> send_email_notification
    send_email_notification 