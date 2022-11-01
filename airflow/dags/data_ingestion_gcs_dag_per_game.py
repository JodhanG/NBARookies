import os
import logging
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import lxml
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
dataset_file = "nba_players_per_game.csv"


url = 'https://www.basketball-reference.com/leagues/NBA_2023_per_game.html'

page = requests.get(url)

soup = BeautifulSoup(page.text, "lxml")


table = soup.find('table', id='per_game_stats')

headers = [th.getText() for th in table.find_all('tr', limit=2)[0].find_all('th')]

#Add underscores to strings that start with a number as those strings are not compatible with BigQuery
for i, header in enumerate(headers):
    if header[0].isnumeric() == True:
        headers[i] = '_' + header

headers.pop(0)

rows = table.find_all('tr')[1:]
rows_data = [[td.getText() for td in rows[i].find_all('td')]
                    for i in range(len(rows))]

rows_data = [x for x in rows_data if x]

nba_players = pd.DataFrame(rows_data, columns=headers)
nba_players.index = np.arange(1, len(nba_players) + 1)


nba_players.to_csv("nba_players_per_game.csv", index=False)

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'nba_rookie_data')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag_per_game",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['de-nba'],
) as dag:

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "player_per_game_totals",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task