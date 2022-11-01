from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os
import logging
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import lxml
from time import time
from sqlalchemy import create_engine

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output.csv'
TABLE_NAME_TEMPLATE = 'nba_rookies_{{ execution_date.strftime(\'%Y_%m\') }}'


def scrape_data():

    url = 'https://www.basketball-reference.com/leagues/NBA_2023_rookies.html#rookies'

    page = requests.get(url)

    soup = BeautifulSoup(page.text, "lxml")


    table = soup.find('table', id='rookies')

    headers = [th.getText() for th in table.find_all('tr', limit=2)[1].find_all('th')]

    #BBall Reference lists these average per game stats under the same headers as career totals, so fixed for clarity
    averages = ['MPG', 'PPG', 'RPG', 'APG']
    headers[-4:] = averages
    headers.pop(0)
    #Add underscores to strings that start with a number as those strings are not compatible with BigQuery
    for i, header in enumerate(headers):
        if header[0].isnumeric() == True:
            headers[i] = '_' + header
    rows = table.find_all('tr')[2:]
    rows_data = [[td.getText() for td in rows[i].find_all('td')]
                        for i in range(len(rows))]

    nba_rookies = pd.DataFrame(rows_data, columns=headers)
    nba_rookies.index = np.arange(1, len(nba_rookies) + 1)


    nba_rookies.drop([21,22,43,44], inplace=True)
    nba_rookies.drop(columns=['Debut', 'Age'], inplace=True)

    nba_rookies.to_csv(OUTPUT_FILE_TEMPLATE, index=False)

def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
    print(table_name, csv_file, execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')

   
    df = pd.read_csv(csv_file)

    df.to_sql(name=table_name, con=engine, if_exists='append')



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
}

local_workflow = DAG(
    "LocalIngestionDAG",
    schedule_interval="@daily",
    default_args=default_args
)

with local_workflow:
    
    get_data_task = PythonOperator(
        task_id="scrape_data_task",
        python_callable=scrape_data,
    )


    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=OUTPUT_FILE_TEMPLATE
        ),
    )



get_data_task >> ingest_task