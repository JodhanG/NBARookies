#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import requests
from bs4 import BeautifulSoup
import lxml
import numpy as np
from sqlalchemy import create_engine
import argparse

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name

    
    
    
    url = 'https://www.basketball-reference.com/leagues/NBA_2023_rookies.html#rookies'

    page = requests.get(url)

    soup = BeautifulSoup(page.text, "lxml")


    table = soup.find('table', id='rookies')

    headers = [th.getText() for th in table.find_all('tr', limit=2)[1].find_all('th')]
    
    #BBall Reference lists these average per game stats under the same headers as career totals, so fixed for clarity
    averages = ['MPG', 'PPG', 'RPG', 'APG']
    headers[-4:] = averages
    headers.pop(0)
    



    rows = table.find_all('tr')[2:]
    rows_data = [[td.getText() for td in rows[i].find_all('td')]
                        for i in range(len(rows))]




    nba_rookies = pd.DataFrame(rows_data, columns=headers)
    nba_rookies.index = np.arange(1, len(nba_rookies) + 1)


    nba_rookies.drop([21,22,43,44], inplace=True)
    nba_rookies.drop(columns=['Debut', 'Age'], inplace=True)

    nba_rookies.to_csv("nba_rookies.csv", index=False)


    df = pd.read_csv('nba_rookies.csv')



    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')



    engine.connect()



    df.to_sql(con=engine, name=table_name, if_exists='replace')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table that will be written to')

    args = parser.parse_args()

    main(args)







