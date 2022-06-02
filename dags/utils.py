from stackapi import StackAPI
from iso3166 import countries
SITENAME = 'stackoverflow'
SITE = StackAPI(SITENAME,key = 'bkpnLeaeXnpiAKADkoo2ig((')
SITE.page_size = 100
import json
import unidecode
import datetime 
import time
import pandas as pd
import psycopg2
import numpy as np
import psycopg2.extras as extras
from html import unescape
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
pd.set_option('display.max_columns', None)


def insert_into_table(ds,table):  
    date = ds
    #tuples = [tuple(x) for x in df.to_numpy()]
    with open("/opt/airflow/dags/files/raw/"+table+"/"+table+"-"+str(date)+".csv", 'r',encoding='utf-8') as f:
        data=[tuple(line) for line in csv.reader(f)]
    tuples = data[1:]
    cols = ','.join(list(data[0]))
    print(cols)    

    #cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO raw.\"%s\"(%s) VALUES %%s" % (table, cols)
    print(query)
    postgres_hook = PostgresHook(postgres_conn_id="LOCAL")
    conn = postgres_hook.get_conn()
    #conn = psycopg2.connect( database="stackoverflow", user='postgres', password='postgres', host='127.0.0.1', port= '5432',options=f'-c search_path=raw')
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
        print(table,"inserted")
        return 0
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    
    cursor.close()

def insert_csv_into_table():
    ds = '2022-03-02'
    insert_into_table(ds,'question')
    insert_into_table(ds,'answer')
    insert_into_table(ds,'account')