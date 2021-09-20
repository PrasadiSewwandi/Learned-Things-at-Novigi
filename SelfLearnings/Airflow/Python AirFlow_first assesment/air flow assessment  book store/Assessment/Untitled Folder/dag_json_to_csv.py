from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.novigi_json_to_csv import NovigiJsonToCSVExportOperator
from airflow.operators import BashOperator,PythonOperator
import pendulum
from datetime import datetime
from airflow.models import Variable
import requests
import logging
import json

local_tz = pendulum.timezone("Australia/Sydney")


default_args = {
        'owner': 'cfy',
        'depends_on_past': False,
        'start_date': airflow.utils.dates.days_ago(1),
        'provide_context': True,
        'email': ['prasadi.jayakodi@novigi.com.au'],
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
}

dag = DAG('dag_json_to_csv_extend',
        default_args=default_args,
        schedule_interval="50 22 * * *",
        tags=['json', 'to_csv'])

#Fetch access token
def get_access_token(**kwargs):
    logging.info('Retriving Auth token')
    access_token_url = "https://accounts.accesscontrol.windows.net/df4f030d-3803-48d3-9708-352dc239405a/tokens/OAuth/2"
    payload = {
    "grant_type": "client_credentials",
    "client_id": "b7e625d1-52dd-4a0f-bacc-f9811dfc6e7d@df4f030d-3803-48d3-9708-352dc239405a",
    "client_secret": "xWJMLFO4v4TBwSMzXnMCxupxwMSMKMj5A7wnnPh5ML0=",
    "resource": "00000003-0000-0ff1-ce00-000000000000/industryfundservice.sharepoint.com@df4f030d-3803-48d3-9708-352dc239405a"
    }
    headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/json"
    }
    access_token_response = requests.post(access_token_url, data =payload, headers=headers)
    
    if access_token_response.status_code == 200:
        logging.info('Retrieved Auth token')
        access_token = access_token_response.json()['access_token']
        print('###############')
        print(access_token)
        print('###########')
        print(access_token_response.json()['access_token'])
    else:
        logging.error('Error occurred when retrieving Zoho auth token.')
        access_token = ""
   
    kwargs['ti'].xcom_push(key='access_token', value=access_token)
   


task_get_access_tocken = PythonOperator(
    task_id='get_access_token',
    python_callable=get_access_token,
    dag=dag
)


t0 = NovigiJsonToCSVExportOperator(
        task_id='dag_json_to_csv',
        api_url= 'https://industryfundservice.sharepoint.com/sites/Unpaidsuper/_api/web/lists/getbytitle%28%27Time%20Capture%27%29/items',
        req_type= 'GET',
        output_csv= '/home/prasadi/Desktop/doneLasaPrasa.csv',
        json_path = '$.d.results',
        api_headers= {'authorization': "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Im5PbzNaRHJPRFhFSzFqS1doWHNsSFJfS1hFZyIsImtpZCI6Im5PbzNaRHJPRFhFSzFqS1doWHNsSFJfS1hFZyJ9.eyJhdWQiOiIwMDAwMDAwMy0wMDAwLTBmZjEtY2UwMC0wMDAwMDAwMDAwMDAvaW5kdXN0cnlmdW5kc2VydmljZS5zaGFyZXBvaW50LmNvbUBkZjRmMDMwZC0zODAzLTQ4ZDMtOTcwOC0zNTJkYzIzOTQwNWEiLCJpc3MiOiIwMDAwMDAwMS0wMDAwLTAwMDAtYzAwMC0wMDAwMDAwMDAwMDBAZGY0ZjAzMGQtMzgwMy00OGQzLTk3MDgtMzUyZGMyMzk0MDVhIiwiaWF0IjoxNjI0MTA5NjY1LCJuYmYiOjE2MjQxMDk2NjUsImV4cCI6MTYyNDE5NjM2NSwiaWRlbnRpdHlwcm92aWRlciI6IjAwMDAwMDAxLTAwMDAtMDAwMC1jMDAwLTAwMDAwMDAwMDAwMEBkZjRmMDMwZC0zODAzLTQ4ZDMtOTcwOC0zNTJkYzIzOTQwNWEiLCJuYW1laWQiOiJiN2U2MjVkMS01MmRkLTRhMGYtYmFjYy1mOTgxMWRmYzZlN2RAZGY0ZjAzMGQtMzgwMy00OGQzLTk3MDgtMzUyZGMyMzk0MDVhIiwib2lkIjoiNTc3MTBhMWEtNzA4Yi00ZWRlLTg1MzctYmU2Y2E5ZTk1Y2E5Iiwic3ViIjoiNTc3MTBhMWEtNzA4Yi00ZWRlLTg1MzctYmU2Y2E5ZTk1Y2E5IiwidHJ1c3RlZGZvcmRlbGVnYXRpb24iOiJmYWxzZSJ9.A5LihQOu1Txn6bj8G6NwSG_ojPkyg7Z_0goeIOLW4xezxaKrCy7t5tFg5O0yof560OdaPacaXNM_ZcBDQD2X3V1rLKVz26qTte1FBuNQ9G_vuHGdnO4oVA4lxZyM_v1u-ROB7_xYuJNK5cfD7qSGRTtUZ05ZcHP-v6bX2GlACORwmkl4JcFP9NxzR79X0nierxyzNE_zOBF3HecpjcpgLB-xmpRq3SSdij4YJXMrc6J_SeB6N-mECQzyXN3GuHDjlywQ1ADseBgnMXTquJuyZ5KyWQW0HfM5yBgroCeQJWRAPp9Pd91BIY1FEpwh-cqJyCGRRlTNhQqztlOSmiogKA",'accept': 'application/json;odata=verbose' },
        dag=dag,
        auto_commit=True)


task_get_access_tocken >> t0 
