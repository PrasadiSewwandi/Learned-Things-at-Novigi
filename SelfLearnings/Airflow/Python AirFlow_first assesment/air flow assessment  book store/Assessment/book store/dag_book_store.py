from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.novigi_json_to_csv import BookStoreOperator
import pendulum
from datetime import datetime
from airflow.models import Variable

local_tz = pendulum.timezone("Australia/Sydney")


default_args = {
        'owner': 'cfy',
        'depends_on_past': False,
        'start_date': airflow.utils.dates.days_ago(1),
        'email': ['prasadi.jayakodi@novigi.com.au'],
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
}

dag = DAG('dag_book_store',
        default_args=default_args,
        schedule_interval="50 22 * * *",
        tags=['json', 'to_csv'])

t0 = BookStoreOperator(
        task_id='dag_book_store',
        api_url= 'https://api.itbook.store/1.0/new',
        req_type= 'GET',
        output_csv= '/home/prasadi/Desktop/book_store_333.csv',
        json_path = '$.books',
        api_headers= '',
        pay_load= '',
        mapping= {'TITTLE' : '$.title','SUB_TITTLE' : '$.subtitle','ISBN' : '$.isbn13','PRICE' : '$.price'},
        dag=dag,
        auto_commit=True)

t0
