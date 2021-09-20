from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.novigi_json_to_csv import NovigiJsonToCSVExportOperator
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

dag = DAG('dag_json_to_csv',
        default_args=default_args,
        #schedule_interval="50 22 * * *",
        tags=['json', 'to_csv'])

t0 = NovigiJsonToCSVExportOperator(
        task_id='dag_json_to_csv',
        api_url= 'https://run.mocky.io/v3/9091b5c4-064f-4249-9050-f780d817d2b5',
        req_type= 'GET',
        output_csv= '/home/prasadi/Desktop/01.csv',
        json_path = '$.data',
        api_headers= '',
        pay_load= '',
        mapping= {'FNAME' : '$.name','AGE' : '$.age','ADDRESS_L1' : '$.address.line1','ADDRESS_L2' : '$.address.line2','ZIP' : '$.address.code.number'},
        dag=dag,
        auto_commit=True)

t0
