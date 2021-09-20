from datetime import timedelta
import airflow
import logging
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.novigi_excel_to_csv import NovigiExcelToCSVExportOperator
import pendulum
from datetime import datetime

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

dag = DAG(
    "excel_to_csv"
    ,default_args=default_args
    ,schedule_interval="50 22 * * *"
    , tags=['Manual_Data']
)

t0 = BashOperator(
        dag=dag,
        task_id="latest_file",
        bash_command="ls -trd /home/prasadi/Desktop/Importance/previouse_work/excell_task/*.xls* | tail -n 1",
        xcom_push=True)

# -------------------EXCEL to CSV conversion ------------------------
t1 = NovigiExcelToCSVExportOperator(
        task_id="read_manual_data_file",
        #input_excel= "/home/prasadi/Desktop/Importance/previouse_work/excell_task/All Funds 2021-04-21 Pmt plan pmts rcvd 01-01-20 to 21-04-21.xls",
        input_excel= "{{task_instance.xcom_pull(task_ids='latest_file')}}",
        output_csv= "/home/prasadi/Desktop/doggy.csv",
        sheet_name="qryPmtsRcvdInDteRange_pt41",
        dag = dag)
 	


t0 >> t1 
