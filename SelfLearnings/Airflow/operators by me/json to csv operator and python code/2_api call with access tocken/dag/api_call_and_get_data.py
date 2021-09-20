from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
from airflow.hooks.base_hook import BaseHook
import pendulum
from datetime import datetime
from airflow.models import Variable
#from airflow.operators.novigi_json_to_csv import NovigiJsonToCSVExportOperator
import novigi_airflow
from novigi_airflow.novigi_json_to_csv import NovigiJsonToCSVExportOperator
#import poosa
#from poosa.novigi_xls_to_csv_operator import NovigiXlsToCSVOperator
#from poosa.novigi_json_to_csv import NovigiJsonToCSVExportOperator

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
    access_token_url = Variable.get("test_token")
    payload = {
    "grant_type": "client_credentials",
    "client_id": Variable.get("client_id_for_json_to_csv_extended") ,
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
        print('****************************************************')
        print(access_token)
        print('****************************************************')

        
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
        api_url= Variable.get("quality_assurance_share_point_list_api_url"),
        req_type= 'GET',
        output_csv= '/home/prasadi/Desktop/TODOOOO.csv',
        json_path = '$.d.results',
        api_headers= {'authorization': 'Bearer '+"{{ti.xcom_pull(key='access_token')}}",'accept': Variable.get("accept_type") },
        mapping ={
                    'Created' : '$.Created',
                    'Created By' : '$.AuthorId',
                    'Date of check' : '$.Dateofcheck',
                    'Checker' : '$.CheckerId',
                    'Officer' : '$.OfficerId',
                    'Fund name' : '$.Fundname',
                    'Employer number' : '$.Employernumber',
                    'CheckerDate of contact/note' : '$.Dateofcontact_x002f_note',
                    'Call recording available?' : '$.Callrecordingavailable_x003f_',
                    'What process step was the contact/action associated with?' : '$.Whatprocessstepwasthecontact_x00',
                    '1.1 Have all compliance areas been met?' : '$.OData__x0031__x002e_1_x0020_Have_x0020',
                    '1.1 Notes' : '$.OData__x0031__x002e_1Notes',
                    '2.1 All business rules and processes followed' : '$.OData__x0032__x002e_1Allbusinessrulesa',
                    '2.1 Notes' : '$.OData__x0032__x002e_1Notes',
                    '2.2 Correct and complete information provided and obtained during the contact/referral' : '$.OData__x0032__x002e_2Correctandcomplet',
                    '2.2 Notes' : '$.OData__x0032__x002e_2Notes',
                    '2.3 Wincollect notes appropriate and accurate' : '$.OData__x0032__x002e_3Wincollectnotesap',
                    '2.3 Notes' : '$.OData__x0032__x002e_3Notes',
                    '2.4 Any information required by or received from during the contact has been actioned and is appropriate' : '$.OData__x0032__x002e_4Anyinformationreq',
                    '2.4 Notes' : '$.OData__x0032__x002e_4Notes',
                    '2.5 Checks conducted and appropriate approvals gained (where applicable)' : '$.OData__x0032__x002e_5Checksconductedan',
                    '2.5 Notes' : '$.OData__x0032__x002e_5Notes',
                    '3.1 Builds relationship' : '$.OData__x0033__x002e_1Buildsrelationshi',
                    '3.1 Notes' : '$.OData__x0033__x002e_1Notes',
                    '3.2 Controls call' : '$.OData__x0033__x002e_2Controlscall',
                    '3.2 Notes' : '$.OData__x0033__x002e_2Notes',
                    '3.3 Easy to understand' : '$.OData__x0033__x002e_3Easytounderstand',
                    '3.3 Notes' : '$.OData__x0033__x002e_3Notes',
                    '3.4 Service accountability' : '$.OData__x0033__x002e_4Serviceaccountabi',
                    '3.4 Notes' : '$.OData__x0033__x002e_4Notes',
                    '1.1 Score' : '$.OData__x0031__x002e_1_x0020_Score',
                    '2.1 Score': '$.OData__x0032__x002e_1_x0020_Score',
                    '2.2 Score': '$.OData__x0032__x002e_2_x0020_Score',
                    '2.3 Score': '$.OData__x0032__x002e_3_x0020_Score',
                    '2.4 Score': '$.OData__x0032__x002e_4_x0020_Score',
                    '2.5 Score': '$.OData__x0032__x002e_5_x0020_Score',
                    '3.1 Score': '$.OData__x0033__x002e_1_x0020_Score',
                    '3.2 Score': '$.OData__x0033__x002e_2_x0020_Score',
                    '3.3 Score': '$.OData__x0033__x002e_3_x0020_Score',
                    '3.4 Score': '$.OData__x0033__x002e_4_x0020_Score',
                    '1.0 Compliance score': '$.Compliance_x0020_score',
                    '2.0 Business Process score': '$.OData__x0032__x002e_0_x0020_Business_x',
                    '3.0 Customer Experience score': '$.OData__x0033__x002e_0_x0020_Customer_x',
                    'Total score' : '$.Total_x0020_score',
                    'Item' : '$.__metadata.id',
                    'Type' : '$.__metadata.type',
                    'Path' : '$.__metadata.uri'
                },
        dag=dag,
        auto_commit=True
        )


task_get_access_tocken >> t0 
