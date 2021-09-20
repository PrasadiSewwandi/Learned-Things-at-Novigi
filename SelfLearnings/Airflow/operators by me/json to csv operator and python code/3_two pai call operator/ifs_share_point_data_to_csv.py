import json 
import csv 
import requests
import re
from http.client import responses
from jsonpath_ng import jsonpath, parse as jparse

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class NovigiIfsSharePointTimeCaptureExportOperator(BaseOperator):

    template_fields = ['api_headers']
    

    def __init__(self, api_url: str, req_type = "GET", output_csv = "", json_path: str = None, api_headers: dict = None, pay_load: dict = None, mapping: dict = {}, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.api_url = api_url
            self.req_type = req_type
            self.output_csv = output_csv
            self.json_path = json_path
            self.api_headers = api_headers
            self.pload = pay_load
            self.mapping = mapping


    def execute(self, context):
            
            response = requests.request(self.req_type, self.api_url ,headers = self.api_headers,data = self.pload)
           
            sucess_range = re.search(r'2[0-9][0-9]', str(response.status_code))
            if not (sucess_range):
                raise AirflowException("Response status code is " + str(response.status_code) + " and response status is '"+ responses[response.status_code]+"'")
            print('****---------------------------------------------***')
            print(response.status_code)
            print('****---------------------------------------------***')
            data = response.json()
          
            jsonpath_expression = jparse(self.json_path)

            match = jsonpath_expression.find(data)
            print('****---------------------------------------------***')
            print(match)
            print('****---------------------------------------------***')
            if len(match) > 0:    

                result = match[0].value
                print('****---------------------------------------------***')
                print('match found')
                print('****---------------------------------------------***')

            else:
                raise AirflowException('can not get match') 

            # now we will open a file for writing 
            data_file = open(self.output_csv, 'w') 
            print('****---------------------------------------------***')
            print('file opened')
            print('****---------------------------------------------***')

            # create the csv writer object 
            csv_writer = csv.writer(data_file) 
            print('****---------------------------------------------***')
            print('create the csv writer object')
            print('****---------------------------------------------***')

            
            # Writing headers of CSV file 
            header = self.mapping.keys()
            
            csv_writer.writerow(header)  

            print('****---------------------------------------------***')
            print('Writing headers of CSV file')
            print('****---------------------------------------------***')

            for values_in_result in result: 
                print('****---------------------------------------------***')
                print('inside the for loop')
                print('****---------------------------------------------***')
                row = []
                #csv_writer.writerow(mapping.values())
                for each_value in self.mapping.values(): 

                    print('****---------------------------------------------***')
                    print('inside the for each loop')
                    print('****---------------------------------------------***')
                    index = list(self.mapping.values()).index(each_value)
                    print('The index of e:', index)
                    jsonpath_exp = jparse(each_value)
                    matcher = jsonpath_exp.find(values_in_result)
                   
                    mapping_value = ''
                    if len(matcher) > 0:
                        if index == 3:
                            member_id = matcher[0].value
                             # url to make the post request with member_id
                            API_ENDPOINT = "https://industryfundservice.sharepoint.com/sites/Unpaidsuper/_api/web/lists/getbytitle('User Information List')/Items("+ str(member_id) +")"
                            # defining the parameters
                            PARAMS = {'$select':'Id, Title'}
                            # make the get request with parameters
                            url_data = requests.get(url = API_ENDPOINT, params = PARAMS, headers = self.api_headers)

                            # convert returned data into json format
                            json_data = url_data.json()
                            # get the member name from the returned values
                            mapping_value = json_data["d"]["Title"]

                        elif index == 8:
                            other_time_category = str(matcher[0].value)
                            
                            mapping_value =other_time_category.replace("–", "-")
                        else:
                            mapping_value = matcher[0].value

                    row.append(mapping_value)
                csv_writer.writerow(row)

            data_file.close() 
           
            return True
