import json 
import csv 
import requests
import re
from http.client import responses
from jsonpath_ng import jsonpath, parse as jparse
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class NovigiIfsSharePointQAExportOperator(BaseOperator):

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

    def apiCall(self,id_value):
        self.id_value = id_value
        # url to make the post request with author_id
        API_ENDPOINT = "https://industryfundservice.sharepoint.com/sites/Unpaidsuper/_api/web/lists/getbytitle('User Information List')/Items("+ str(self.id_value) +")"
        # defining the parameters
        PARAMS = {'$select':'Id, Title'}
        # make the get request with parameters
        url_data = requests.get(url = API_ENDPOINT, params = PARAMS, headers = self.api_headers)

        # convert returned data into json format
        json_data = url_data.json()
        
        mapping_value = json_data["d"]["Title"]

        # return the value

        return mapping_value



    def execute(self, context):


            
            response = requests.request(self.req_type, self.api_url ,headers = self.api_headers,data = self.pload)
           
            sucess_range = re.search(r'2[0-9][0-9]', str(response.status_code))
            if not (sucess_range):
                raise AirflowException("Response status code is " + str(response.status_code) + " and response status is '"+ responses[response.status_code]+"'")
            
            data = response.json()
          
            jsonpath_expression = jparse(self.json_path)

            match = jsonpath_expression.find(data)
            
            if len(match) > 0:    

                result = match[0].value

            else:
                raise AirflowException('can not get match') 

            # now we will open a file for writing 
            data_file = open(self.output_csv, 'w') 

            # create the csv writer object 
            csv_writer = csv.writer(data_file) 

            
            # Writing headers of CSV file 
            header = self.mapping.keys()
            
            csv_writer.writerow(header)  


            for values_in_result in result: 
                
                row = []
                
                #csv_writer.writerow(mapping.values())
                for each_value in self.mapping.values(): 

                    print('Each Value : ', each_value)
                    jsonpath_exp = jparse(each_value)
                    matcher = jsonpath_exp.find(values_in_result)
                   
                    mapping_value = ''
                    if len(matcher) > 0:
                        
                        if each_value not in ('$.AuthorId' , '$.EditorId' , '$.CheckerId' , '$.OfficerId'):

                           mapping_value = matcher[0].value
                           
                        else:
                            
                            id_value = matcher[0].value
                            
                            # get the  name from the returned values
                            mapping_value = self.apiCall(id_value)
                            
                    row.append(mapping_value)
                    
                csv_writer.writerow(row)

            data_file.close() 
           
            return True
