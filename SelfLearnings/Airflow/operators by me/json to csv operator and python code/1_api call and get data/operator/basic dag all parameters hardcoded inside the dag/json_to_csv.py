import json 
import csv 
import requests
from jsonpath_ng import jsonpath, parse as jparse



from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook




# Opening JSON file and loading the data 
# into the variable data 
class JSONToCSVExportOperator(BaseOperator):

    def parse(api_url: str, output_csv: str, json_path: str = None, api_headers: dict = None,mapping: dict = None):

        request = requests.get(api_url,headers=api_headers)

        data = request.json()

        jsonpath_expression = jparse(json_path)

        match = jsonpath_expression.find(data)


        result = match[0].value
    


    # now we will open a file for writing 
        data_file = open(output_csv, 'w') 

    # create the csv writer object 
        csv_writer = csv.writer(data_file) 

        count = 0
    

        for values_in_result in result: 

            if count == 0:            

            # Writing headers of CSV file 
                header = mapping.keys()

                csv_writer.writerow(header)  
                count += 1
            
            row = []
        #csv_writer.writerow(mapping.values())
            for each_value in mapping.values(): 

                jsonpath_exp = jparse(each_value)
                matcher = jsonpath_exp.find(values_in_result)

                mapping_value = ''
                if len(matcher) > 0:
                    mapping_value = matcher[0].value

                row.append(mapping_value)
            csv_writer.writerow(row)

        data_file.close() 
        return;

    parse('https://run.mocky.io/v3/9091b5c4-064f-4249-9050-f780d817d2b5', '/home/prasadi/Desktop/path_to_test02.csv', '$.data','',{'FNAME' : '$.name','AGE' : '$.age','ADDRESS_L1' : '$.address.line1','ADDRESS_L2' : '$.address.line2','ZIP' : '$.address.code.number'})
   