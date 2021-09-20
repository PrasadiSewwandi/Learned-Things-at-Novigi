#Importing the libraries

import json 
import csv 
import requests
from jsonpath_ng import jsonpath, parse as jparse


def parse(api_url: str, req_type = "GET", output_csv = "", json_path: str = None, api_headers: dict = None, pay_load: dict = None, mapping: dict = None):

    # The get() method sends a GET request to the specified url with the api headers if it is required

    response = requests.request(req_type, api_url ,headers = api_headers,data = pay_load)

    # json() returns a JSON object of the result (if the result was written in JSON format, if not it raises an error).

    data = response.json()
    print(data)

    # JSONPath is an expression language to parse JSON data. It’s very similar to the XPath expression language to parse XML data.
    # The idea is to parse the JSON data and get the value we want. This is more memory efficient because, 
    # we don’t need to read the complete JSON data.
    # There are many JSONPath libraries in Python such as jsonpath/jsonpath-rw/jsonpath-ng
    # The jsonpath-ng module is the most comprehensive(covering or involving a large scope) and written purely in Python. It supports both Python 2 and Python 3.

    jsonpath_expression = jparse(json_path)

    # The result of JsonPath.find provide detailed context and path data so it is easy to traverse to parent objects,
    #  print full paths to pieces of data, and generate automatic ids.

    match = jsonpath_expression.find(data)
    # 'print(match)' will return follows
    """
    [DatumInContext(value=[{'name': 'Name 1', 'age': 12, 'address': {'line1': 'Line1', 'line2': 'Line2', 'code': {'number': '11580', 'zone': 'DE'}}},
    {'name': 'Name 2', 'age': 16, 'address': {'line1': 'Linex1', 'line2': 'Linex2', 'code': {'number': '18570', 'zone': 'BE'}}}, 
    {'name': 'Name 3', 'age': 52, 'address': {'line1': 'Liney1', 'line2': 'Liney2', 'code': {'number': '88595', 'zone': 'TG'}}},
    {'name': 'Name 4', 'age': 32, 'address': {'line1': 'Liney1', 'line2': 'Liney2'}}], path=Fields('data'), 
    context=DatumInContext(value={'page': 2, 'version': 23.5, 'data': [{'name': 'Name 1', 'age': 12, 
    'address': {'line1': 'Line1', 'line2': 'Line2', 'code': {'number': '11580', 'zone': 'DE'}}},
    {'name': 'Name 2', 'age': 16, 'address': {'line1': 'Linex1', 'line2': 'Linex2', 'code': {'number': '18570', 'zone': 'BE'}}}, {'name': 'Name 3', 'age': 52,
    'address': {'line1': 'Liney1', 'line2': 'Liney2', 'code': {'number': '88595', 'zone': 'TG'}}}, {'name': 'Name 4', 'age': 32, 'address': {'line1': 'Liney1', 'line2': 'Liney2'}}]}, path=Root(), context=None))]
    """
    """
    find actually returns a list of objects with contextual information.
    if we need to get the jason array object that we need to extrat from this list
    so we'll need to do something like:
    """
    result = match[0].value
    """
    print(result) will give the follows
    [{'name': 'Name 1', 'age': 12, 'address': {'line1': 'Line1', 'line2': 'Line2', 
    'code': {'number': '11580', 'zone': 'DE'}}}, {'name': 'Name 2', 'age': 16, 
    'address': {'line1': 'Linex1', 'line2': 'Linex2', 'code': {'number': '18570', 'zone': 'BE'}}},
    {'name': 'Name 3', 'age': 52, 'address': {'line1': 'Liney1', 'line2': 'Liney2',
    'code': {'number': '88595', 'zone': 'TG'}}}, {'name': 'Name 4', 'age': 32,
    'address': {'line1': 'Liney1', 'line2': 'Liney2'}}]
    """

    # now we will open a file for writing 
    data_file = open(output_csv, 'w') 

    # create the csv writer object 
    csv_writer = csv.writer(data_file) 

    header = mapping.keys()
    csv_writer.writerow(header)
    for values_in_result in result: 
        
        row = []
        #csv_writer.writerow(mapping.values())
        for each_value in mapping.values(): 

            jsonpath_exp = jparse(each_value)
            matcher = jsonpath_exp.find(values_in_result)
            # now we need to make a single row
            mapping_value = ''
            if len(matcher) > 0:
            # that means matcher includes values
                mapping_value = matcher[0].value

            row.append(mapping_value)
        csv_writer.writerow(row)

    data_file.close() 
    return;

parse('https://run.mocky.io/v3/9091b5c4-064f-4249-9050-f780d817d2b5', 'GET','/home/prasadi/Desktop/path_to_test11.csv', '$.data','','',{'FNAME' : '$.name','AGE' : '$.age','ADDRESS_L1' : '$.address.line1','ADDRESS_L2' : '$.address.line2','ZIP' : '$.address.code.number'})
