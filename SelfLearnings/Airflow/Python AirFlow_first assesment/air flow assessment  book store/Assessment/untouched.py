#Importing the libraries

import json 
import csv 
import requests
import MySQLdb
from jsonpath_ng import jsonpath, parse as jparse


def parse(api_url: str, req_type = "GET", output_csv = "", json_path: str = None, api_headers: dict = None, pay_load: dict = None, mapping: dict = None):

    # The get() method sends a GET request to the specified url with the api headers if it is required
    mydb = MySQLdb.connect(host='localhost',
        user='root',
        passwd='root',
        db='bookStore')
    
    cursor = mydb.cursor()
    response = requests.request(req_type, api_url ,headers = api_headers,data = pay_load)

    # json() returns a JSON object of the result (if the result was written in JSON format, if not it raises an error).

    data = response.json()
    #print(data)

    # JSONPath is an expression language to parse JSON data. It’s very similar to the XPath expression language to parse XML data.
    # The idea is to parse the JSON data and get the value we want. This is more memory efficient because, 
    # we don’t need to read the complete JSON data.
    # There are many JSONPath libraries in Python such as jsonpath/jsonpath-rw/jsonpath-ng
    # The jsonpath-ng module is the most comprehensive(covering or involving a large scope) and written purely in Python. It supports both Python 2 and Python 3.

    jsonpath_expression = jparse(json_path)

    # The result of JsonPath.find provide detailed context and path data so it is easy to traverse to parent objects,
    #  print full paths to pieces of data, and generate automatic ids.

    match = jsonpath_expression.find(data)
    
    result = match[0].value
    

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
        print(row[2])
        try:
            cursor.execute('INSERT INTO book_assesmnt VALUES(%s, %s, %s,%s)',row)
        except Exception as e:
            print('Could not save', str(e))
    #close the connection to the database.
    mydb.commit()
    cursor.close()

    data_file.close() 

    return;

parse('https://api.itbook.store/1.0/new', 'GET','/home/prasadi/Desktop/path_to_test166.csv', '$.books','','',{'TITTLE' : '$.title','SUB_TITTLE' : '$.subtitle','ISBN' : '$.isbn13','PRICE' : '$.price'})
