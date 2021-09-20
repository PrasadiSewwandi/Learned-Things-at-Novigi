import json 
import csv 
import sys
import urllib
import urllib.request
import scrapeasy
from http.client import responses
from pprint import pprint
import urllib.error
from urllib.error import HTTPError
import requests
import requests.exceptions
from requests.exceptions import ConnectionError
from jsonpath_ng import jsonpath, parse as jparse
from django.http import HttpResponse



# Opening JSON file and loading the data 
# into the variable data 

def parse(api_url: str, output_csv: str, json_path: str = None, api_headers: dict = None):
      try:
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
          pprint(sys.path)

          for emp in result: 
	          if count == 0: 

		# Writing headers of CSV file 
		          header = emp.keys() 
		          csv_writer.writerow(header) 
		          count += 1

	# Writing data of CSV file 
	          csv_writer.writerow(emp.values()) 

          data_file.close() 
          return;
      except:
          if request.status_code == 404:
              print("Page Not Found")
          elif request.status_code == 403:
              print("Forbidden")   
          elif request.status_code == 400:
              print("Bad Request")   
          elif request.status_code == 401:
              print("Unotherized")  
          else:
              print("Response status is " + responses[request.status_code])  

                  
#parse('https://export-demo.vrbarea.com/v3_0/property.xml', '/home/prasadi/Desktop/path_to_test2.csv', '$.Meta.Children',{'authorization': "Basic QUYxOjgxNzI4My05MWM4YzgtNjQ1MmE2LWY4Yzc4Yy00NmNjNTU="})
#parse('https://jsonplaceholder.typicode.com/posts', '/home/prasadi/Desktop/path_to_test9.csv', '$')
parse('https://industryfundservice.sharepoint.com/sites/Unpaidsuper/_api/web/lists/getbytitle%28%27Quality%20Assurance%27%29/items', '/home/prasadi/Desktop/foxrat.csv', '$.d.results',{'authorization': "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Im5PbzNaRHJPRFhFSzFqS1doWHNsSFJfS1hFZyIsImtpZCI6Im5PbzNaRHJPRFhFSzFqS1doWHNsSFJfS1hFZyJ9.eyJhdWQiOiIwMDAwMDAwMy0wMDAwLTBmZjEtY2UwMC0wMDAwMDAwMDAwMDAvaW5kdXN0cnlmdW5kc2VydmljZS5zaGFyZXBvaW50LmNvbUBkZjRmMDMwZC0zODAzLTQ4ZDMtOTcwOC0zNTJkYzIzOTQwNWEiLCJpc3MiOiIwMDAwMDAwMS0wMDAwLTAwMDAtYzAwMC0wMDAwMDAwMDAwMDBAZGY0ZjAzMGQtMzgwMy00OGQzLTk3MDgtMzUyZGMyMzk0MDVhIiwiaWF0IjoxNjI3NTM5MDEzLCJuYmYiOjE2Mjc1MzkwMTMsImV4cCI6MTYyNzYyNTcxMywiaWRlbnRpdHlwcm92aWRlciI6IjAwMDAwMDAxLTAwMDAtMDAwMC1jMDAwLTAwMDAwMDAwMDAwMEBkZjRmMDMwZC0zODAzLTQ4ZDMtOTcwOC0zNTJkYzIzOTQwNWEiLCJuYW1laWQiOiJiN2U2MjVkMS01MmRkLTRhMGYtYmFjYy1mOTgxMWRmYzZlN2RAZGY0ZjAzMGQtMzgwMy00OGQzLTk3MDgtMzUyZGMyMzk0MDVhIiwib2lkIjoiNTc3MTBhMWEtNzA4Yi00ZWRlLTg1MzctYmU2Y2E5ZTk1Y2E5Iiwic3ViIjoiNTc3MTBhMWEtNzA4Yi00ZWRlLTg1MzctYmU2Y2E5ZTk1Y2E5IiwidHJ1c3RlZGZvcmRlbGVnYXRpb24iOiJmYWxzZSJ9.XT8fTgtYw2kqfLrLxznB5ikFH9ljRpDHXbZ6IGhe8Ia2YI8tvo2xY2R_pjAX-te2MpACErb5B_Qo11pzMKLnUu6O_MbH0hn-orkH-dAc1d_1WZck53pu-CWVmQxugcHd0ZLq6BiKKPT2YPKDkQGchjaet1ZSsfG7eU2WS8Cm3Q8Magn5SSBn5aY-G6I_N3GdAKiZV11UDSeMxXQH28DSCrSGNr-lDiKMtnCjGTFWg7EXLXEGXMwugRQJXjVvkR-IT8VILk5onzVnmgXI_VF2KStZHNsHkJ1WpKegL6cH2ERPkPMY4tFkGDoCC0JeWX7Tn0IcZUqmdkhJyMDck1Jp3w",'accept': 'application/json;odata=verbose' })
#parse('https://run.mocky.io/v3/08d77b33-4214-4636-bead-123b525b2e77', '/home/prasadi/Desktop/path_to_test3.csv', '$')
