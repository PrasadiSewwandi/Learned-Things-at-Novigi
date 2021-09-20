import json
import csv
import urllib
import urllib.request
from http.client import responses
import urllib.error
from urllib.error import HTTPError
import requests
import requests.exceptions
from requests.exceptions import ConnectionError
from jsonpath_ng import jsonpath, parse as jparse
from django.http import HttpResponse


# Opening JSON file and loading the data
# into the variable data


def parse(
    api_url: str, output_csv: str, json_path: str = None, api_headers: dict = None
):
    try:
        request = requests.get(api_url, headers=api_headers)
        data = request.json()

        jsonpath_expression = jparse(json_path)

        match = jsonpath_expression.find(data)

        result = match[0].value

        # now we will open a file for writing
        data_file = open(output_csv, "w")

        # create the csv writer object
        csv_writer = csv.writer(data_file)

        count = 0

        for emp in result:
            if count == 0:

                # Writing headers of CSV file
                header = emp.keys()
                res = list(emp.keys())[0]
                print("The first key of dictionary is : " + str(res))
                csv_writer.writerow(header)
                count += 1

            # Writing data of CSV file
            csv_writer.writerow(emp.values())

        data_file.close()
        return
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


# parse('https://export-demo.vrbarea.com/v3_0/property.xml', '/home/prasadi/Desktop/path_to_test2.csv', '$.Meta.Children',{'authorization': "Basic QUYxOjgxNzI4My05MWM4YzgtNjQ1MmE2LWY4Yzc4Yy00NmNjNTU="})
# parse('https://jsonplaceholder.typicode.com/posts', '/home/prasadi/Desktop/path_to_test9.csv', '$')
parse(
    "https://industryfundservice.sharepoint.com/sites/Unpaidsuper/_api/web/lists/getbytitle%28%27Quality%20Assurance%27%29/items",
    "/home/prasadi/Desktop/foxrat.csv",
    "$.d.results",
    {
        "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Im5PbzNaRHJPRFhFSzFqS1doWHNsSFJfS1hFZyIsImtpZCI6Im5PbzNaRHJPRFhFSzFqS1doWHNsSFJfS1hFZyJ9.eyJhdWQiOiIwMDAwMDAwMy0wMDAwLTBmZjEtY2UwMC0wMDAwMDAwMDAwMDAvaW5kdXN0cnlmdW5kc2VydmljZS5zaGFyZXBvaW50LmNvbUBkZjRmMDMwZC0zODAzLTQ4ZDMtOTcwOC0zNTJkYzIzOTQwNWEiLCJpc3MiOiIwMDAwMDAwMS0wMDAwLTAwMDAtYzAwMC0wMDAwMDAwMDAwMDBAZGY0ZjAzMGQtMzgwMy00OGQzLTk3MDgtMzUyZGMyMzk0MDVhIiwiaWF0IjoxNjI1NTAzMzk4LCJuYmYiOjE2MjU1MDMzOTgsImV4cCI6MTYyNTU5MDA5OCwiaWRlbnRpdHlwcm92aWRlciI6IjAwMDAwMDAxLTAwMDAtMDAwMC1jMDAwLTAwMDAwMDAwMDAwMEBkZjRmMDMwZC0zODAzLTQ4ZDMtOTcwOC0zNTJkYzIzOTQwNWEiLCJuYW1laWQiOiJiN2U2MjVkMS01MmRkLTRhMGYtYmFjYy1mOTgxMWRmYzZlN2RAZGY0ZjAzMGQtMzgwMy00OGQzLTk3MDgtMzUyZGMyMzk0MDVhIiwib2lkIjoiNTc3MTBhMWEtNzA4Yi00ZWRlLTg1MzctYmU2Y2E5ZTk1Y2E5Iiwic3ViIjoiNTc3MTBhMWEtNzA4Yi00ZWRlLTg1MzctYmU2Y2E5ZTk1Y2E5IiwidHJ1c3RlZGZvcmRlbGVnYXRpb24iOiJmYWxzZSJ9.IpCY-N1SjjSWLwiKyt8r2jSjcEf1KtN32RJaghyURtwDS_mzCDW0Qiauovs7IueHCt2-I32TQ045Nk6QagQ2ibcZDXKITGp0wGFBCrRX_RLj6UkHP9PbS5IPk2KohcFwEwretT-OpDC9EOi23JZ1xqFpsbKLj2z-mqMfrYyjSFQU9bBxDLnlQnNA04qsHTD93UadtEZCuD-ADk7mEtuXhxiySo7E-UvifC5g0KU0LF7uzMaYVBsOimQ7uQL2LswSRz6F5g0HbPqsnPxX67M_Sg_eWBaZWxpzKTiXYTlf-lv_YPHN80imPedifWDBHlnYjU7DWP98d_YGYjH7KO6lmQ",
        "accept": "application/json;odata=verbose",
    },
)
# parse('https://run.mocky.io/v3/08d77b33-4214-4636-bead-123b525b2e77', '/home/prasadi/Desktop/path_to_test3.csv', '$')
