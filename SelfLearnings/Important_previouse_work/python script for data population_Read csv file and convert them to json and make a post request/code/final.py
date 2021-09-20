#Requests package for python import requests
import csv
import json
import requests

#Parse CSV file and convert to JSON

with open('gws.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        out = json.dumps(row )
        print(out) 
        url = 'http://localhost:3000/user/'
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        response = requests.post(url,data=out,headers=headers )





