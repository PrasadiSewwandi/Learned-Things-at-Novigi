import csv
import json
import requests

# Function to convert a CSV to JSON
# Takes the file paths as arguments
csvFilePath = 'gws.csv'
jsonFilePath = 'Names.json'

data ={}
with open(csvFilePath, encoding='utf-8') as csvf:
	csvReader = csv.DictReader(csvf)
	for rows in csvReader:
		id = rows['user_id']
		data[id] = rows
		
# function to dump data
with open(jsonFilePath, 'w') as jsonFile:
	jsonFile.write(json.dumps(data, indent=4))

url = 'http://localhost:3000/user/'
payload = open("Names.json")
headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
r = requests.post(url, data=payload, headers=headers)