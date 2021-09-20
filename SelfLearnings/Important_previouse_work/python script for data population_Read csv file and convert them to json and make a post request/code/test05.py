import csv
import json


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