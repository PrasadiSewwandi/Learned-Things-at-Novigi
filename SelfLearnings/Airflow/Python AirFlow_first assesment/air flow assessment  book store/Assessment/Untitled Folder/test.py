
import requests
import logging
import json
print('hi')


def get_access_token():
    logging.info('Retriving Auth token')
    access_token_url = "https://accounts.accesscontrol.windows.net/df4f030d-3803-48d3-9708-352dc239405a/tokens/OAuth/2"
    payload = {
    "grant_type": "client_credentials",
    "client_id": "b7e625d1-52dd-4a0f-bacc-f9811dfc6e7d@df4f030d-3803-48d3-9708-352dc239405a",
    "client_secret": "xWJMLFO4v4TBwSMzXnMCxupxwMSMKMj5A7wnnPh5ML0=",
    "resource": "00000003-0000-0ff1-ce00-000000000000/industryfundservice.sharepoint.com@df4f030d-3803-48d3-9708-352dc239405a"
    }
    headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/json"
    }
    access_token_response = requests.post(access_token_url, payload, headers=headers)
    print(access_token_response.content)

    if access_token_response.status_code == 200:
        logging.info('Retrieved Auth token')
       
        access_token = access_token_response.json()['access_token']
    else:
        print(access_token_response.json()['access_token'])
        logging.error('Error occurred when retrieving Zoho auth token.')
        access_token = ""
    logging.info("AT$%%&$%%$^$%$ "+ access_token)

get_access_token()