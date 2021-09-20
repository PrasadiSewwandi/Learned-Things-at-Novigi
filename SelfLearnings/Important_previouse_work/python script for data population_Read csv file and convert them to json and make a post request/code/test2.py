
import requests

headers = {
    'Content-type': 'application/json',
}

data = '{"user_username": "test05","user_password": "6837","user_first_name": "test05","user_last_name": "McGowan","user_dob": "1900/01/01","user_user_status_id": 1,"user_user_type_id": 1}'

response = requests.post('http://localhost:3000/user/', headers=headers, data=data)