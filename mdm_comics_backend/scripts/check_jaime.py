import requests

response = requests.post(
    'https://api.mdmcomics.com/api/auth/login',
    json={'email': 'munozdigitalmedia@gmail.com', 'password': '!=#PROent11237'}
)
print(f"Login: {response.status_code}")
print(response.text)
