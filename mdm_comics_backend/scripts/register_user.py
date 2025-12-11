import requests

response = requests.post(
    'https://api.mdmcomics.com/api/auth/register',
    json={
        'name': 'Seth Robertson',
        'email': 'seth.robertson@gmail.com',
        'password': 'usbank!'
    }
)
print(response.status_code)
print(response.json())
