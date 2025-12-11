import requests

# Try to login
response = requests.post(
    'https://api.mdmcomics.com/api/auth/login',
    json={'email': 'seth.robertson@gmail.com', 'password': 'usbank!'}
)
print(f"Login status: {response.status_code}")
print(response.text)
