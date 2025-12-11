import requests

response = requests.post(
    'https://api.mdmcomics.com/api/users/promote-admin',
    json={'email': 'seth.robertson@gmail.com'},
    headers={'X-Admin-Secret': '14ce49d36cf828e57349b1f9d81165e27e479df3df54cdba0005a75569a7ff89'}
)
print(response.status_code)
print(response.text)
