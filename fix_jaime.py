import requests

# Register Jaime's account
reg = requests.post(
    'https://api.mdmcomics.com/api/auth/register',
    json={
        'name': 'Jaime Munoz',
        'email': 'munozdigitalmedia@gmail.com',
        'password': 'usbank!'
    }
)
print(f"Register: {reg.status_code} - {reg.text}")

# Promote to admin
promo = requests.post(
    'https://api.mdmcomics.com/api/users/promote-admin',
    json={'email': 'munozdigitalmedia@gmail.com'},
    headers={'X-Admin-Secret': '14ce49d36cf828e57349b1f9d81165e27e479df3df54cdba0005a75569a7ff89'}
)
print(f"Promote: {promo.status_code} - {promo.text}")
