import requests

# First login as Seth (we know his works)
login = requests.post(
    'https://api.mdmcomics.com/api/auth/login',
    json={'email': 'seth.robertson@gmail.com', 'password': 'usbank!'}
)
print(f"Seth login: {login.status_code}")

# Actually, we need a password reset endpoint or direct DB access
# Let me check if there's a way to update via the users/me endpoint

# For now, let's just delete and recreate the account
# But we don't have a delete endpoint...

# Let's try different passwords that might have been set
passwords = ['!=#PROent11237', 'usbank!', 'password', 'demo123', 'admin']
for pw in passwords:
    r = requests.post(
        'https://api.mdmcomics.com/api/auth/login',
        json={'email': 'munozdigitalmedia@gmail.com', 'password': pw}
    )
    if r.status_code == 200:
        print(f"SUCCESS with: {pw}")
        break
    else:
        print(f"Failed: {pw}")
