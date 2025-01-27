from GetAuthtoken import get_access_token
auth_url = "https://sso.sg.cropin.in/auth/realms/asp/protocol/openid-connect/token"

# Call the function
token = get_access_token(auth_url, 9649964096, 123456)
print(token)