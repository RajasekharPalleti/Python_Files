from GetAuthtoken import get_access_token
tenant_code = "asp"

# Call the function
token = get_access_token(tenant_code, 9649964096, 123456)
print(token)