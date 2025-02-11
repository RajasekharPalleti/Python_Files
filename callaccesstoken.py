#Author : Rajasekhar Palleti

from GetAuthtoken import get_access_token

tenant_code = "campogto"

# Call the function
token = get_access_token(tenant_code, "5252525255", "cropin123")
print(token)