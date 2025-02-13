#Author : Rajasekhar Palleti

from GetAuthtoken import get_access_token

tenant_code = "agratanzania"
environment="prod2"  # prod1 or prod2

# Call the function
token = get_access_token(tenant_code, "111222888", "agra123", environment=environment)
print(token)