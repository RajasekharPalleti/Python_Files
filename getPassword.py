import password

tenant_code = "asp"
username = "9649964096"  # Example username

password = password.get_password(tenant_code, username)
print(f"Password is {password}")