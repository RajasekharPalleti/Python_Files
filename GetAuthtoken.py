import requests

# Define constants for fixed values
GRANT_TYPE = "password"
CLIENT_ID = "resource_server"  # Replace with your actual client ID
CLIENT_SECRET = "resource_server"  # Replace with your actual client secret


def get_access_token(auth_url, username, password):
    """Fetch the access token using the provided URL, username, and password."""
    try:
        # Define the payload for the POST request
        payload = {
            "username": username,
            "password": password,
            "grant_type": GRANT_TYPE,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }

        # Send the POST request with x-www-form-urlencoded data
        response = requests.post(auth_url, data=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse and return the access token
        return response.json().get("access_token")
    except Exception as e:
        print(f"Failed to retrieve access token: {e}")
        return None
