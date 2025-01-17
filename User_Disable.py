import requests
import pandas as pd
import time

#Inputs

EXCEL_FILE = 'C:\\Users\\rajasekhar.palleti\\Downloads\\ADM_new_list.xlsx'  # Path to your Excel file
PUT_API_URL = 'https://cloud.cropin.in/services/user/api/users/enable/{id}?enableFlag=false'  # Replace with actual PUT API endpoint
BEARER_TOKEN = 'eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJSNHhEV0RxcERJMi1oNzFPZ3VxUkhNYmROSkM5b2lVTHFGelFfOVJCZ29VIn0.eyJqdGkiOiI2N2ZhMjQ3Yi1mOGVmLTQyYzUtYThkOC1mM2YzN2NlOTg4YjAiLCJleHAiOjE3NjYwNzQ2NzcsIm5iZiI6MCwiaWF0IjoxNzM0NTM4Njc3LCJpc3MiOiJodHRwczovL3Nzby5zZy5jcm9waW4uaW4vYXV0aC9yZWFsbXMvYWRtIiwiYXVkIjpbInJlc291cmNlX3NlcnZlciIsImFjY291bnQiXSwic3ViIjoiNGI1MWVjYzgtOWI5NS00MjBjLWE4ZTQtYzVhZWVhMDVhODk5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoid2ViX2FwcCIsIm5vbmNlIjoiX2NsaWVudF93ZWJfYXBwIiwiYXV0aF90aW1lIjoxNzM0NTM4Njc3LCJzZXNzaW9uX3N0YXRlIjoiZjY1NzQzMDYtMTcxNS00ODVjLTgzYzctYTViZTMxODgyODZmIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsicmVzb3VyY2Vfc2VydmVyIjp7InJvbGVzIjpbIkZBUk1FUl9BQ1JFU1FVQVJFXzQ4MjAxIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBlbWFpbCBtaWNyb3Byb2ZpbGUtand0IHByb2ZpbGUgb2ZmbGluZV9hY2Nlc3MgcGhvbmUgYWRkcmVzcyIsInVzZXJfcm9sZSI6WyJGQVJNRVJfQUNSRVNRVUFSRV80ODIwMSJdLCJ1cG4iOiI5NjIzODA5NDk1IiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJhZGRyZXNzIjp7fSwiZ3JvdXBzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXSwicHJlZmVycmVkX3VzZXJuYW1lIjoiOTYyMzgwOTQ5NSIsInRlbmFudCI6ImFkbSJ9.OfVhQX4Nb28de-znTtr4f7I6kuTRZZOIR4K0UozmxnaKIiZfHsqKGwhKI-nlh2iz69BlH9sDxTphKZfopneDH7Lu-5KQ-BHeb-d7bPjTcUhOHckGawSQaxwEvRt3Ile01i-pSKmPE1hjlIZBmIi1w2QmMklYuoDHJaZH07H_iFYh0Nho9JnHt7X1HJcKwvrTHptUHxoPazKxn8ZQOeq5DTMG-0KW1d9cTqVAKJt9L5a-p_aCPobZebMjdHVtYUwU1Ag_jyErglBrzIWG61HZ1UCjiT5BJ6FXfK0IyOD5ulSeNCx4cqyvT4DNVXYBH78bqHXKo9_ZrgdCLE7TGbv19g'  # Replace with your Bearer token
WAIT_TIME = 0.01  # Time in seconds to wait between API hits

# Headers for API requests
HEADERS = {
    'Authorization': f'Bearer {BEARER_TOKEN}',
    'Content-Type': 'application/json',
}

def disable_user(user_id):
    """Send a PUT request to disable a single user."""
    url = PUT_API_URL.format(id=user_id)

    response = requests.put(url, headers=HEADERS)
    if response.status_code in [200, 204]:
        print(f"Successfully disabled user ID: {user_id}")
        return 'Success'
    else:
        print(f"Failed to disable user ID: {user_id}. Status Code: {response.status_code}")
        return 'Failed'

def main():
    # Load Excel data
    print("Reading Excel file...")
    try:
        data = pd.read_excel(EXCEL_FILE)
        if 'ids' not in data.columns:
            raise ValueError("Excel file must contain a column named 'ids'")
        # Check 'status' column exists
        if 'status' not in data.columns:
            data['status'] = ''
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    ids = data['ids'].tolist()

    # Process IDs individually
    for i, user_id in enumerate(ids):
        print(f"Disabling user ID: {user_id}")

        # Disable user via PUT API
        status = disable_user(user_id)
        data.at[i, 'status'] = status

        # Wait for 0.001 second before the next API call
        time.sleep(WAIT_TIME)

    # Save the updated Excel file
    print("Saving updated Excel file...")
    data.to_excel(EXCEL_FILE, index=False)
    print("All users have been processed!")

if __name__ == "__main__":
    main()
