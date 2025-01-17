import requests
import pandas as pd
import time

# Constants and configurations
EXCEL_FILE = 'C:\\Users\\rajasekhar.palleti\\Downloads\\farmer_data.xlsx'  # Path to your Excel file
GET_API_URL = 'https://cloud.cropin.in/services/farm/api/farmers/{farmer_id}'  # Replace with actual GET API endpoint
PUT_API_URL = 'https://cloud.cropin.in/services/farm/api/farmers/sync'  # Replace with actual PUT API endpoint
BEARER_TOKEN = 'eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI1LVQ4TTFrWm5PY2hIT0p5V3R2TE9sNlByNzFFVkgxWWxwOWc5blFxenVVIn0.eyJqdGkiOiI4YzkyMDYyMC03ZDA0LTRhMTgtYWU1NC0zNzljZWVkMTVmODEiLCJleHAiOjE3MzY5Mjc3NjgsIm5iZiI6MCwiaWF0IjoxNzM1NjMxNzY4LCJpc3MiOiJodHRwczovL3Nzby5zZy5jcm9waW4uaW4vYXV0aC9yZWFsbXMvcGVwc2ljb2luZGlhIiwiYXVkIjpbInJlc291cmNlX3NlcnZlciIsImFjY291bnQiXSwic3ViIjoiNTBmYWQ2MjYtODUwNy00Y2UzLWE1N2ItY2M1MDY1NGQ4OGY2IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoid2ViX2FwcCIsIm5vbmNlIjoiX2NsaWVudF93ZWJfYXBwIiwiYXV0aF90aW1lIjoxNzM1NjMxNzY4LCJzZXNzaW9uX3N0YXRlIjoiZTM5YWE5MGQtMDczOS00YjM1LWIwZjktZGMyNjM3MTVmNjU5IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsicmVzb3VyY2Vfc2VydmVyIjp7InJvbGVzIjpbIlJPTEVfQURNSU5fMTM1MSJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgbWljcm9wcm9maWxlLWp3dCBhZGRyZXNzIGVtYWlsIHBob25lIHByb2ZpbGUgb2ZmbGluZV9hY2Nlc3MiLCJ1c2VyX3JvbGUiOlsiUk9MRV9BRE1JTl8xMzUxIl0sInVwbiI6IjgwMDAxODAwMDIiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImFkZHJlc3MiOnt9LCJuYW1lIjoiUGVwc2ljby1JbmRpYSBBZG1pbiIsImdyb3VwcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl0sInByZWZlcnJlZF91c2VybmFtZSI6IjgwMDAxODAwMDIiLCJnaXZlbl9uYW1lIjoiUGVwc2ljby1JbmRpYSBBZG1pbiIsInRlbmFudCI6InBlcHNpY29pbmRpYSIsImVtYWlsIjoiaW5kaWFhZG1pbkBwZXBzaWNvLm9yZyJ9.x-nedV4jwRKAmX44B31DJ-YW2ZD9SW3GgEENPp-rqNHOEefm8YHCv02bs2_NlZhB3I32XKZ5TUI8Urd23LPJrc6XAYCD3hYP60gddbWMYiqyqw6XrkZLEB3sWs0ODM70I72-NObDG3DZWq5EjeBd0wYTFl-Gbv3N8UV5V17hw3bxZ0CnGKC6EbQaGGYZHuIlZOw2NBYl-HRMuZ0pUQIGN4AUk8D0V44B4129skjMRkAqmjBW_KIzluXLRO8cHNri7Xjd7ycb27zoJuOfqwK1xG2i1hpGvzIbfIT3TbuuzVIJ1Ez2hc_y1-NElOgBMVSiZ6cfCQkGJ55Uy7fsn0hoCQ'  # Replace with your Bearer token
WAIT_TIME = 0.4  # Time in seconds to wait between API hits
ITERATIONS = 222  # Total number of iterations

# Headers for API requests
HEADERS = {
    'Authorization': f'Bearer {BEARER_TOKEN}',
    'Content-Type': 'application/json',
}


def get_farmer_details(farmer_id):
    """Fetch farmer details using GET API."""
    url = GET_API_URL.format(farmer_id=farmer_id)
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch details for Farmer ID: {farmer_id}. Status Code: {response.status_code}")
        return None


def update_farmer_details(farmer_id, updated_data):
    """Send updated farmer details using PUT API."""
    url = PUT_API_URL.format(farmer_id=farmer_id)
    response = requests.put(url, headers=HEADERS, json=updated_data)
    if response.status_code in [200, 204]:
        print(f"Successfully updated Farmer ID: {farmer_id}")
        return True
    else:
        print(f"Failed to update Farmer ID: {farmer_id}. Status Code: {response.status_code}")
        return False


def main():
    # Load Excel data
    print("Reading Excel file...")
    try:
        data = pd.read_excel(EXCEL_FILE)
        if 'A' not in data.columns or 'C' not in data.columns:
            raise ValueError("Excel file must contain 'A' (farmer_id) and 'C' (new_firstName) columns")
        # Ensure column D for update status exists
        if 'D' not in data.columns:
            data['D'] = ''
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Iterate through rows up to the specified iteration limit
    for i in range(min(ITERATIONS, len(data))):
        farmer_id = data.at[i, 'A']
        new_first_name = data.at[i, 'C']

        print(f"Iteration {i + 1}: Processing Farmer ID: {farmer_id}")

        # Step 1: Get farmer details
        farmer_details = get_farmer_details(farmer_id)
        data['D'] = data['D'].astype(object)
        if not farmer_details:
            data.at[i, 'D'] = 'GET Failed'
            continue  # Skip to the next iteration if GET fails

        # Step 2: Check and modify the 'firstName' field
        current_first_name = farmer_details.get('firstName', '')
        if current_first_name != new_first_name:
            farmer_details['firstName'] = new_first_name
            # Step 3: Update farmer details via PUT API
            success = update_farmer_details(farmer_id, farmer_details)
            data.at[i, 'D'] = 'Updated' if success else 'PUT Failed'
        else:
            print(f"No update needed for Farmer ID: {farmer_id}")
            data.at[i, 'D'] = 'No Update Needed'

        # Step 4: Wait for 1 second before the next API call
        time.sleep(WAIT_TIME)

    # Save the updated Excel file
    print("Saving updated Excel file...")
    data.to_excel(EXCEL_FILE, index=False)
    print("Processing completed!")


if __name__ == "__main__":
    main()
