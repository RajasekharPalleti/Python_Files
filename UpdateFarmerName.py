import requests
import pandas as pd
import time
from GetAuthtoken import get_access_token

# Constants and configurations
EXCEL_FILE = 'C:\\Users\\rajasekhar.palleti\\Downloads\\farmer_data.xlsx'  # Path to your Excel file
GET_API_URL = 'https://cloud.cropin.in/services/farm/api/farmers/{farmer_id}'  # Replace with actual GET API endpoint
PUT_API_URL = 'https://cloud.cropin.in/services/farm/api/farmers/sync'  # Replace with actual PUT API endpoint
BEARER_TOKEN = get_access_token('your_tenant_code', 'your_username', 'your_password', 'prod1')  # Replace with actual credentials
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
