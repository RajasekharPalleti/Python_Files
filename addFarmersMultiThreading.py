import pandas as pd
import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor
from GetAuthtoken import get_access_token


# Function to process a single row
def process_row(index, row, user_api_url, farmer_api_url, headers, df):
    print(f"🔄 Processing row {index + 1}...")

    # Extract user IDs and clean up data
    userIds = str(row.iloc[4]).strip()
    user_data_list = []

    if pd.notna(row.iloc[4]) and userIds:
        userIds = userIds.split(',')
    else:
        userIds = []

    user_api_failed = False
    farmer_api_failed = False
    response = None

    # Fetch user details if IDs exist
    if userIds:
        print("🌍 Fetching user details...")
        for user in userIds:
            user = user.strip()
            if user:
                try:
                    response = requests.get(f"{user_api_url}/{user}", headers=headers)
                    if response.status_code == 200:
                        user_data_list.append(response.json())
                    else:
                        print(f"⚠️ Failed to fetch user {user}: {response.status_code} - {response.text}")
                        user_api_failed = True
                except Exception as e:
                    print(f"❌ Error fetching user {user}: {str(e)}")
                    user_api_failed = True

    df.at[index, 'user_response'] = json.dumps(user_data_list)
    time.sleep(0.2)

    # Prepare payload for farmer API
    print("📦 Preparing farmer API payload...")
    farmer_payload = {
        "status": "DISABLE",
        "data": {
            "mobileNumber": row.iloc[3],
            "countryCode": f"+{str(row.iloc[2]).strip()}",
            "languagePreference": row.iloc[5],
            "farmeradditionl2": "A",
            "farmeradditionl4": "A",
            "farmeradditionl3": "A"
        },
        "images": {},
        "declaredArea": {
            "enableConversion": "true",
            "unit": "ACRE"
        },
        "firstName": row.iloc[0],
        "farmerCode": row.iloc[1],
        "assignedTo": user_data_list,
        "gender": "MALE",
        "address": {
            "country": row.iloc[6],
            "formattedAddress": row.iloc[7],
            "houseNo": None,
            "buildingName": None,
            "administrativeAreaLevel1": row.iloc[8],
            "locality": None,
            "administrativeAreaLevel2": None,
            "sublocalityLevel1": row.iloc[9],
            "sublocalityLevel2": None,
            "landmark": None,
            "postalCode": row.iloc[10],
            "placeId": None,
            "latitude": row.iloc[11],
            "longitude": row.iloc[12]
        },
        "isGDPRCompliant": "true"
    }

    # Converting farmer_payload to multipart dto
    multipart_data = {"dto": (None, json.dumps(farmer_payload), "application/json")}

    # Send POST request to farmer API
    print(f"🚀 Sending POST request to farmer API for {row.iloc[0]}...")
    try:
        response = requests.post(farmer_api_url, headers=headers, files=multipart_data)
        if response.status_code == 201:
            print(f"✅ Farmer created successfully: {row.iloc[0]}")
            df.at[index, 'Status'] = '✅ Success'
            df.at[index, 'farmer_response'] = response.text
        else:
            print(f"⚠️ Farmer creation failed: {response.status_code} - {response.text}")
            df.at[index, 'Status'] = f"❌ Failed: {response.status_code}"
            farmer_api_failed = True
    except Exception as e:
        print(f"❌ Error during farmer creation: {str(e)}")
        df.at[index, 'Status'] = "Error"
        farmer_api_failed = True

    # Store response only if an API call failed
    if user_api_failed or farmer_api_failed:
        df.at[index, 'Response'] = response.text if farmer_api_failed else "User API failed"

    # Simulating processing delay
    time.sleep(5)


# Function to process data with multi-threading
def post_data_to_api(user_api_url, farmer_api_url, token, input_excel, sheet_name, output_excel):
    print("📂 Loading input Excel file...")
    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    # Ensure necessary columns exist
    columns_to_check = ["Status", "Response", "user_response", "farmer_response"]
    for col in columns_to_check:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str)

    # Set headers for API requests
    headers = {'Authorization': f'Bearer {token}'}

    # Multi-threading execution with 4 threads
    index: int
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for index, row in df.iterrows():
            futures.append(executor.submit(process_row, index, row, user_api_url, farmer_api_url, headers, df))

        # Wait for all threads to complete
        for future in futures:
            future.result()

    # Save output to Excel file
    print("💾 Saving output Excel file...")
    df.to_excel(output_excel, index=False)
    print("✅ Process completed! Output saved.")


# Inputs and configurations
farmer_api_url = "https://cloud.cropin.in/services/farm/api/farmers"
user_api_url = "https://cloud.cropin.in/services/user/api/users"
input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\agraTenantsFarmerUploadTemplate.xlsx"
sheet_name = "Sheet1"
output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\agraTenantsFarmerUploadTemplateUpdated.xlsx"
tenant_code = "asp"

# Get authentication token
print("🌍 Fetching Auth_Token......")
token = get_access_token(tenant_code, "9649964096", "123456")

if token:
    post_data_to_api(user_api_url, farmer_api_url, token, input_excel, sheet_name, output_excel)
else:
    print("❌ Failed to retrieve access token. Process terminated.")
