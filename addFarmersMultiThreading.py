# Author: Rajasekhar Palleti
# QA
import os

import pandas as pd
import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor
from GetAuthtoken import get_access_token


# Function to process a single row
def process_row(index, row, user_api_url, farmer_api_url, headers, df):
    print(f"🔄 Processing row {index + 1}...")

    def get_value(cell):
        """Returns None if the cell is empty or NaN, otherwise returns the string value."""
        return None if pd.isna(cell) or str(cell).strip() == "" else cell

    # Extract firstName and validate
    first_name = get_value(row.iloc[0])
    if not first_name:
        print(f"⚠️ Row {index + 1} skipped due to invalid farmer name.")
        df.at[index, 'Response'] = "invalid farmer name and creation is skipped"
        df.at[index, 'Status'] = "⚠️ Skipped"
        return

    # # Extract user IDs and clean up data
    # userIds = str(row.iloc[4]).strip()
    # user_data_list = []
    user_data_list = [
        {
            "id": 1401,
            "name": "Tanzania Admin",
            "data": {
                "environment": "prod2",
                "isSREnabled": True
            },
            "assignedManagersName": [],
            "managers": [],
            "userStatus": None,
            "location": None,
            "contactNumber": "111222888",
            "email": "tanzaniaadmin0@agra.org",
            "preferences": {
                "id": 1451,
                "currency": "TANZANIAN_SHILLING",
                "language": "en",
                "timeZone": "EST",
                "areaUnits": "HECTARE",
                "locale": None,
                "data": {},
                "locations": None
            },
            "address": None,
            "assignedTo": None,
            "managersName": None,
            "companyId": 1251,
            "parentCompanyId": None,
            "userRoleId": 1351,
            "userRoleName": "ROLE_ADMIN",
            "correspondingKcId": None,
            "resources": None,
            "countryCode": "+243",
            "companyStatus": None,
            "clientId": "6e3fbd4c-a075-4c6d-acdc-52407aac1df9",
            "images": None,
            "companyPreferences": None,
            "locations": {
                "id": 1501,
                "name": "Tanzania",
                "administrativeAreaLevel5": None,
                "administrativeAreaLevel4": None,
                "administrativeAreaLevel3": None,
                "administrativeAreaLevel2": None,
                "administrativeAreaLevel1": None,
                "country": "Tanzania",
                "latitude": None,
                "longitude": None,
                "sublocalityLevel1": None,
                "sublocalityLevel2": None,
                "sublocalityLevel3": None,
                "sublocalityLevel4": None,
                "sublocalityLevel5": None,
                "geoInfo": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                    [
                                        [
                                            29.33999997140636,
                                            -11.76125392757591
                                        ],
                                        [
                                            40.63980004969153,
                                            -11.76125392757591
                                        ],
                                        [
                                            40.63980004969153,
                                            -0.9843968413481883
                                        ],
                                        [
                                            29.33999997140636,
                                            -0.9843968413481883
                                        ],
                                        [
                                            29.33999997140636,
                                            -11.76125392757591
                                        ]
                                    ]
                                ]
                            },
                            "properties": {}
                        }
                    ]
                },
                "bounds": {
                    "southwest": {
                        "lat": -11.76125392757591,
                        "lng": 29.33999997140636
                    },
                    "northeast": {
                        "lat": -0.9843968413481883,
                        "lng": 40.63980004969153
                    }
                },
                "placeId": "ChIJEaFpSDFRSxgRMMTBSzEXWog"
            },
            "enabled": True,
            "isAcreSquareUser": False,
            "isGDPRCompliant": False,
            "loginType": None,
            "preferredName": "111222888",
            "deviceToken": None,
            "deviceType": None,
            "createdBy": "SYSTEM",
            "lastModifiedBy": "1401",
            "deleted": False
        }
    ]
    #
    # if pd.notna(row.iloc[4]) and userIds:
    #     userIds = userIds.split(',')
    # else:
    #     userIds = []
    #
    # # Skip row execution if user list is empty
    # if not userIds:
    #     print(f"⚠️ Row {index + 1} skipped due to empty user list.")
    #     df.at[index, 'Response'] = "Skipped due to empty user list"
    #     df.at[index, 'Status'] = "⚠️ Skipped"
    #     return
    #
    user_api_failed = False
    farmer_api_failed = False
    response = None

    # # Fetch user details if IDs exist
    # if userIds:
    #     print("🌍 Fetching user details...")
    #     for user in userIds:
    #         user = user.strip()
    #         if user:
    #             try:
    #                 response = requests.get(f"{user_api_url}/{user}", headers=headers)
    #                 if response.status_code == 200:
    #                     user_data_list.append(response.json())
    #                 else:
    #                     print(f"⚠️ Failed to fetch user {user}: {response.status_code} - {response.text}")
    #                     user_api_failed = True
    #             except Exception as e:
    #                 print(f"❌ Error fetching user {user}: {str(e)}")
    #                 user_api_failed = True
    #
    # time.sleep(0.2)

    # Prepare payload for farmer API
    print("📦 Preparing farmer API payload...")
    farmer_payload = {
        "status": "DISABLE",
        "data": {
            "mobileNumber": get_value(row.iloc[3]),
            "countryCode": f"+{str(row.iloc[2]).strip()}",
            "languagePreference": get_value(row.iloc[5]),
            # "farmeradditionl3": "A",
            # "farmeradditionl4": "A",
            # "farmeradditionl2": "A",
            "registrationDate": get_value(row.iloc[16]),
            "gdprConsent": get_value(row.iloc[17]),
            "ageRange": get_value(row.iloc[15])
        },
        "images": {},
        "declaredArea": {
            "enableConversion": "True",
            "unit": "HECTARE"
        },
        "firstName": first_name,
        "farmerCode": get_value(row.iloc[1]),
        "assignedTo": user_data_list,
        "gender": get_value(row.iloc[14]).upper() if get_value(row.iloc[14]) else None,
        "address": {
            "country": get_value(row.iloc[6]),
            "formattedAddress": get_value(row.iloc[7]),
            "houseNo": None,
            "buildingName": None,
            "administrativeAreaLevel1": get_value(row.iloc[8]),
            "locality": None,
            "administrativeAreaLevel2": get_value(row.iloc[9]),
            "sublocalityLevel1": get_value(row.iloc[10]),
            "sublocalityLevel2": None,
            "landmark": None,
            "postalCode": get_value(row.iloc[11]),
            "placeId": "ChIJ6Yuupv8VphkRB5evs7ThIW0",
            "latitude": get_value(row.iloc[12]),
            "longitude": get_value(row.iloc[13])
        },
        # "isGDPRCompliant": "True"
    }

    # Converting farmer_payload to multipart dto
    multipart_data = {"dto": (None, json.dumps(farmer_payload, default=str), "application/json")}

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
            df.at[index, 'Status'] = f"⚠️ Failed: {response.status_code}"
            farmer_api_failed = True
    except Exception as e:
        print(f"❌ Error during farmer creation: {str(e)}")
        df.at[index, 'Status'] = "❌ Error"
        farmer_api_failed = True

    # Store response only if an API call failed
    if user_api_failed or farmer_api_failed:
        df.at[index, 'Response'] = response.text if farmer_api_failed and response is not None else "User API failed"

    # Simulating processing delay
    time.sleep(0.5)


# Function to process data with multi-threading
def post_data_to_api(user_api_url, farmer_api_url, token, excel_path, sheet_name):
    print("📂 Loading input Excel file...")
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    # Ensure necessary columns exist
    columns_to_check = ["Status", "Response", "farmer_response"]
    for col in ["Status", "Response", "farmer_response"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str)

    # Set headers for API requests
    headers = {'Authorization': f'Bearer {token}'}

    # Function to save the Excel file safely without disturbing other sheets
    def save_output_file(df, excel_path, sheet_name, attempt=1):
        try:
            time.sleep(10)  # Wait for 10 seconds before saving

            # Check if the file exists
            if os.path.exists(excel_path):
                # Load existing sheets to preserve data
                with pd.ExcelWriter(excel_path, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                # If file does not exist, create a new one
                with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"💾 Output file saved successfully at attempt {attempt}")

        except Exception as e:
            if attempt < 3:  # Retry up to 3 times
                print(
                    f"⚠️ Error saving output file: Please close any open instances of the file. Retrying in 30 seconds... Error: {e}")
                time.sleep(30)
                save_output_file(df, excel_path, sheet_name, attempt + 1)
            else:
                print(f"❌ Failed to save output file after 3 attempts. Please close any open instances of the file.")

    # Multi-threading execution with 4 threads
    index: int
    with ThreadPoolExecutor(max_workers=1) as executor:  # Use 4 threads for better performance
        futures = []
        for index, row in df.iloc[1000:].iterrows():  # Provide df.iloc[start:end] to control row range
            futures.append(executor.submit(process_row, index, row, user_api_url, farmer_api_url, headers, df))

        # Wait for all threads to complete
        for future in futures:
            future.result()

    # Final save after completion
    print("💾 Final save of output Excel file...")
    save_output_file(df, excel_path, sheet_name)  # Replace with the actual sheet name
    print("✅ Process completed! Output saved.")


# Inputs and configurations
farmer_api_url = "https://sf-africa-service.cropin.in/prod2/services/farm/api/farmers"
user_api_url = "https://sf-africa-service.cropin.in/prod2/services/user/api/users"
# farmer_api_url = "https://cloud.cropin.in/services/farm/api/farmers"
# user_api_url = "https://cloud.cropin.in/services/user/api/users"
excel_path = "C:\\Users\\rajasekhar.palleti\\Downloads\\AGRA_Tanzania_2.xlsx"
sheet_name = "Sheet3"
tenant_code = "agratanzania"
environment = "prod2" #prod1 or prod2

# Get authentication token
print("🌍 Fetching Auth_Token......")
token = get_access_token(tenant_code,"111222888","agra123",environment)

if token:
    print("🌍 Fetching Auth_Token Success proceeding with execution")
    post_data_to_api(user_api_url, farmer_api_url, token, excel_path, sheet_name)
else:
    print("❌ Failed to retrieve access token. Process terminated.")