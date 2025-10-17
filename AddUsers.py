import pandas as pd
import requests
import json
import time
from GetAuthtoken import get_access_token


# Function to fetch location details from Google Maps API

def get_location_details(address, api_key):
    """Fetch structured location details for a given address using Google Maps API"""

    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": api_key}

    response = requests.get(base_url, params=params)
    data = response.json()

    if data["status"] != "OK":
        print(f"❌ Failed to get details for address: {address}")
        print(data)
        return None

    result = data["results"][0]
    geometry = result["geometry"]
    location = geometry["location"]
    bounds = geometry.get("bounds", geometry.get("viewport", {}))
    northeast = bounds.get("northeast", {})
    southwest = bounds.get("southwest", {})

    components = {comp["types"][0]: comp["long_name"] for comp in result["address_components"]}

    structured_data = {
        "bounds": {
            "northeast": {"lat": northeast.get("lat"), "lng": northeast.get("lng")},
            "southwest": {"lat": southwest.get("lat"), "lng": southwest.get("lng")}
        },
        "political": components.get("sublocality_level_1") or components.get("locality"),
        "country": components.get("country"),
        "administrativeAreaLevel3": components.get("administrative_area_level_3"),
        "administrativeAreaLevel2": components.get("administrative_area_level_2"),
        "administrativeAreaLevel1": components.get("administrative_area_level_1"),
        "placeId": result["place_id"],
        "latitude": location["lat"],
        "longitude": location["lng"],
        "geoInfo": {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [southwest.get("lng"), southwest.get("lat")],
                            [northeast.get("lng"), southwest.get("lat")],
                            [northeast.get("lng"), northeast.get("lat")],
                            [southwest.get("lng"), northeast.get("lat")],
                            [southwest.get("lng"), southwest.get("lat")]
                        ]]
                    }
                }
            ]
        },
        "name": address
    }

    return structured_data


# Function to process Excel and send user creation API requests

def post_data_to_api(user_api_url, token, excel_sheet, sheet_name, google_api_key):
    print("📂 Loading input Excel file...")
    df = pd.read_excel(excel_sheet, sheet_name=sheet_name)

    def get_value(cell):
        """Returns None if the cell is empty or NaN, otherwise returns the string value."""
        return None if pd.isna(cell) or str(cell).strip() == "" else cell

    columns_to_check = ["Status", "Response", "User_response"]
    for col in columns_to_check:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str)

    # Loop through all rows
    for index, row in df.iterrows():
        print(f"🔄 Processing row {index + 1}...")

        companyId = 1251
        user_name = get_value(row.iloc[0])

        # Extract manager IDs
        managerIds = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
        managerIds = [mid.strip() for mid in managerIds.split(",") if mid.strip()] if managerIds else []

        contactNumber = row.iloc[2].strip()
        userRoleId = row.iloc[3].strip()
        email = row.iloc[4].strip()
        countryCode = row.iloc[5].strip()
        location_name = row.iloc[6].strip()
        timeZone = row.iloc[7].strip()
        language = row.iloc[8].strip()
        currency = row.iloc[9].strip()
        areaUnits = row.iloc[10].strip()
        locale = row.iloc[11].strip()

        if not user_name:
            print(f"⚠️ Row {index + 1} skipped due to invalid user name.")
            df.at[index, 'Response'] = "Invalid user name — creation skipped"
            df.at[index, 'Status'] = "⚠️ Skipped"
            continue

        if not managerIds:
            print(f"⚠️ Row {index + 1} skipped due to empty manager list.")
            df.at[index, 'Response'] = "Skipped due to empty manager list"
            df.at[index, 'Status'] = "⚠️ Skipped"
            continue

        # Auto-fetch location details using Google Maps API
        print(f" Fetching location details for: {location_name}")
        location_details = get_location_details(location_name, google_api_key)

        if not location_details:
            print(f"❌ Skipping user creation due to missing location details for {location_name}.")
            df.at[index, 'Status'] = "❌ Location fetch failed"
            df.at[index, 'Response'] = "Location details not found"
            continue

        # Prepare payload
        print("📦 Preparing user API payload...")
        user_payload = {
            "companyId": companyId,
            "data": {},
            "images": {},
            "contactNumber": contactNumber,
            "name": user_name,
            "userRoleId": userRoleId,
            "countryCode": countryCode,
            "email": email,
            "locations": location_details,
            "managers": [managerIds],
            "assignedTo": None,
            "preferences": {
                "data": {},
                "timeZone": timeZone,
                "language": language,
                "currency": currency,
                "areaUnits": areaUnits,
                "locale": locale
            }
        }

        headers = {'Authorization': f'Bearer {token}'}
        multipart_data = {"dto": (None, json.dumps(user_payload), "application/json")}

        # Send request
        print(f"🚀 Sending POST request to user API for {user_name}...")
        try:
            response = requests.post(user_api_url, headers=headers, files=multipart_data)
            if response.status_code == 201:
                print(f"✅ User created successfully: {user_name}")
                df.at[index, 'Status'] = '✅ Success'
                df.at[index, 'User_response'] = response.text
            else:
                print(f"⚠️ User creation failed: {response.status_code} - {response.text}")
                df.at[index, 'Status'] = f"❌ Failed: {response.status_code}"
                df.at[index, 'Response'] = response.text
        except Exception as e:
            print(f"❌ Error during user creation: {str(e)}")
            df.at[index, 'Status'] = "❌ Error"
            df.at[index, 'Response'] = str(e)

        # Wait before next iteration
        time.sleep(1)

    # ✅ Save output Excel safely
    def save_output_file(df, excel_sheet, attempt=1):
        try:
            df.to_excel(excel_sheet, index=False)
            print(f"💾 Output file saved successfully (attempt {attempt})")
        except Exception as err:
            if attempt < 3:
                print(f"⚠️ Error saving output file: {err}. Retrying in 30 seconds...")
                time.sleep(30)
                save_output_file(df, excel_sheet, attempt + 1)
            else:
                print(f"❌ Failed to save output file after 3 attempts. Please close the file.")

    print("💾 Saving Excel file...")
    save_output_file(df, excel_sheet)
    print("✅ Process completed! Output saved successfully.")


# --- MAIN EXECUTION ---
user_api_url = "https://cloud.cropin.in/services/user/api/users/images"
excel_sheet = r"C:\Users\rajasekhar.palleti\Downloads\agraTenantsFarmerUploadTemplate.xlsx"
sheet_name = "Sheet1"
tenant_code = "asp"
environment = "prod1"
google_api_key = "c9ef0d0e71695c18b4b4762b035d512b"  # 🔑 Replace this with your valid key

print("🌍 Fetching Auth_Token...")
token = get_access_token(tenant_code, "9649964096", "123456", environment)

if token:
    post_data_to_api(user_api_url, token, excel_sheet, sheet_name, google_api_key)
else:
    print("❌ Failed to retrieve access token. Process terminated.")
