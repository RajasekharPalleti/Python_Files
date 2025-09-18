# Author: Rajasekhar Palleti
# Script: Update Asset Address from Lat-Long using Google Geocoding API

import requests
import json
import pandas as pd
import time
from GetAuthtoken import get_access_token

# === Config ===
GOOGLE_API_KEY = "AIzaSyAwy--7hbQ9x-_rFT2lCi52o0rF0JvbA7E"   # Replace with valid Google Maps API Key
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

file_path = r"C:\Users\rajasekhar.palleti\Downloads\Regional-plots-usa-california-freshno.xlsx"
sheet_name = "result"
api_url = "https://cloud.cropin.in/services/farm/api/assets"


# === Step 1: Get location details from lat/long ===
def get_location_details(lat, lng):
    params = {"latlng": f"{lat},{lng}", "key": GOOGLE_API_KEY}
    response = requests.get(GEOCODE_URL, params=params)
    data = response.json()

    if response.status_code != 200 or not data.get("results"):
        return {}

    result = data["results"][0]  # take first match
    address_components = result["address_components"]

    def get_component(types):
        for comp in address_components:
            if any(t in comp["types"] for t in types):
                return comp.get("long_name", "")
        return ""

    address = {
        "country": get_component(["country"]),
        "formattedAddress": result.get("formatted_address", ""),
        "administrativeAreaLevel1": get_component(["administrative_area_level_1"]),
        "locality": get_component(["locality"]),
        "administrativeAreaLevel2": get_component(["administrative_area_level_2"]),
        "sublocalityLevel1": get_component(["sublocality_level_1"]),
        "sublocalityLevel2": get_component(["sublocality_level_2"]),
        "landmark": "",       # Not directly available
        "postalCode": get_component(["postal_code"]),
        "houseNo": "",        # Not always available
        "buildingName": "",   # Not always available
        "placeId": result.get("place_id", ""),
        "latitude": lat,
        "longitude": lng
    }
    return {"address": address}


# === Step 2: Main function to process Excel & update API ===
def post_data_to_api(api_url, token, file_path, sheet_name):
    headers = {"Authorization": f"Bearer {token}"}
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    required_columns = ["latitude", "longitude", "status", "Response"]
    for col in required_columns:
        if col not in df.columns:
            df[col] = ""

    df = df.astype(str)

    for index, row in df.iterrows():
        try:
            asset_id = str(row.iloc[0]).strip()   # assuming first column is asset_id
            lat = row.get("latitude", "").strip()
            lng = row.get("longitude", "").strip()

            print(f"🔄 Processing row {index + 1}: asset_id = {asset_id}")

            if not asset_id or not lat or not lng:
                df.at[index, "status"] = "⚠️ Skipped - Missing asset_id/lat/lng"
                continue

            # === Step 2a: Get existing asset details ===
            get_response = requests.get(f"{api_url}/{asset_id}", headers=headers)
            get_response.raise_for_status()
            asset_data = get_response.json()

            # === Step 2b: Get address from lat/lng ===
            location = get_location_details(lat, lng)
            if not location:
                df.at[index, "status"] = "❌ Failed: No location details"
                continue

            # Update asset_data["address"]
            asset_data["address"] = location["address"]

            # === Step 2c: PUT API update ===
            multipart_data = {
                "dto": (None, json.dumps(asset_data), "application/json")
            }
            put_response = requests.put(api_url, headers=headers, files=multipart_data)
            put_response.raise_for_status()

            df.at[index, "status"] = "✅ Success"
            df.at[index, "Response"] = put_response.text
            print(f"✅ Address updated successfully for {asset_id}")

        except requests.exceptions.RequestException as e:
            df.at[index, "status"] = f"❌ Failed: {str(e)}"
            print(f"❌ Update failed for {asset_id}: {str(e)}")
        except Exception as e:
            df.at[index, "status"] = f"⚠️ Error: {str(e)}"
            print(f"⚠️ Unexpected error on row {index + 1}: {str(e)}")

        time.sleep(0.5)  # prevent API overload

    # === Step 3: Save results to Excel ===
    with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    print("📁 Results saved to Excel")


# === Entry Point ===
if __name__ == "__main__":
    overall_start_time = time.time()
    print("🔐 Retrieving access token...")
    token = get_access_token("walmart", "12345012345", "Cropin@123", "prod1")

    if token:
        print("✅ Token retrieved successfully.")
        post_data_to_api(api_url, token, file_path, sheet_name)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")
