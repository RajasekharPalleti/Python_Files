#Author: Rajasekhar Palleti
# Description: This script reads farmer_id and user_id from an Excel file,
#              Add the new user to the farmer's assignedTo list by removing existing one, and updates the farmer via a PUT API call.

import requests
import pandas as pd
import json
import time
from GetAuthtoken import get_access_token


def update_farmer_assigned_to(token, excel_path, sheet_name, output_excel,
                              BASE_URL_FARMER, BASE_URL_USER):

    headers = {"Authorization": f"Bearer {token}"}

    print("📂 Loading Excel data...")
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    # Ensure required columns exist
    if 'farmer_id' not in df.columns or 'user_id' not in df.columns:
        raise ValueError("❌ Excel must contain 'farmer_id' and 'user_id' columns")

    df['status'] = ""
    df['message'] = ""

    for index, row in df.iterrows():
        farmer_id = row['farmer_id']
        user_id = row['user_id']

        try:
            print(f"\n Processing Farmer ID: {farmer_id}, User ID: {user_id}")

            # Get Farmer DTO
            farmer_resp = requests.get(f"{BASE_URL_FARMER}/{farmer_id}", headers=headers)
            if farmer_resp.status_code != 200:
                raise Exception(f"Farmer GET failed ({farmer_resp.status_code}): {farmer_resp.text}")

            farmer_dto = farmer_resp.json()

            #Get User DTO
            user_resp = requests.get(f"{BASE_URL_USER}/{user_id}", headers=headers)
            if user_resp.status_code != 200:
                raise Exception(f"User GET failed ({user_resp.status_code}): {user_resp.text}")

            user_dto = user_resp.json()

            # Update assignedTo list
            farmer_dto["assignedTo"] = user_dto
            print(f" Assigned new user {user_dto["name"]} to {farmer_dto["firstName"]}")

            # Convert DTO to multipart payload
            multipart_data = {
                "dto": (None, json.dumps(farmer_dto), "application/json")
            }

            # PUT API call
            put_resp = requests.put(
                f"{BASE_URL_FARMER}",
                headers=headers,
                files=multipart_data
            )

            if put_resp.status_code in [200, 204]:
                print("✅ Farmer updated successfully.")
                df.at[index, 'status'] = "Success"
                df.at[index, 'message'] = "Farmer updated successfully"
            else:
                raise Exception(f"PUT failed ({put_resp.status_code}): {put_resp.text}")

        except Exception as e:
            print(f"❌ Error for Farmer ID {farmer_id}: {e}")
            df.at[index, 'status'] = "Failed"
            df.at[index, 'message'] = str(e)

        time.sleep(0.5)

    # Save output Excel
    df.to_excel(output_excel, index=False)
    print(f"\n📁 Process completed. Results saved to: {output_excel}")



if __name__ == "__main__":
    BASE_URL_FARMER = "https://cloud.cropin.in/services/farm/api/farmers"
    BASE_URL_USER = "https://cloud.cropin.in/services/user/api/users"

    INPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\Farmer_AssignedTo_Update.xlsx"
    SHEET_NAME = "Sheet1"
    OUTPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\Farmer_AssignedTo_Update_Output.xlsx"

    print("🌍 Fetching Auth_Token...")
    token = get_access_token("asp", "9649964096", "123456", "prod1")

    if token:
        update_farmer_assigned_to(token, INPUT_EXCEL, SHEET_NAME, OUTPUT_EXCEL,
                                  BASE_URL_FARMER, BASE_URL_USER)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")
