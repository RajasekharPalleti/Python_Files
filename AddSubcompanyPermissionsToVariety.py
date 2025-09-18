# Author: Rajasekhar Palleti
# Script: Add sub company permission to existing varieties based on Excel input

import json
import requests
import pandas as pd
import time
from datetime import datetime
from GetAuthtoken import get_access_token

# Base URL (use correct environment)
VARIETY_URL = "https://cloud.cropin.in/services/farm/api/varieties"


def update_variety(token, variety_data):
    """Update variety with new data"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = requests.put(VARIETY_URL, headers=headers, json=variety_data)
    response.raise_for_status()
    return response.json()


def post_data_to_api(token, input_excel, output_excel, sheet_name):
    """Read Excel, update varieties with new subCompany permissions, save results"""
    df = pd.read_excel(input_excel, sheet_name=sheet_name)
    df['Status'] = ''
    df['Response'] = ''

    headers = {"Authorization": f"Bearer {token}"}

    for i, row in df.iterrows():
        try:
            variety_id = int(row['VarietyID'])
            subCompany_id = int(row['SubCompanyID'])

            # --- Fetch variety data ---
            try:
                variety_response = requests.get(f"{VARIETY_URL}/{variety_id}", headers=headers)
                variety_response.raise_for_status()
                variety_data = variety_response.json()
            except Exception as fetch_err:
                df.at[i, 'Status'] = "Failed"
                df.at[i, 'Response'] = f"Fetch error: {fetch_err}"
                print(f"❌ Failed to fetch variety {variety_id}: {fetch_err}")
                continue

            # --- Ensure subCompanyIds field is present ---
            if "subCompanyIds" not in variety_data or not isinstance(variety_data["subCompanyIds"], list):
                variety_data["subCompanyIds"] = []

            # --- Add new SubCompany ID if not already present ---
            if subCompany_id not in variety_data["subCompanyIds"]:
                variety_data["subCompanyIds"].append(subCompany_id)

                try:
                    update_response = update_variety(token, variety_data)
                    df.at[i, 'Status'] = "Success"
                    df.at[i, 'Response'] = json.dumps(update_response)
                    print(f"🚀 Variety ID {variety_id} updated successfully with SubCompany {subCompany_id}")
                except Exception as update_err:
                    df.at[i, 'Status'] = "Failed"
                    df.at[i, 'Response'] = f"Update error: {update_err}"
                    print(f"❌ Failed to update variety {variety_id}: {update_err}")
            else:
                df.at[i, 'Status'] = "Skipped"
                df.at[i, 'Response'] = f"SubCompany {subCompany_id} already exists"
                print(f"ℹ️ Variety ID {variety_id} already has SubCompany {subCompany_id}, skipped.")

        except Exception as e:
            df.at[i, 'Status'] = "Failed"
            df.at[i, 'Response'] = str(e)
            print(f"❌ Error processing row {i+2}: {e}")

        time.sleep(0.5)  # prevent hitting API too fast

    # --- Save output with timestamp for safety ---
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_excel.replace(".xlsx", f"_{timestamp}.xlsx")
    df.to_excel(output_file, sheet_name=sheet_name, index=False)
    print(f"\n✅ All processing complete. Output saved to {output_file}")


if __name__ == "__main__":
    input_excel = r"C:\Users\rajasekhar.palleti\Downloads\AddCropStagesToVarieties.xlsx"
    output_excel = r"C:\Users\rajasekhar.palleti\Downloads\AddCropStagesToVarieties_Output.xlsx"
    sheet_name = "Sheet1"
    environment = "prod1"

    print("🔐 Fetching access token...")
    token = get_access_token("sakataseeds", "2024202501", "Cropin12345", environment)

    if token:
        print("✅ Access token acquired. Starting process...")
        post_data_to_api(token, input_excel, output_excel, sheet_name)
    else:
        print("❌ Failed to fetch access token. Please verify credentials or environment.")
