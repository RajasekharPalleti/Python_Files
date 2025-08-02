# Author: Rajasekhar Palleti
# Script: Add or update crop stages to existing varieties based on Excel input using pandas

import json
import requests
import pandas as pd
import time
from GetAuthtoken import get_access_token

def fetch_crop_stages(token):
    url = cropstage_url
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def create_crop_stage(token, name, description, days_after_sowing):
    url = cropstage_url
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "name": name,
        "description": description,
        "daysAfterSowing": days_after_sowing
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def update_variety(token, variety_id, variety_data):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = requests.put(f"{variety_url}", headers=headers, json=variety_data)
    response.raise_for_status()
    return response.json()

def post_data_to_api(token, input_excel, output_excel, sheet_name):
    df = pd.read_excel(input_excel, sheet_name=sheet_name)
    df['Status'] = ''
    df['Response'] = ''

    headers = {"Authorization": f"Bearer {token}"}
    crop_stages = fetch_crop_stages(token)
    crop_stage_names = {stage['name'].lower(): stage for stage in crop_stages}

    for i, row in df.iterrows():
        try:
            variety_id = int(row.iloc[0]) if pd.notna(row.iloc[0]) else None
            crop_stage_name = row.iloc[1]
            description = row.iloc[2] if pd.notna(row.iloc[2]) else None
            days_after_sowing = row.iloc[3] if pd.notna(row.iloc[3]) else None

            if pd.isna(variety_id) or pd.isna(crop_stage_name):
                df.at[i, 'Status'] = "Skipped: Missing Variety ID or Crop Stage Name"
                df.at[i, 'Response'] = "Variety ID or Crop Stage Name is empty"
                print(f"\n⏳ Skipping the row {i+2} due to missing variety_id / cropstage name data.")
                continue

            print(f"\n⏳ Processing Variety ID: {variety_id}, Crop Stage: {crop_stage_name}")

            variety_response = requests.get(f"{variety_url}/{variety_id}", headers=headers)
            variety_response.raise_for_status()
            variety_data = variety_response.json()

            crop_stage_to_add = crop_stage_names.get(str(crop_stage_name).lower())

            if not crop_stage_to_add:
                print(f"⚠️ Crop stage '{crop_stage_name}' does not exist. Creating...")
                crop_stage_to_add = create_crop_stage(token, crop_stage_name, description, days_after_sowing)
                crop_stage_names[str(crop_stage_name).lower()] = crop_stage_to_add
            else:
                print(f"✅ Crop stage '{crop_stage_name}' already exists.")

            existing_stages = variety_data.get("cropStages", [])
            if any(stage['name'].lower() == str(crop_stage_name).lower() for stage in existing_stages):
                print(f"⚠️ Crop stage '{crop_stage_name}' already added to variety. Skipping update.")
                df.at[i, 'Status'] = "Skipped: Already Present"
                df.at[i, 'Response'] = json.dumps(variety_data)
                continue

            variety_data.setdefault("cropStages", []).append(crop_stage_to_add)
            update_response = update_variety(token, variety_id, variety_data)
            print(f"✅ Updated variety with new crop stage: {crop_stage_name}")
            df.at[i, 'Status'] = "Success"
            df.at[i, 'Response'] = json.dumps(update_response)

        except Exception as e:
            print(f"❌ Failed to process row {i+2}: {str(e)}")
            df.at[i, 'Status'] = "Failed"
            df.at[i, 'Response'] = str(e)

        time.sleep(0.5)

    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\n✅ Processing complete. Output saved to {output_excel}")

if __name__ == "__main__":
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\AddCropStagesToVarieties.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\AddCropStagesToVarieties_Output.xlsx"
    sheet_name = "Sheet1"
    cropstage_url = "https://cloud.cropin.in/services/farm/api/crop-stages"
    variety_url = "https://cloud.cropin.in/services/farm/api/varieties"
    environment = "prod1"

    print("⏳ Retrieving access token...")
    token = get_access_token("asp", "9649964096", "123456", environment)

    if token:
        print("✅ Access token retrieved successfully")
        post_data_to_api(token, input_excel, output_excel, sheet_name)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")