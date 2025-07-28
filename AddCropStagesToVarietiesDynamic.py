# Author: Rajasekhar Palleti
# Script: Add crop stages to existing varieties based on Excel input

import json
import requests
import pandas as pd
import time
from GetAuthtoken import get_access_token

# URLs (use your actual environment's endpoints)
cropstage_url = "https://cloud.cropin.in/services/farm/api/crop-stages"
variety_url = "https://cloud.cropin.in/services/farm/api/varieties"

def fetch_crop_stages(token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(cropstage_url, headers=headers)
    response.raise_for_status()
    return response.json()

def update_variety(token, variety_data):
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
    crop_stages_master = fetch_crop_stages(token)
    crop_stage_map = {stage['name'].lower(): stage for stage in crop_stages_master}

    for i, row in df.iterrows():
        try:
            variety_id = int(row['VarietyID']) if pd.notna(row['VarietyID']) else None
            raw_stage_names = row['CropStageName']
            # description = row['Description'] if pd.notna(row['Description']) else None // Description is not used in this script
            days_after_sowing = int(row['daysAfterSowing']) if pd.notna(row['daysAfterSowing']) else None

            if pd.isna(variety_id) or pd.isna(raw_stage_names):
                df.at[i, 'Status'] = "Skipped: Missing Variety ID or Crop Stage Name"
                df.at[i, 'Response'] = "Variety ID or Crop Stage Name is empty"
                continue

            print(f"\n⏳ Processing Variety ID: {variety_id} with Crop Stage(s): {raw_stage_names}")

            # Fetch variety data
            variety_response = requests.get(f"{variety_url}/{variety_id}", headers=headers)
            variety_response.raise_for_status()
            variety_data = variety_response.json()

            # Parse and clean stage names
            # stage_names = [str(raw_stage_names).strip().split(',')] if stages are written as comma-separated
            stage_names = [str(raw_stage_names).strip()]

            if "cropStages" not in variety_data or not isinstance(variety_data["cropStages"], list):
                variety_data["cropStages"] = []

            existing_stage_names = {stage['name'].lower() for stage in variety_data['cropStages']}
            appended_stages = []

            for stage_name in stage_names:
                stage_key = stage_name.lower()
                stage_template = crop_stage_map.get(stage_key)

                if not stage_template:
                    print(f"⚠️ Crop stage '{stage_name}' not found in master. Skipping.")
                    continue

                if stage_key in existing_stage_names:
                    print(f"✔️ Crop stage '{stage_name}' already present. Skipping.")
                    continue

                # Clone and modify stage object
                stage_to_add = stage_template.copy()
                stage_to_add['daysAfterSowing'] = days_after_sowing
                appended_stages.append(stage_to_add)
                print(f"✅ Prepared stage '{stage_name}' with DAS {days_after_sowing} for update.")

            # Only update if we have something new
            if appended_stages:
                variety_data['cropStages'].extend(appended_stages)
                update_response = update_variety(token, variety_data)
                df.at[i, 'Status'] = "Success"
                df.at[i, 'Response'] = json.dumps(update_response)
                print(f"🚀 Variety ID {variety_id} updated successfully.")
            else:
                df.at[i, 'Status'] = "Skipped: No New Stages"
                df.at[i, 'Response'] = "No new stages added."

        except Exception as e:
            df.at[i, 'Status'] = "Failed"
            df.at[i, 'Response'] = str(e)
            print(f"❌ Error processing row {i+2}: {e}")

        time.sleep(0.5)

    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\n✅ All processing complete. Output saved to {output_excel}")

if __name__ == "__main__":
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\AddCropStagesToVarieties.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\AddCropStagesToVarieties_Output.xlsx"
    sheet_name = "Sheet1"
    environment = "prod1"

    print("🔐 Fetching access token...")
    token = get_access_token("sakataseeds", "2024202501", "Cropin12345", environment)

    if token:
        print("✅ Access token acquired. Starting process...")
        post_data_to_api(token, input_excel, output_excel, sheet_name)
    else:
        print("❌ Failed to fetch access token. Please verify credentials or environment.")
