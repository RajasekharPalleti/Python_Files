# Author: Rajasekhar Palleti
# Script: Add or update seed grades to existing varieties
# The script will check if the seed grades exists; if not, it will create it and add it to the variety.

import json
import requests
import pandas as pd
import time
from GetAuthtoken import get_access_token

def fetch_seed_grade(token):
    url = seed_grade_url
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def create_seed_grade(token, name, description):
    url = seed_grade_url
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "name": name,
        "description": description
    }
    response = requests.post(url, headers=headers, json=payload)
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
    seed_grade = fetch_seed_grade(token)
    seed_grade_names = {seed['name'].lower(): seed for seed in seed_grade}

    for i, row in df.iterrows():
        try:
            variety_id = int(row.iloc[0]) if pd.notna(row.iloc[0]) else None
            seed_grade_name = row.iloc[1]
            description = row.iloc[2] if pd.notna(row.iloc[2]) else None

            if pd.isna(variety_id) or pd.isna(seed_grade_name):
                df.at[i, 'Status'] = "Skipped: Missing Variety ID or Seed Grade Name"
                df.at[i, 'Response'] = "Variety ID or Seed Grade Name is empty"
                print(f"\n⏳ Skipping the row {i+2} due to missing variety_id / Seed Grade name data.")
                continue

            print(f"\n⏳ Processing Variety ID: {variety_id}, Seed Grade: {seed_grade_name}")

            variety_response = requests.get(f"{variety_url}/{variety_id}", headers=headers)
            variety_response.raise_for_status()
            variety_data = variety_response.json()

            seed_grade_to_add = seed_grade_names.get(str(seed_grade_name).lower())

            if not seed_grade_to_add:
                print(f"⚠️ Seed Grade '{seed_grade_name}' does not exist. Creating...")
                seed_grade_to_add = create_seed_grade(token, seed_grade_name, description)
                seed_grade_names[str(seed_grade_name).lower()] = seed_grade_to_add
            else:
                print(f"✅ Seed Grade '{seed_grade_name}' already exists.")

            existing_stages = variety_data.get("seedGrades", [])
            if any(seed['name'].lower() == str(seed_grade_name).lower() for seed in existing_stages):
                print(f"⚠️ Seed Grade '{seed_grade_name}' already added to variety. Skipping update.")
                df.at[i, 'Status'] = "Skipped: Already Present"
                df.at[i, 'Response'] = json.dumps(variety_data)
                continue

            variety_data.setdefault("seedGrades", []).append(seed_grade_to_add)
            update_response = update_variety(token, variety_data)
            print(f"✅ Updated variety with new seed grade: {seed_grade_name}")
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
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\AddSeedGradesToVarieties.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\AddSeedGradesToVarieties_Output.xlsx"
    sheet_name = "Sheet1"
    seed_grade_url = "https://cloud.cropin.in/services/farm/api/seed-grades"
    variety_url = "https://cloud.cropin.in/services/farm/api/varieties"
    environment = "prod1"

    print("⏳ Retrieving access token...")
    token = get_access_token("asp", "9649964096", "123456", environment)

    if token:
        print("✅ Access token retrieved successfully")
        post_data_to_api(token, input_excel, output_excel, sheet_name)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")