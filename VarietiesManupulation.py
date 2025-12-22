# Author: Rajasekhar Palleti
# Script: Add or update crop stages to existing varieties
# The script will check if the crop stage exists; if not, it will create it and add it to the variety.

import json
import requests
import pandas as pd
import time
from GetAuthtoken import get_access_token


def post_data_to_api(token, input_excel, output_excel, sheet_name):
    df = pd.read_excel(input_excel, sheet_name=sheet_name)
    df['Status'] = ''
    df['Response'] = ''

    headers = {"Authorization": f"Bearer {token}"}

    for i, row in df.iterrows():
        try:
            variety_id = int(row.iloc[0]) if pd.notna(row.iloc[0]) else None

            variety_response = requests.get(f"{variety_url}/{variety_id}", headers=headers)
            variety_data = variety_response.json()
            variety_data['cropStages'] = []

            requests.put(f"{variety_url}", headers=headers, json=variety_data)

        except Exception as e:
            print(f"❌ Failed to process row {i+2}: {str(e)}")
            df.at[i, 'Status'] = "Failed"
            df.at[i, 'Response'] = str(e)

        time.sleep(0.5)

    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\n✅ Processing complete. Output saved to {output_excel}")


if __name__ == "__main__":
    input_excel = r"C:\Users\rajasekhar.palleti\Downloads\Variety_IDS_Elder.xlsx"
    output_excel = r"C:\Users\rajasekhar.palleti\Downloads\Variety_IDS_Elder_output.xlsx"
    sheet_name = "Sheet1"
    cropstage_url = "https://cloud.cropin.in/services/farm/api/crop-stages"
    variety_url = "https://cloud.cropin.in/services/farm/api/varieties"
    environment = "prod1"

    print("⏳ Retrieving access token...")
    token = get_access_token("eld", "429860737", "Elders@123", environment)

    if token:
        print("✅ Access token retrieved successfully")
        post_data_to_api(token, input_excel, output_excel, sheet_name)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")