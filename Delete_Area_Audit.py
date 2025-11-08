# Author: Rajasekhar Palleti

import pandas as pd
import requests
import time
from GetAuthtoken import get_access_token


# Function to delete area audit one-by-one from Excel
def delete_area_audit(file_path, sheet_name, output_file, base_url, access_token, ca_column):
    # Read Excel (specific sheet)
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # Validate column
    if ca_column not in df.columns:
        print(f"Error: '{ca_column}' column is missing in the Excel file.")
        return

    # Prepare tracking columns
    if 'Status' not in df.columns:
        df['Status'] = ''
    if 'Response' not in df.columns:
        df['Response'] = ''

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    # Iterate one-by-one (no chunking)
    for i, row in df.iterrows():
        ca_id = row[ca_column]
        url = f"{base_url}{ca_id}/area-audit"

        print(f"Deleting area audit for CA ID: {ca_id} ...")
        try:
            resp = requests.delete(url, headers=headers, timeout=20)

            if resp.status_code in (200, 204):
                df.at[i, 'Status'] = 'Success'
                df.at[i, 'Response'] = resp.text or ''
            else:
                df.at[i, 'Status'] = f"Failed: {resp.status_code}"
                df.at[i, 'Response'] = resp.text or ''

        except Exception as e:
            df.at[i, 'Status'] = 'Error'
            df.at[i, 'Response'] = f"{type(e).__name__}: {e}"

        # polite pacing
        time.sleep(0.5)

    # Write output to a new file (do not overwrite input)
    df.to_excel(output_file, index=False)
    print(f"Processing complete. Results saved to {output_file}")


# ------------------------- CONSTANTS -------------------------
FILE_PATH = r"C:\Users\rajasekhar.palleti\Downloads\mdlz Area audit information\Tamil Nadu(1)_is_outside_india.xlsx"
SHEET_NAME = "Sheet2"
OUTPUT_FILE = r"C:\Users\rajasekhar.palleti\Downloads\mdlz Area audit information\Tamil Nadu(1)_is_outside_india_output.xlsx"

BASE_URL = "https://cloud.cropin.in/services/farm/api/croppable-areas/"
CA_COLUMN = "Croppable_area_id"


# -------------------------- EXECUTE --------------------------
token = get_access_token("asp", "9649964096", "123456", "prod1")

if token:
    print("token retrieved successfully")
    delete_area_audit(
        file_path=FILE_PATH,
        sheet_name=SHEET_NAME,
        output_file=OUTPUT_FILE,
        base_url=BASE_URL,
        access_token=token,
        ca_column=CA_COLUMN
    )
else:
    print("Failed to retrieve access token.")
