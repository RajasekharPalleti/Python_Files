# Author: Rajasekhar Palleti

import json
import requests
import pandas as pd
import time
from datetime import datetime
from GetAuthtoken import get_access_token


def post_data_to_api(api_url, token, input_excel, output_excel, sheet_name):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    if "Status" not in df.columns:
        df["Status"] = ""
    if "CA_Response" not in df.columns:
        df["CA_Response"] = ""

    # for index, row in df.iloc[1:4].iterrows():
    for index, row in df.iterrows():
        CA_id = row.iloc[0]  # Column A
        variety_id = row.iloc[5]  # Column F
        raw_sowing_date = row.iloc[8]  # Column I

        # Handle if it's already a datetime or string
        if isinstance(raw_sowing_date, pd.Timestamp):  # Excel datetime type
            sowingDate = raw_sowing_date.strftime("%Y-%m-%dT%H:%M:%S.000+0000")
        else:
            # If string like "2024-11-19T00:00:00" or "2024-11-19T08:00:00" it will take only date part
            # and convert to the required format
            sowingDate = datetime.strptime(str(raw_sowing_date).split("T")[0], "%Y-%m-%d").strftime(
                "%Y-%m-%dT%H:%M:%S.000+0000"
            )


        if pd.isna(CA_id) or pd.isna(variety_id) or pd.isna(raw_sowing_date):
            df.at[index, "Status"] = "Skipped: Missing Data"
            continue

        try:
            get_response = requests.get(f"{api_url}/{CA_id}", headers=headers)
            if get_response.status_code != 200:
                df.at[index, "Status"] = f"GET Failed: {get_response.status_code}"
                print(f"GET failed for CA_ID: {CA_id} — Status Code: {get_response.status_code}")
                continue
            get_response.raise_for_status()
            CA_data = get_response.json()
            print(f"\nRow {index + 2} — CA_ID: {CA_id}")

            # Set values from Excel
            # CA_data["varietyId"] = variety_id
            CA_data["sowingDate"] = sowingDate
            print(f"Updated Variety_id: {variety_id}, sowing_date: {sowingDate}")

            time.sleep(1)

            put_response = requests.put(f"{api_url}", headers=headers, data=json.dumps(CA_data))
            if put_response.status_code != 200:
                df.at[index, "Status"] = f"PUT Failed: {put_response.status_code}"
                print(put_response)
                df.at[index, "CA_Response"] = put_response.text
                print(f"PUT failed for CA_ID: {CA_id} — Status Code: {put_response.status_code}")
                continue
            put_response.raise_for_status()

            df.at[index, "Status"] = "Success"
            df.at[index, "CA_Response"] = put_response.text
            print(f"Successfully updated CA_ID: {CA_id}")

        except requests.exceptions.RequestException as e:
            df.at[index, "Status"] = f"Failed: {str(e)}"
            print(f"Update failed for CA_ID: {CA_id} — {e}")

        time.sleep(2)

    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\nExcel file updated and saved as {output_excel}")


if __name__ == "__main__":
    input_excel = r"C:\Users\rajasekhar.palleti\Downloads\bat tenant second set.xlsx"
    sheet_name = "Sheet1"
    output_excel = r"C:\Users\rajasekhar.palleti\Downloads\bat tenant second set_DOS_Crop_output.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/croppable-areas"

    print("Retrieving access token...")
    token = get_access_token("bat", "6543345612", "Cropin123", "prod1")
    if token:
        print("Access token retrieved successfully.")
        post_data_to_api(api_url, token, input_excel, output_excel, sheet_name)
    else:
        print("Failed to retrieve access token. Process terminated.")
