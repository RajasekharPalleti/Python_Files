# Author: Rajasekhar Palleti

import json
import requests
import pandas as pd
import time

from GetAuthtoken import get_access_token


def post_data_to_api(api_url, token, input_excel, output_excel, sheet_name):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "channel": "mobile"
    }

    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    if "Status" not in df.columns:
        df["Status"] = ""
    if "CA_Response" not in df.columns:
        df["CA_Response"] = ""

    for index, row in df.iterrows():
        CA_id = row.iloc[0]  # Column A
        AuditValue = float(row.iloc[5])  # Column F, always as float

        if pd.isna(CA_id) or pd.isna(AuditValue):
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

            # Ensure areaAudit is a dict
            if "areaAudit" not in CA_data or not isinstance(CA_data["areaAudit"], dict):
                CA_data["areaAudit"] = {}

            # Assign float value directly to auditedArea under areaAudit
            CA_data["areaAudit"]["auditedArea"] = AuditValue

            # Ensure top-level auditedArea is a dict
            if "auditedArea" not in CA_data or not isinstance(CA_data["auditedArea"], dict):
                CA_data["auditedArea"] = {}

            # Assign float value to count field inside auditedArea dict
            CA_data["auditedArea"]["count"] = AuditValue

            print(f"Updated auditedArea: {AuditValue}")

            time.sleep(0.2)

            put_response = requests.put(f"{api_url}/area-audit", headers=headers, data=json.dumps(CA_data))
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

        time.sleep(0.2)

    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\nExcel file updated and saved as {output_excel}")


if __name__ == "__main__":
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\1.xlsx"
    sheet_name = "Sheet1"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\1_report.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/croppable-areas"

    print("Retrieving access token...")
    token = get_access_token("gbtogo", "m.blaser@gebana.com", "Cropin@123", "prod1")

    if token:
        print("Access token retrieved successfully.")
        post_data_to_api(api_url, token, input_excel, output_excel, sheet_name)
    else:
        print("Failed to retrieve access token. Process terminated.")
