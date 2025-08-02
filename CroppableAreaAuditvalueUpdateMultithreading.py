# Author: Rajasekhar Palleti

import json
import requests
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from GetAuthtoken import get_access_token
from threading import Lock

# Lock for thread-safe DataFrame writes
lock = Lock()

def process_row(index, row, api_url, headers):
    result = {}
    CA_id = row.iloc[0]  # Column A
    try:
        AuditValue = float(row.iloc[5])  # Column F
    except Exception:
        result["Status"] = "Skipped: Invalid AuditValue"
        return index, result

    if pd.isna(CA_id) or pd.isna(AuditValue):
        result["Status"] = "Skipped: Missing Data"
        return index, result

    try:
        time.sleep(0.5)  # Delay before GET
        get_response = requests.get(f"{api_url}/{CA_id}", headers=headers)
        if get_response.status_code != 200:
            result["Status"] = f"GET Failed: {get_response.status_code}"
            return index, result

        CA_data = get_response.json()

        # Ensure proper structure
        if "areaAudit" not in CA_data or not isinstance(CA_data["areaAudit"], dict):
            CA_data["areaAudit"] = {}
        CA_data["areaAudit"]["auditedArea"] = AuditValue

        if "auditedArea" not in CA_data or not isinstance(CA_data["auditedArea"], dict):
            CA_data["auditedArea"] = {}
        CA_data["auditedArea"]["count"] = AuditValue

        print(f"Row {index + 2} — CA_ID: {CA_id}, Updated auditedArea: {AuditValue}")

        time.sleep(0.5)  # Delay before PUT
        put_response = requests.put(
            f"{api_url}/area-audit",
            headers=headers,
            data=json.dumps(CA_data)
        )

        if put_response.status_code != 200:
            result["Status"] = f"PUT Failed: {put_response.status_code}"
            result["CA_Response"] = put_response.text
            return index, result

        result["Status"] = "Success"
        result["CA_Response"] = put_response.text
        print(f"PUT Success for CA_ID: {CA_id} (Row {index + 2})")
        return index, result

    except requests.exceptions.RequestException as e:
        result["Status"] = f"Failed: {str(e)}"
        return index, result


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

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(process_row, index, row, api_url, headers): index
            for index, row in df.iterrows()
        }

        for future in as_completed(futures):
            index = futures[future]
            try:
                idx, result = future.result()
                with lock:
                    for key, value in result.items():
                        df.at[idx, key] = value
            except Exception as e:
                print(f"Thread failed for row {index}: {e}")
                with lock:
                    df.at[index, "Status"] = f"Thread Error: {str(e)}"

    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\nExcel file updated and saved as {output_excel}")


if __name__ == "__main__":
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\3.xlsx"
    sheet_name = "Sheet1"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\3_report.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/croppable-areas"

    print("Retrieving access token...")
    token = get_access_token("gbtogo", "m.blaser@gebana.com", "Cropin@123", "prod1")

    if token:
        print("Access token retrieved successfully.")
        post_data_to_api(api_url, token, input_excel, output_excel, sheet_name)
    else:
        print("Failed to retrieve access token. Process terminated.")
