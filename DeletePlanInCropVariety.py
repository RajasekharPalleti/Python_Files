#Author: Rajasekhar Palleti

import pandas as pd
import requests
import time
from GetAuthtoken import get_access_token
"""
    Reads plan IDs from 2nd column of Excel,
    deletes them via API, and writes 'status' and 'response' back to output Excel.
"""

def delete_plan_ids(url, input_excel, sheet_name, output_excel, token):
    try:
        df = pd.read_excel(input_excel, sheet_name=sheet_name)
    except Exception as e:
        print(f"❌ Failed to read Excel file: {e}")
        return

    # Lists to store results
    status_list = []
    response_list = []

    # Iterate row by row
    for idx, row in df.iterrows():
        plan_ids_str = row.iloc[1]  # second column value

        # Skip empty cells
        if pd.isna(plan_ids_str) or str(plan_ids_str).strip() == "":
            status_list.append("SKIPPED")
            response_list.append("No plan ID provided")
            print(f"Row {idx}: SKIPPED (no ID)")
            continue

        # Clean comma-separated IDs
        plan_ids = ",".join([pid.strip() for pid in str(plan_ids_str).split(",") if pid.strip()])

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        try:
            resp = requests.delete(f"{url}{plan_ids}", headers=headers)
            if resp.status_code in (200, 204):
                status_list.append("PASS")
            else:
                status_list.append("FAIL")
            response_list.append(resp.text)
            print(f"Row {idx}: Deleting [{plan_ids}] -> {status_list[-1]} (HTTP {resp.status_code})")
        except Exception as e:
            status_list.append("FAIL")
            response_list.append(str(e))
            print(f"Row {idx}: Deleting [{plan_ids}] -> FAIL (exception: {e})")

        time.sleep(0.5)  # throttle requests

    # Add results to DataFrame
    df["status"] = status_list
    df["response"] = response_list

    # Save to an output file
    try:
        df.to_excel(output_excel, index=False)
        print(f"✅ Deletion completed. Results saved to: {output_excel}")
    except Exception as e:
        print(f"❌ Failed to write output Excel: {e}")


if __name__ == "__main__":
    # ===== CONSTANTS =====
    url = "https://cloud.cropin.in/services/farm/api/plans/bulk?ids="
    INPUT_EXCEL = r"C:\path\to\your\input.xlsx"
    SHEET_NAME = "Sheet1"
    OUTPUT_EXCEL = r"C:\path\to\your\output.xlsx"

    # ===== GET TOKEN =====
    token = get_access_token("asp", "9649964096", "123456", "prod1")

    # ===== CHECK TOKEN =====
    if not token:
        print("❌ Failed to retrieve token. Stopping execution.")
    else:
        print("✅ Token retrieved. Proceeding with deletions...")
        delete_plan_ids(url,INPUT_EXCEL, SHEET_NAME, OUTPUT_EXCEL, token)
