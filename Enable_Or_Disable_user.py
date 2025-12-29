# Author: Rajasekhar Palleti
# Script: Enable / Disable Users from Excel
# API: PUT /services/user/api/users/enable/{userId}?enableFlag=true|false

import pandas as pd
import requests
import time
from GetAuthtoken import get_access_token

delay_time = 0.5  # seconds


def enable_disable_users(api_base_url, token, input_excel, output_excel, sheet_name):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    print("📂 Reading Excel file...")
    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    # Ensure status columns exist
    if "Status" not in df.columns:
        df["Status"] = ""
    if "Response" not in df.columns:
        df["Response"] = ""

    for index, row in df.iterrows():
        user_id = row["user_id"]
        enable_flag = row["enableFlag"]

        if pd.isna(user_id) or pd.isna(enable_flag):
            df.at[index, "Status"] = "⚠️ Skipped: Missing Data"
            continue

        # Normalize enableFlag (true/false)
        enable_flag_str = str(enable_flag).lower()
        if enable_flag_str not in ["true", "false"]:
            df.at[index, "Status"] = "❌ Invalid enableFlag"
            continue

        url = f"{api_base_url}/enable/{int(user_id)}?enableFlag={enable_flag_str}"

        print(f"🔄 Processing row {index + 1} | UserID: {user_id} | enableFlag: {enable_flag_str}")

        try:
            response = requests.put(url, headers=headers)

            if response.status_code in [200, 204]:
                df.at[index, "Status"] = "✅ Success"
                df.at[index, "Response"] = response.text
                print(f"✅ User {user_id} updated successfully")
            else:
                df.at[index, "Status"] = f"❌ Failed: {response.status_code}"
                df.at[index, "Response"] = response.text
                print(f"❌ Failed for User {user_id}: {response.status_code}")

        except Exception as e:
            df.at[index, "Status"] = "❌ Error"
            df.at[index, "Response"] = str(e)
            print(f"❌ Exception for User {user_id}: {e}")

        time.sleep(delay_time)  # Same delay pattern as your scripts

    print("💾 Saving output Excel...")
    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"✅ File saved: {output_excel}")


# =========================
# MAIN EXECUTION
# =========================
if __name__ == "__main__":

    BASE_API_URL = "https://cloud.cropin.in/services/user/api/users"
    TENANT_CODE = "asp"
    ENVIRONMENT = "prod1"

    INPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\Enable_Disable_Users.xlsx"
    OUTPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\Enable_Disable_Users_Output.xlsx"
    SHEET_NAME = "Sheet1"

    print("🔐 Fetching access token...")
    token = get_access_token(TENANT_CODE, "9649964096", "123456", ENVIRONMENT)

    if token:
        print("✅ Access token retrieved successfully.")
        enable_disable_users(
            BASE_API_URL,
            token,
            INPUT_EXCEL,
            OUTPUT_EXCEL,
            SHEET_NAME
        )
    else:
        print("❌ Failed to retrieve access token. Process terminated.")
        exit()