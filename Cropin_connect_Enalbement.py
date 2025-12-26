# Author: Rajasekhar Palleti
# Script: Cropin Connect (AcreSquare) Enablement – Batch of 100 with UserRole validation

import pandas as pd
import requests
import json
import time
from GetAuthtoken import get_access_token


# =========================
# CONFIG
# =========================
API_URL = "https://cloud.cropin.in/services/farm/api/acresquare/farmers-enable"
BATCH_SIZE = 100
DELAY = 10  # seconds


def enable_cropin_connect(token, input_excel, output_excel, sheet_name):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    # Ensure required columns
    required_cols = ["farmer_id", "userRoleId"]
    for col in required_cols:
        if col not in df.columns:
            raise Exception(f"Missing required column: {col}")

    # Add output columns
    for col in ["Status", "Response"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str)

    total_rows = len(df)

    for start in range(0, total_rows, BATCH_SIZE):
        batch_df = df.iloc[start:start + BATCH_SIZE]
        batch_index = batch_df.index

        farmer_ids = batch_df["farmer_id"].dropna().astype(int).tolist()
        role_ids = batch_df["userRoleId"].dropna().astype(int).unique().tolist()

        # ---- Validation: Same User Role ID ----
        if len(role_ids) != 1:
            msg = f"❌ Multiple UserRoleIds found in batch: {role_ids}"
            print(msg)
            df.at[batch_index[0], "Status"] = "Skipped"
            df.at[batch_index[0], "Response"] = msg
            continue

        user_role_id = role_ids[0]
        payload = farmer_ids

        print(f"\n🚀 Processing batch starting row {start + 2}")
        print(f"👥 Farmers count: {len(farmer_ids)} | UserRoleId: {user_role_id}")

        try:
            response = requests.post(
                f"{API_URL}?userRoleId={user_role_id}",
                headers=headers,
                json=payload,
                timeout=30
            )

            if response.status_code in [200, 201, 204]:
                df.at[batch_index[0], "Status"] = "Success"
                df.at[batch_index[0], "Response"] = response.text
                print("✅ Enablement successful")
            else:
                df.at[batch_index[0], "Status"] = f"Failed ({response.status_code})"
                df.at[batch_index[0], "Response"] = response.text
                print(f"❌ API Failed: {response.status_code}")

        except Exception as e:
            df.at[batch_index[0], "Status"] = "Error"
            df.at[batch_index[0], "Response"] = str(e)
            print(f"❌ Exception: {e}")

        time.sleep(DELAY)

    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\n✅ Process completed. Output saved to {output_excel}")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    INPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\Cropin_Connect_Enablement.xlsx"
    OUTPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\Cropin_Connect_Enablement_Output.xlsx"
    SHEET_NAME = "Sheet1"
    ENVIRONMENT = "prod1"

    print("🔐 Fetching access token...")
    token = get_access_token("asp", "9649964096", "123456", ENVIRONMENT)

    if token:
        print("✅ Token received")
        enable_cropin_connect(token, INPUT_EXCEL, OUTPUT_EXCEL, SHEET_NAME)
    else:
        print("❌ Failed to retrieve token. Script aborted.")
