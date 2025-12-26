# Author: Rajasekhar Palleti
# Purpose: Disable users in bulk (50 IDs per request)

import pandas as pd
import requests
import time
from GetAuthtoken import get_access_token

# =========================
# CONFIGURATION
# =========================
INPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\user_ids.xlsx"
OUTPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\user_ids_output.xlsx"
SHEET_NAME = "Sheet1"
BATCH_SIZE = 50
API_URL = "https://cloud.cropin.in/services/user/api/users/bulk"
DELAY_SECONDS = 5 # Delay between batches to avoid rate limiting

TENANT_CODE = "asp"
USERNAME = "9649964096"
PASSWORD = "123456"
ENVIRONMENT = "prod1"

# =========================
# GET TOKEN
# =========================
print("🔐 Fetching access token...")
token = get_access_token(TENANT_CODE, USERNAME, PASSWORD, ENVIRONMENT)

if not token:
    print("❌ Token generation failed. Exiting.")
    exit()

print("✅ Token received. Starting execution...")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# =========================
# READ EXCEL
# =========================
df = pd.read_excel(INPUT_EXCEL, sheet_name=SHEET_NAME)

if "user_id" not in df.columns:
    raise ValueError("❌ Excel must contain 'user_id' column")

# Clean IDs
df["user_id"] = df["user_id"].dropna().astype(int).astype(str)

# Add result columns
for col in ["Status", "Processed_User_Ids", "API_Response"]:
    if col not in df.columns:
        df[col] = ""

# =========================
# PROCESS IN BATCHES
# =========================
total = len(df)

for i in range(0, total, BATCH_SIZE):
    batch_df = df.iloc[i:i + BATCH_SIZE]
    user_ids = batch_df["user_id"].tolist()
    ids_param = ",".join(user_ids)

    print(f"\n🔁 Batch {(i // BATCH_SIZE) + 1}")
    print(f"👥 User IDs: {ids_param}")

    try:
        response = requests.delete(
            f"{API_URL}?ids={ids_param}&enabled=false",
            headers=headers
        )

        status = "Success" if response.status_code in (200, 204) else f"Failed ({response.status_code})"
        response_text = response.text

    except Exception as e:
        status = "Error"
        response_text = str(e)

    # Write result only in first row of batch
    first_index = batch_df.index[0]
    df.at[first_index, "Status"] = status
    df.at[first_index, "Processed_User_Ids"] = ids_param
    df.at[first_index, "API_Response"] = response_text

    print(f"📌 Result: {status}")
    time.sleep(DELAY_SECONDS)

# =========================
# SAVE OUTPUT
# =========================
df.to_excel(OUTPUT_EXCEL, index=False)
print(f"\n✅ Process completed. Output saved to:\n{OUTPUT_EXCEL}")
