# Author: Rajasekhar Palleti
# Script: Refresh Assets using GET + PUT (Multipart) with Multithreading

import pandas as pd
import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from GetAuthtoken import get_access_token


def refresh_assets_from_excel(token):
    # ================= CONFIG ================= #
    API_URL = "https://cloud.cropin.in/services/farm/api/assets"

    INPUT_EXCEL = r"C:\Users\meghavarna.m\Downloads\asset_ids.xlsx"
    OUTPUT_EXCEL = r"C:\Users\meghavarna.m\Downloads\asset_ids_output.xlsx"

    MAX_RETRIES = 2
    MAX_WORKERS = 4
    SUBMIT_GAP_SECONDS = 1
    # ========================================== #

    headers = {
        "Authorization": f"Bearer {token}"
    }

    print("📂 Reading Excel...")
    df = pd.read_excel(INPUT_EXCEL)

    if "Status" not in df.columns:
        df["Status"] = ""
    if "Message" not in df.columns:
        df["Message"] = ""

    # -------------------------------------------------
    # Worker function
    # -------------------------------------------------
    def process_asset(index, asset_id):
        if pd.isna(asset_id):
            return index, "SKIPPED", "Asset ID missing"

        asset_id = int(asset_id)
        print(f"🔄 Processing Asset ID: {asset_id}")

        last_error = "Unknown error"

        for _ in range(MAX_RETRIES):
            try:
                # -------- GET --------
                get_resp = requests.get(f"{API_URL}/{asset_id}", headers=headers)

                if get_resp.status_code != 200:
                    last_error = f"GET Failed: {get_resp.status_code}"
                    time.sleep(SUBMIT_GAP_SECONDS)
                    continue

                asset_data = get_resp.json()

                # Skip deleted assets
                if asset_data.get("deleted") is True:
                    return index, "SKIPPED", "Asset is deleted"

                # -------- PUT (NO ID IN URL) --------
                multipart_data = {
                    "dto": (None, json.dumps(asset_data), "application/json")
                }

                put_resp = requests.put(
                    API_URL,
                    headers=headers,
                    files=multipart_data
                )

                if put_resp.status_code in (200, 204):
                    return index, "PASS", "Refreshed successfully"

                last_error = f"PUT Failed: {put_resp.status_code}"
                time.sleep(SUBMIT_GAP_SECONDS)

            except Exception as e:
                last_error = str(e)
                time.sleep(SUBMIT_GAP_SECONDS)

        return index, "FAIL", last_error

    # -------------------------------------------------
    # Multithreading execution
    # -------------------------------------------------
    futures = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for index, row in df.iterrows():
            futures.append(
                executor.submit(process_asset, index, row.iloc[0])
            )
            time.sleep(SUBMIT_GAP_SECONDS)  # 1 sec gap between submissions

        for future in as_completed(futures):
            index, status, message = future.result()
            df.at[index, "Status"] = status
            df.at[index, "Message"] = message

    print("💾 Writing output Excel...")
    df.to_excel(OUTPUT_EXCEL, index=False)
    print(f"✅ Done. Output saved at:\n{OUTPUT_EXCEL}")


# ================= EXECUTION =================
if __name__ == "__main__":

    print("🔐 Fetching access token...")
    token = get_access_token("tenant_code", "username", "password", "prod1")

    if token:
        print("✅ Access token retrieved successfully.")
        refresh_assets_from_excel(token)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")
