# Author: Rajasekhar Palleti (Fixed Version)

import json
import pandas as pd
import requests
import time

from GetAuthtoken import get_access_token


def post_data_to_api(api_url, token, file_path, sheet_name):

    headers = {
        "Authorization": f"Bearer {token}"
    }

    df = pd.read_excel(file_path, sheet_name=sheet_name)

    required_columns = ["status", "Response"]
    for col in required_columns:
        if col not in df.columns:
            df[col] = ""

    df = df.astype(str)

    for index, row in df.iterrows():

        asset_id = str(row.iloc[1]).strip()

        print(f"\n🔄 Processing row {index + 1}: asset_id = {asset_id}")

        if not asset_id:
            df.at[index, "status"] = "⚠️ Skipped - Missing asset_id"
            continue

        try:
            get_response = requests.get(f"{api_url}/{asset_id}", headers=headers)
            get_response.raise_for_status()
            asset_data = get_response.json()

            print(f"✅ Asset fetched: {asset_id}")

            if not isinstance(asset_data.get("data"), dict):
                df.at[index, "status"] = "❌ Failed: No data attribute"
                continue

            if "irrigation type" in asset_data["data"]:
                asset_data["data"].pop("irrigation type")
                print(f"🗑️ Irrigation type removed for: {asset_id}")
            else:
                print(f"ℹ️ Irrigation type not present for: {asset_id}")

            time.sleep(0.2)

            multipart_data = {
                "dto": (None, json.dumps(asset_data), "application/json")
            }

            put_response = requests.put(api_url, headers=headers, files=multipart_data)
            put_response.raise_for_status()

            df.at[index, "status"] = "✅ Success"
            df.at[index, "Response"] = put_response.text

            print(f"🚀 Asset updated successfully: {asset_id}")

        except requests.exceptions.RequestException as e:
            df.at[index, "status"] = f"❌ API Failed: {str(e)}"
            print(f"❌ API error for asset {asset_id}: {str(e)}")

        except Exception as e:
            df.at[index, "status"] = f"⚠️ Error: {str(e)}"
            print(f"⚠️ Unexpected error for asset {asset_id}: {str(e)}")

        time.sleep(0.5)

    # Save Excel
    print("\n📁 Writing results to Excel")
    if "." in file_path:
        base, ext = file_path.rsplit(".", 1)
        output_file = f"{base}_output.{ext}"
    else:
        output_file = f"{file_path}_output"
    print(f"📄 Output file: {output_file}")
    with pd.ExcelWriter(output_file, engine='openpyxl', mode='w') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    print("✅ Processing complete. Results saved.")


if __name__ == "__main__":
    file_path = r"C:\Users\rajasekhar.palleti\Downloads\Asset_Details to remove the irrigation type in data.xlsx"
    sheet_name = "result"
    api_url = "https://cloud.cropin.in/services/farm/api/assets"

    print("🔐 Retrieving access token...")
    token = get_access_token("jkf", "8688194896", "cropin@123", "prod1")

    if token:
        print("✅ Access token retrieved successfully.")
        post_data_to_api(api_url, token, file_path, sheet_name)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")
