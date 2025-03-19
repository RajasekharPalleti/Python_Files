# Author: Rajasekhar Palleti

import json
import pandas as pd
import requests
import time

from GetAuthtoken import get_access_token

def post_data_to_api(api_url, token, file_path, sheet_name):
    # Set up the headers using the retrieved token
    headers = {
        "Authorization": f"Bearer {token}"
    }

    # Load Excel file with the specific sheet
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # Ensure necessary columns exist, else add them
    required_columns = ["status", "Response"]
    for col in required_columns:
        if col not in df.columns:
            df[col] = ""

    df = df.astype(str)  # Convert to string to avoid dtype issues

    # Iterate over rows
    for index, row in df.iloc[:].iterrows():
        try:
            asset_id = str(row.iloc[0]).strip()
            new_country_name = str(row.iloc[1]).strip()

            print(f"🔄 Processing row {index + 1}: asset_id = {asset_id}")

            # Skip empty asset_id
            if not asset_id:
                print(f"⚠️ Skipping row {index + 1}: asset_id is missing.")
                df.at[index, "status"] = "⚠️ Skipped - Missing asset_id"
                continue

            try:
                # GET API call to retrieve asset data
                get_response = requests.get(f"{api_url}/{asset_id}", headers=headers)
                get_response.raise_for_status()
                asset_data = get_response.json()
                print(f"Getting asset details for: {asset_id}")

                # Update the country with the new country name
                if "address" in asset_data and isinstance(asset_data["address"], dict):
                    if "country" not in asset_data["address"] or asset_data["address"]["country"] is None:
                        asset_data["address"]["country"] = ""
                    asset_data["address"]["country"] = new_country_name
                    print(f"Country attribute modified for: {asset_id}")
                else:
                    df.at[index, "status"] = "Failed: No address attribute"
                    continue

                # Wait before making the PUT API call
                time.sleep(0.2)

                # Convert the payload to a multipart format
                multipart_data = {
                    "dto": (None, json.dumps(asset_data), "application/json")  # Convert the entire payload to JSON string
                }

                # PUT API call to update the asset data
                put_response = requests.put(api_url, headers=headers, files=multipart_data)
                put_response.raise_for_status()

                # Log success in DataFrame
                df.at[index, "status"] = "Success"
                df.at[index, "Response"] = put_response.text  # Store response
                print(f"✅ Country updated successfully for: {asset_id}")

            except requests.exceptions.RequestException as e:
                # Log failure in DataFrame
                df.at[index, "status"] = f"❌ Failed: {str(e)}"
                print(f"❌ Country update failed for: {asset_id}")

            # Wait before processing the next row
            time.sleep(0.5)

        except Exception as e:
            df.at[index, "status"] = f"Error: {str(e)}"
            print(f"⚠️ Unexpected error on row {index + 1}: {str(e)}")

    # Save updated Excel file
    print("📁 Writing results to Excel")
    with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    print("✅ Processing complete. Results saved.")


if __name__ == "__main__":
    # Start time before execution
    overall_start_time = time.time()

    # Define constants and file paths
    file_path = "C:\\Users\\rajasekhar.palleti\\Downloads\\Asset_updated.xlsx"
    sheet_name = "result"
    api_url = "https://cloud.cropin.in/services/farm/api/assets"

    # Retrieve access token
    print("🔐 Retrieving access token...")
    token = get_access_token("hpw", "Fd.sourcingdata@hpwag.ch", "Rus19057", "prod1")

    if token:
        print("✅ Access token retrieved successfully.")
        post_data_to_api(api_url, token, file_path, sheet_name)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")

    # End time after execution
    overall_end_time = time.time()

    # Calculate total execution time
    total_execution_time = overall_end_time - overall_start_time

    # Convert to minutes and seconds
    minutes = int(total_execution_time // 60)
    seconds = int(total_execution_time % 60)
    print(f"⏱️ Overall Execution Time: {minutes} min {seconds} sec")