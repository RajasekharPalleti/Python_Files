# Author: Rajasekhar Palleti

import json
import requests
import openpyxl
import pandas as pd
import time

from GetAuthtoken import get_access_token


def post_data_to_api(api_url, token, input_excel, output_excel):
    # Set up the headers using the retrieved token
    headers = {
        "Authorization": f"Bearer {token}"
    }

    # Load the Excel file using pandas
    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    # Ensure required columns exist
    if "Status" not in df.columns:
        df["Status"] = ""  # Add empty Status column if missing
    if "Asset_Response" not in df.columns:
        df["Asset_Response"] = ""  # Add empty Asset_Response column if missing

    # Process each row
    for index, row in df.iterrows():
        asset_id = row.iloc[0]  # Assuming Asset ID is in the first column (A)
        country_name = row.iloc[3]  # Assuming new Country attribute is in the fourth column (D)

        if pd.isna(asset_id) or pd.isna(country_name):
            df.at[index, "Status"] = "Skipped: Missing Data"
            continue

        try:
            # GET API call to retrieve asset data
            index : int
            get_response = requests.get(f"{api_url}/{asset_id}", headers=headers)
            get_response.raise_for_status()
            asset_data = get_response.json()
            print(f"Row = {index + 2}")  # Adding 2 because index starts from 0, and Excel starts from 1

            # Update the additional attribute section
            if "address" in asset_data and isinstance(asset_data["address"], dict):
                if "country" not in asset_data["address"] or asset_data["address"]["country"] is None:
                    asset_data["address"]["country"] = ""
                asset_data["address"]["country"] = country_name  # Update country name
                print(f"Country modified for: {asset_id}")
            else:
                df.at[index, "Status"] = "Failed: No country data"
                continue

            # Wait before making the PUT API call
            time.sleep(0.2)

            # Convert the payload to a multipart format
            multipart_data = {
                "dto": (None, json.dumps(asset_data), "application/json")
            }

            # PUT API call to update the asset data
            put_response = requests.put(api_url, headers=headers, files=multipart_data)
            put_response.raise_for_status()

            # Log success and store response
            df.at[index, "Status"] = "Success"
            df.at[index, "Asset_Response"] = put_response.text  # Store full API response
            print(f"Country is updated successfully for: {asset_id}")

        except requests.exceptions.RequestException as e:
            # Log failure and error message
            df.at[index, "Status"] = f"Failed: {str(e)}"
            print(f"Country update failed for: {asset_id}")

        # Wait before processing the next row
        time.sleep(0.2)

    # Save the updated file
    df.to_excel(output_excel, sheet_name="Sheet1", index=False)
    print(f"Excel file updated and saved as {output_excel}")


if __name__ == "__main__":
    # Define constants and file paths
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\Asset Updation_HPW.xlsx"
    sheet_name = "Sheet1"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\Asset Updation_HPW_Updated.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/assets"

    # Retrieve access token
    print("Retrieving access token")
    token = get_access_token("hpw", "Fd.sourcingdata@hpwag.ch", "Rus19057", "prod1")

    if token:
        print("Access token retrieved successfully")
        post_data_to_api(api_url, token, input_excel, output_excel)
    else:
        print("Failed to retrieve access token. Process terminated.")
