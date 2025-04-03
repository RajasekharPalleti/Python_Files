#Author : Rajasekhar Palleti

import json
import requests
import openpyxl
import time
from GetAuthtoken import get_access_token


def remove_tags_from_asset(asset_data, tags_to_remove):
    if "data" in asset_data and "tags" in asset_data["data"]:
        asset_data["data"]["tags"] = [tag for tag in asset_data["data"]["tags"] if tag["name"] not in tags_to_remove]
    return asset_data


def process_assets(api_url, token, input_excel, output_excel):
    wb = openpyxl.load_workbook(input_excel)
    sheet = wb.active

    headers = {"Authorization": f"Bearer {token}"}

    if sheet.cell(1, sheet.max_column).value != "Status":
        sheet.cell(1, sheet.max_column + 1, "Status")

    status_col = sheet.max_column

    for row in range(2, sheet.max_row + 1):
        asset_id = sheet.cell(row=row, column=1).value  # Asset ID in column 1
        tags_to_remove = sheet.cell(row=row, column=2).value  # Tags to remove in column 2

        if tags_to_remove:
            tags_to_remove = [tag.strip() for tag in tags_to_remove.split(",")]
        else:
            tags_to_remove = []

        print(f"Processing Asset ID {asset_id} - Removing Tags: {tags_to_remove}")

        if not asset_id or not tags_to_remove:
            sheet.cell(row=row, column=status_col, value="Skipped: Missing Data")
            continue

        try:
            # Fetch asset details
            get_response = requests.get(f"{api_url}/{asset_id}", headers=headers)
            get_response.raise_for_status()
            asset_data = get_response.json()

            # Remove tags
            updated_asset_data = remove_tags_from_asset(asset_data, tags_to_remove)

            # Convert to multipart data
            multipart_data = {"dto": (None, json.dumps(updated_asset_data), "application/json")}

            time.sleep(0.5)  # Sleep to avoid API rate limits

            # Send update request
            put_response = requests.put(api_url, headers=headers, files=multipart_data)
            put_response.raise_for_status()

            sheet.cell(row=row, column=status_col, value="Success")
            print(f"✅ Successfully updated Asset ID {asset_id}")

        except requests.exceptions.RequestException as e:
            sheet.cell(row=row, column=status_col, value="Failed")
            print(f"❌ Failed to update Asset ID {asset_id}: {e}")

        time.sleep(0.5)  # Sleep to avoid API rate limits

    wb.save(output_excel)
    print(f"✅ Excel file updated and saved as {output_excel}")


if __name__ == "__main__":
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\Asset_Tags.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\Asset_Tags_Output.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/assets"
    environment = "prod1"

    print("⏳ Retrieving access token...")
    token = get_access_token("gpi", "9120232024", "Anilkumarkolla", environment)

    if token:
        print("✅ Access token retrieved successfully")
        process_assets(api_url, token, input_excel, output_excel)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")
