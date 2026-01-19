# Author: Rajasekhar Palleti

import json
import requests
import openpyxl
import time
from GetAuthtoken import get_access_token


def remove_tags_from_asset(asset_data, tags_to_remove):
    if "data" in asset_data and "tags" in asset_data["data"]:
        asset_data["data"]["tags"] = [
            tag for tag in asset_data["data"]["tags"]
            if tag["name"] not in tags_to_remove
        ]
    return asset_data


def process_assets(api_url, token, excel_file, sheet_name):
    wb = openpyxl.load_workbook(excel_file)

    if sheet_name not in wb.sheetnames:
        print(f"❌ Sheet '{sheet_name}' not found in the workbook.")
        return

    sheet = wb[sheet_name]
    headers = {"Authorization": f"Bearer {token}"}

    # Add 'Status' and 'Failure Reason' columns if not present
    headers_row = [sheet.cell(row=1, column=col).value for col in range(1, sheet.max_column + 1)]

    # Add missing header columns at the end (avoid overwriting if adding both)
    next_col = sheet.max_column + 1
    if "Status" not in headers_row:
        sheet.cell(row=1, column=next_col, value="Status")
        next_col += 1
    if "Failure Reason" not in headers_row:
        sheet.cell(row=1, column=next_col, value="Failure Reason")

    # Refresh header index after potential changes
    headers_row = [sheet.cell(row=1, column=col).value for col in range(1, sheet.max_column + 1)]
    status_col = headers_row.index("Status") + 1
    failure_reason_col = headers_row.index("Failure Reason") + 1

    for row in range(2, sheet.max_row + 1):
        asset_id = sheet.cell(row=row, column=1).value  # Asset ID in column 1
        tags_to_remove = sheet.cell(row=row, column=2).value  # Tags to remove in column 2

        # Normalize tags_to_remove to a list of strings. Accepts strings, numbers, rich text, etc.
        if tags_to_remove is None or (isinstance(tags_to_remove, str) and tags_to_remove.strip() == ""):
            tags_to_remove = []
        else:
            tags_str = str(tags_to_remove)
            tags_to_remove = [tag.strip() for tag in tags_str.split(",") if tag.strip()]

        print(f"Processing Asset ID {asset_id} - Removing Tags: {tags_to_remove}")

        if not asset_id or not tags_to_remove:
            sheet.cell(row=row, column=status_col, value="Skipped: Missing Data")
            sheet.cell(row=row, column=failure_reason_col, value="Missing Farmer ID or Tags")
            continue

        try:
            # Fetch asset details
            get_response = requests.get(f"{api_url}/{asset_id}", headers=headers)
            get_response.raise_for_status()
            asset_data = get_response.json()

            # Check which tags are missing
            existing_tags = [tag["name"] for tag in asset_data.get("data", {}).get("tags", [])]
            missing_tags = [tag for tag in tags_to_remove if tag not in existing_tags]

            if missing_tags:
                sheet.cell(row=row, column=failure_reason_col,
                           value=f"Tags not found in asset data: {', '.join(missing_tags)}")
                sheet.cell(row=row, column=status_col, value="Skipped")
                print(f"Given tags not found in asset data so we are keeping the asset data as same as before")
            else:
                sheet.cell(row=row, column=failure_reason_col, value="")

            # Remove tags
            updated_asset_data = remove_tags_from_asset(asset_data, tags_to_remove)

            # Convert to multipart form
            multipart_data = {"dto": (None, json.dumps(updated_asset_data), "application/json")}
            time.sleep(0.5)

            # Send update request
            put_response = requests.put(api_url, headers=headers, files=multipart_data)
            put_response.raise_for_status()

            if not missing_tags:
                sheet.cell(row=row, column=status_col, value="Success")

            print(f"✅ Successfully updated the asset {asset_id}")

        except requests.exceptions.RequestException as e:
            sheet.cell(row=row, column=status_col, value="Failed")
            sheet.cell(row=row, column=failure_reason_col, value=str(e))
            print(f"❌ Failed to remove tags for asset {asset_id}: {e}")

        time.sleep(0.5)

    wb.save(excel_file)
    print(f"✅ Excel file updated and saved: {excel_file}")


if __name__ == "__main__":
    excel_file = "C:\\Users\\rajasekhar.palleti\\Downloads\\Asset_Tags.xlsx" # 👈 Replace with the actual excel path
    sheet_name = "Sheet1"  # 👈 Replace with the actual sheet name
    api_url = "https://cloud.cropin.in/services/farm/api/assets"
    environment = "prod1"

    print("⏳ Retrieving access token...")
    token = get_access_token("asp", "9649964096", "123456", environment)

    if token:
        print("✅ Access token retrieved successfully")
        process_assets(api_url, token, excel_file, sheet_name)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")
