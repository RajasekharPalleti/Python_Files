#Author : Rajasekhar Palleti

import json
import requests
import openpyxl
import time
from GetAuthtoken import get_access_token


def remove_tags_from_farmer(farmer_data, tags_to_remove):
    if "data" in farmer_data and "tags" in farmer_data["data"]:
        farmer_data["data"]["tags"] = [tag for tag in farmer_data["data"]["tags"] if tag["name"] not in tags_to_remove]
    return farmer_data


def process_farmers(api_url, token, input_excel, output_excel):
    wb = openpyxl.load_workbook(input_excel)
    sheet = wb.active

    headers = {"Authorization": f"Bearer {token}"}

    if sheet.cell(1, sheet.max_column).value != "Status":
        sheet.cell(1, sheet.max_column + 1, "Status")

    status_col = sheet.max_column

    for row in range(2, sheet.max_row + 1):
        farmer_id = sheet.cell(row=row, column=1).value  # Farmer ID in column 1
        tags_to_remove = sheet.cell(row=row, column=2).value  # Tags to remove in column 2

        if tags_to_remove:
            tags_to_remove = [tag.strip() for tag in tags_to_remove.split(",")]
        else:
            tags_to_remove = []

        print(f"Processing Farmer ID {farmer_id} - Removing Tags: {tags_to_remove}")

        if not farmer_id or not tags_to_remove:
            sheet.cell(row=row, column=status_col, value="Skipped: Missing Data")
            continue

        try:
            # Fetch farmer details
            get_response = requests.get(f"{api_url}/{farmer_id}", headers=headers)
            get_response.raise_for_status()
            farmer_data = get_response.json()

            # Remove tags
            updated_farmer_data = remove_tags_from_farmer(farmer_data, tags_to_remove)

            # Convert to multipart data
            multipart_data = {"dto": (None, json.dumps(updated_farmer_data), "application/json")}
            time.sleep(0.5)  # Sleep to avoid API rate limits

            # Send update request
            put_response = requests.put(api_url, headers=headers, files=multipart_data)
            put_response.raise_for_status()

            sheet.cell(row=row, column=status_col, value="Success")
            print(f"✅ Successfully updated Farmer ID {farmer_id}")

        except requests.exceptions.RequestException as e:
            sheet.cell(row=row, column=status_col, value="Failed")
            print(f"❌ Failed to update Farmer ID {farmer_id}: {e}")

        time.sleep(0.5)  # Sleep to avoid API rate limits

    wb.save(output_excel)
    print(f"✅ Excel file updated and saved as {output_excel}")


if __name__ == "__main__":
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\Farmer_Tags.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\Farmer_Tags_Output.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/farmers"
    environment = "prod1"

    print("⏳ Retrieving access token...")
    token = get_access_token("asp", "9649964096", "123456", environment)

    if token:
        print("✅ Access token retrieved successfully")
        process_farmers(api_url, token, input_excel, output_excel)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")
