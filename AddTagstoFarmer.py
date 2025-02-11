import json
import requests
import openpyxl
import time
import sys  # Import sys to exit the script
from GetAuthtoken import get_access_token


def fetch_tags(token, tags_api_url, required_tags):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    try:
        print("Fetching all tags from the tags API")
        response = requests.get(tags_api_url, headers=headers)
        response.raise_for_status()
        tags_data = response.json()
        if not tags_data:
            print("Error: Tags API returned an empty response. Exiting script.")
            sys.exit(1)
        tags_dict = {tag["name"]: tag for tag in tags_data if tag["name"] in required_tags}
        print(f"Fetched required {len(tags_dict)} tags from the tags API")
        if not tags_dict:
            print("Error: No matching tags found in the API response. Exiting script.")
            sys.exit(1)
        return tags_dict
    except requests.exceptions.RequestException as e:
        print(f"Error fetching tags from API: {e}. Exiting script.")
        sys.exit(1)


def post_data_to_api(api_url, token, input_excel, output_excel, tags_api_url, required_tags):
    tags_dict = fetch_tags(token, tags_api_url, required_tags)
    headers = {"Authorization": f"Bearer {token}"}
    wb = openpyxl.load_workbook(input_excel)
    sheet = wb.active

    if sheet.cell(1, sheet.max_column).value != "Status":
        sheet.cell(1, sheet.max_column + 1, "Status")
    if sheet.cell(1, sheet.max_column).value != "Failure Reason":
        sheet.cell(1, sheet.max_column + 1, "Failure Reason")

    status_col = sheet.max_column - 1
    failure_reason_col = sheet.max_column

    for row in range(2, sheet.max_row + 1):
        farmer_id = sheet.cell(row=row, column=1).value  # Assume farmer IDs are in column 1
        print(f"Processing row {row} - Farmer ID: {farmer_id}")

        if not farmer_id:
            sheet.cell(row=row, column=status_col, value="Skipped: Missing Data")
            continue
        try:
            print(f"Fetching details for Farmer ID: {farmer_id}")
            get_response = requests.get(f"{api_url}/{farmer_id}", headers=headers)
            get_response.raise_for_status()
            farmer_data = get_response.json()

            if "data" in farmer_data and isinstance(farmer_data["data"], dict):
                existing_tags = {tag["name"] for tag in farmer_data["data"].get("tags", [])}
                new_tags = [tag for name, tag in tags_dict.items() if name not in existing_tags]
                print(f"Taken {len(new_tags)} new {new_tags} tags from the tags API")

                if new_tags:
                    if "tags" not in farmer_data["data"] or farmer_data["data"]["tags"] is None:
                        farmer_data["data"]["tags"] = []  # Ensure it's a list

                    farmer_data["data"]["tags"].extend(new_tags)  # Add new tags safely
                    print(f"Added new tags to Farmer ID: {farmer_id}")
                else:
                    print(f"No new tags to add for Farmer ID: {farmer_id}, skipping update.")
                    sheet.cell(row=row, column=status_col, value="Skipped: Tags Already Present")
                    continue
            else:
                print(f"Failed to update tags for Farmer ID: {farmer_id} - No 'data' key found")
                sheet.cell(row=row, column=status_col, value="Failed: No data key in farmer data")
                sheet.cell(row=row, column=failure_reason_col, value=str(farmer_data))
                continue

            time.sleep(0.2)
            multipart_data = {"dto": (None, json.dumps(farmer_data), "application/json")}
            print(f"Posting updated data for Farmer ID: {farmer_id}")
            put_response = requests.put(api_url, headers=headers, files=multipart_data)
            put_response.raise_for_status()
            sheet.cell(row=row, column=status_col, value="Success")
            print(f"Successfully posted updates for Farmer ID: {farmer_id}")

        except requests.exceptions.RequestException as e:
            error_message = str(e)
            if hasattr(e, 'response') and e.response is not None:
                error_message = e.response.text
            sheet.cell(row=row, column=status_col, value="Failed")
            sheet.cell(row=row, column=failure_reason_col, value=error_message)
            print(f"Failed to update Farmer ID: {farmer_id} - Error: {error_message}")

        time.sleep(0.5)

    wb.save(output_excel)
    print(f"Excel file updated and saved as {output_excel}")


if __name__ == "__main__":
    tenant_code = "asp"
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\FarmerTagsInsert.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\FarmerTagsInsertUpdated.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/farmers"
    tags_api_url = "https://cloud.cropin.in/services/farm/api/tags?size=5000"
    required_tags = ["cropin connect", "aaaaaaaa", "raja", "sekhar"]

    print("Retrieving access token")
    token = get_access_token(tenant_code, "9649964096", "123456")

    if token:
        print("Access token retrieved successfully")
        post_data_to_api(api_url, token, input_excel, output_excel, tags_api_url, required_tags)
    else:
        print("Failed to retrieve access token. Process terminated.")
