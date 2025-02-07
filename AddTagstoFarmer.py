# Author: Rajasekhar Palleti

import json
import requests
import openpyxl
import time
from GetAuthtoken import get_access_token

# Input which we need to add to Farmer DTO in data -> tags
tags = [ {
      "id" : 3274852,
      "name" : "cropin connect",
      "clientId" : "b5ddbbf5-5751-4db0-aded-f8426cda2cc5"
    } ]

def post_data_to_api(api_url, token, input_excel, output_excel):
    # Set up the headers using the retrieved token
    headers = {
        "Authorization": f"Bearer {token}"
    }

    # Load the Excel workbook
    wb = openpyxl.load_workbook(input_excel)
    sheet = wb.active

    # Check and add a "Status" column header if it doesn't exist
    if sheet.cell(1, sheet.max_column).value != "Status":
        sheet.cell(1, sheet.max_column + 1, "Status")
    status_col = sheet.max_column

    # Process each row starting from the second row
    for row in range(2, sheet.max_row + 1):
        farmer_id = sheet.cell(row=row, column=1).value   # Assuming Farmer ID is in column A
        if not farmer_id:
            sheet.cell(row=row, column=status_col, value="Skipped: Missing Data")
            continue

        try:
            # GET API call to retrieve farmer data
            get_response = requests.get(f"{api_url}/{farmer_id}", headers=headers)
            get_response.raise_for_status()
            farmer_data = get_response.json()
            print(f"Row = {row}")
            print(f"Getting asset details for: {farmer_id}")

            # Update/Insert tags in data section
            if "data" in farmer_data and isinstance(farmer_data["data"], dict):
                farmer_data["data"]["tags"] = tags
                print(f"Tags is Inserted/modified for: {farmer_id}")
            else:
                sheet.cell(row=row, column=status_col, value="Failed: No data key in farmer data")
                continue

            # Wait before making the PUT API call
            time.sleep(0.2)
            # Convert the payload to a multipart format
            multipart_data = {
                "dto": (None, json.dumps(farmer_data), "application/json")  # Convert the entire payload to JSON string
            }

            # PUT API call to update the farmer data
            put_response = requests.put(api_url, headers=headers, files=multipart_data)
            put_response.raise_for_status()

            # Log success in Excel
            sheet.cell(row=row, column=status_col, value="Success")
            print(f"Tags is inserted/modified through put API successfully for: {farmer_id}")
        except requests.exceptions.RequestException as e:
            # Log failure in Excel and print the error
            sheet.cell(row=row, column=status_col, value=f"Failed: {str(e)}")
            print(f"Tags is inserted/modified through put API failed for: {farmer_id}")

        # Wait before processing the next row
        time.sleep(0.5)

    # Save the updated Excel file
    wb.save(output_excel)
    print(f"Excel file updated and saved as {output_excel}")

if __name__ == "__main__":
    # Define constants and file paths
    tenant_code = "asp"
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\FarmerTagsInsert.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\FarmerTagsInsertUpdated.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/farmers"


    # Retrieve access token
    print("Retrieving access token")
    token = get_access_token(tenant_code, "9649964096", "123456")

    if token:
        print("Access token retrieved successfully")
        post_data_to_api(api_url, token, input_excel, output_excel)
    else:
        print("Failed to retrieve access token. Process terminated.")
