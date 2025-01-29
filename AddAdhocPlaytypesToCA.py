#Author : Rajasekhar Palleti
#QA

import json
import pandas as pd
import requests
import time
from GetAuthtoken import get_access_token

def safe_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0  # Default to 0 or handle as needed
# Function to post data to the API
def post_data_to_api(get_api_url, put_api_url, token, input_file, output_file, dto=None):
    # Load the Excel file into a DataFrame
    exdata = pd.read_excel(input_file)

    # Replace NaN values with empty strings
    exdata = exdata.fillna("")  # Replace NaN with empty strings to avoid JSON compliance issues

    # Ensure a new column 'status' exists in the DataFrame
    exdata['status'] = ""
    exdata['Response'] = ''  # Add a column to store the full response

    # Iterate over each row in the DataFrame
    for index, row in exdata.iterrows():
        try:
            # Extract necessary values from the row
            croppable_area_id = row.iloc[0]  # Croppable Area ID from column 1
            plan_name = row.iloc[1]  # Plan Name from column 2
            plantype_id = row.iloc[2]  # Plantype ID from column 3
            plan_type_name = row.iloc[3]  # Plan Type Name from column 4
            project_id = row.iloc[4]  # Project ID from column 5
            varieties = row.iloc[5]  # Varieties from column 6
            schedule_type = row.iloc[6]  # Schedule Type from column 7
            no_of_days = safe_int(row.iloc[7])  # Number of Days from column 8
            execute_when = row.iloc[8]  # Execute When from column 9
            reference_date = row.iloc[9]  # Reference Date from column 10
            required_days = safe_int(row.iloc[10])  # Required Days from column 11

            # GET API call to fetch plantype
            index : int
            print(f"Row {index+1}: Sending GET request to fetch plan type with ID {plantype_id}")
            get_url = f"{get_api_url}/{plantype_id}"
            headers = {"Authorization": f"Bearer {token}"}
            get_response = requests.get(get_url, headers=headers)

            if get_response.status_code == 200:
                print(f"Row {index+1}: Successfully fetched plan type")
                plantype_response = get_response.json()
            else:
                print(f"Row {index+1}: Failed to fetch plan type, Status Code: {get_response.status_code}")
                exdata.at[index, 'status'] = f"Failed GET: {get_response.status_code}"
                continue

            # Prepare POST API call payload
            print(f"Row {index+1}: Preparing POST request payload")
            post_payload = {
                "croppableAreaId": croppable_area_id,
                "data": {
                    "information": {
                        "planName": plan_name,
                        "planType": plantype_response,
                        "geoLocation": False,
                        "signature": False
                    },
                    "customAttributes": {},
                    "planHeaderAttributes": [],
                    "planHeaderGroup": {}
                },
                "images": {},
                "name": plan_name,
                "planTypeId": plantype_id,
                "planTypeName": plan_type_name,
                "projectId": project_id,
                "schedule": {
                    "planParams": f"?croppableAreaIds={croppable_area_id}",
                    "type": schedule_type,
                    "fixedDate": False,
                    "noOfDays": int(no_of_days),
                    "executeWhen": execute_when,
                    "referenceDate": reference_date,
                    "requiredDays": required_days,
                    "recuring": False
                },
                "varieties": [varieties]
            }
            # Convert the payload to a multipart format
            multipart_data = {
                "dto": (None, json.dumps(post_payload), "application/json") # Convert the entire payload to JSON string
            }

            # POST API call with multipart/form-data
            print(f"Row {index+1}: Sending POST request to create/update plan as multipart/form-data")

            post_response = requests.post(
                f"{put_api_url}/{croppable_area_id}",
                headers=headers,
                files=multipart_data
            )

            # Handle the response
            if post_response.status_code == 200:
                print(f"Row {index+1}: POST request successful")
                exdata.at[index, 'status'] = "Success"
                exdata.at[index, 'Response'] = f"Code: {post_response.status_code}, Message: {post_response.text}"
            else:
                print(f"Row {index+1}: POST request failed, Status Code: {post_response.status_code}")
                exdata.at[index, 'status'] = f"Failed POST: {post_response.status_code}"
                exdata.at[index, 'Response'] = f"Reason: {post_response.reason}, Message: {post_response.text}"

            # Wait for 1 second before the next iteration
            print("Waiting for 0.3 second before the next iteration")
            time.sleep(0.3)

        except Exception as e:
            print(f"Row {index+1}: Encountered an error: {str(e)}")
            exdata.at[index, 'status'] = f"Error: {str(e)}"

    # Save the updated DataFrame to the output Excel file
    print(f"Saving results to {output_file}")
    exdata.to_excel(output_file, index=False)
    print(f"Process completed. Output written to {output_file}")

# Main program
if __name__ == "__main__":
    get_api_url = "https://cloud.cropin.in/services/farm/api/plan-types"
    put_api_url = "https://cloud.cropin.in/services/farm/api/plans/non-pops/ca"
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\API_Plan_Template.xlsx"  # Replace with your input Excel file name
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\API_Plan_Template_output.xlsx"  # Output Excel file to store the results
    tenant_code = "asp"
    # Retrieve access token
    print("Retrieving access token")
    token = get_access_token(tenant_code, "9649964096", "123456")

    if token:
        print("Access token retrieved successfully")
        post_data_to_api(get_api_url, put_api_url, token, input_excel, output_excel)
    else:
        print("Failed to retrieve access token. Process terminated.")