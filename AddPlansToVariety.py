# Author: Rajasekhar Palleti
# QA Automation Script to Fetch Plan Type and Submit Plan Details via API

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
def post_data_to_api(post_api_url, put_api_url, token, input_file, output_file, dto=None):
    print(f"\n[INFO] Loading data from Excel: {input_file}")
    exdata = pd.read_excel(input_file)

    print("[INFO] Cleaning data: Replacing NaN values with empty strings")
    exdata = exdata.fillna("")

    print("[INFO] Adding status tracking columns")
    exdata['status'] = ""
    exdata['Response'] = ''

    print(f"\n[INFO] Starting to process {len(exdata)} rows from the Excel file")

    # Iterate over each row
    for index, row in exdata.iterrows():
        print(f"\n[ROW {index + 1}] Processing row")

        try:
            # Extracting values from each column
            croppable_area_id = row.iloc[0]
            plan_name = row.iloc[1]
            plantype_id = row.iloc[2]
            # plan_type_name = row.iloc[3]
            # project_id = row.iloc[4]
            varieties = row.iloc[5]
            schedule_type = row.iloc[6]
            no_of_days = safe_int(row.iloc[7])
            execute_when = row.iloc[8]
            reference_date = row.iloc[9]
            required_days = safe_int(row.iloc[10])

            print(f"[ROW {index + 1}] Sending GET request for Plan Type ID: {plantype_id}")
            get_url = f"{get_api_url}/{plantype_id}"
            headers = {"Authorization": f"Bearer {token}"}
            get_response = requests.get(get_url, headers=headers)

            if get_response.status_code == 200:
                print(f"[ROW {index + 1}] GET request successful.")
                plantype_response = get_response.json()
            else:
                print(f"[ROW {index + 1}] GET request failed with status: {get_response.status_code}")
                exdata.at[index, 'status'] = f"Failed GET: {get_response.status_code}"
                continue

            # Preparing POST payload
            print(f"[ROW {index + 1}] Preparing payload for POST request")
            post_payload = {
                "data": {
                    "information": {
                        "planName": plan_name,
                        "planType": plantype_response,
                        "geoLocation": False,
                        "signature": False
                    },
                    "conditions": {},
                    "customAttributes": {},
                    "planHeaderAttributes": [],
                    "planHeaderGroup": {},
                    "conditionData": {
                        "data": {},
                        "conditionParams": varieties
                    }
                },
                "images": {},
                "varieties": [
                    varieties
                ],
                "name": plan_name,
                "planTypeId": plantype_id,
                "schedule": {
                    "type": schedule_type,
                    "fixedDate": False,
                    "noOfDays": no_of_days,
                    "executeWhen": execute_when,
                    "referenceDate": reference_date,
                    "requiredDays": required_days,
                    "recuring": False
                }
            }

            multipart_data = {
                "dto": (None, json.dumps(post_payload), "application/json")
            }

            print(f"[ROW {index + 1}] Sending POST request to {put_api_url}/{croppable_area_id}")
            post_response = requests.post(
                f"{post_api_url}",
                headers=headers,
                files=multipart_data
            )

            if post_response.status_code == 200:
                print(f"[ROW {index + 1}] POST request successful")
                exdata.at[index, 'status'] = "Success"
                exdata.at[index, 'Response'] = f"Code: {post_response.status_code}, Message: {post_response.text}"
            else:
                print(f"[ROW {index + 1}] POST request failed, Status: {post_response.status_code}")
                exdata.at[index, 'status'] = f"Failed POST: {post_response.status_code}"
                exdata.at[index, 'Response'] = f"Reason: {post_response.reason}, Message: {post_response.text}"

            print(f"[ROW {index + 1}] Waiting for 0.3 seconds before continuing...")
            time.sleep(0.3)

        except Exception as e:
            print(f"[ROW {index + 1}] ❌ Error: {str(e)}")
            exdata.at[index, 'status'] = f"Error: {str(e)}"

    print(f"\n[INFO] Saving results to output Excel: {output_file}")
    exdata.to_excel(output_file, index=False)
    print("[INFO] Process completed successfully.")


# Main program
if __name__ == "__main__":
    print("\n========== PLAN API AUTOMATION SCRIPT STARTED ==========")
    get_api_url = "https://cloud.cropin.in/services/farm/api/plan-types"
    post_api_url = "https://cloud.cropin.in/services/farm/api/plans"
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\API_Plan_Template.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\API_Plan_Template_output.xlsx"
    tenant_code = "asp"

    print("[INFO] Requesting access token for tenant:", tenant_code)
    token = get_access_token(tenant_code, "9649964096", "123456", "prod1")

    if token:
        print("[INFO] Access token retrieved successfully ✅")
        post_data_to_api(get_api_url, post_api_url, token, input_excel, output_excel)
    else:
        print("[ERROR] Failed to retrieve access token. ❌ Process terminated.")

    print("========== SCRIPT FINISHED ==========\n")
