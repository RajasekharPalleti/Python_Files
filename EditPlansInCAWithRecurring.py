# Author: Rajasekhar Palleti
# QA Automation Script to Fetch Plan Type and Submit Plan Details via API (Refactored)

import json
import pandas as pd
import requests
import time
from GetAuthtoken import get_access_token


# --- Utility functions ---
def safe_int(value):
    """Convert value to int safely"""
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def safe_bool(value):
    """Convert Excel values to boolean"""
    if str(value).strip().lower() in ["true", "1", "yes", "y"]:
        return True
    return False


# --- Core function ---
def put_data_to_api(api_url, token, input_file, output_file):
    print(f"\n[INFO] Loading data from Excel: {input_file}")
    exdata = pd.read_excel(input_file)

    print("[INFO] Cleaning data: Replacing NaN values with empty strings")
    exdata = exdata.fillna("")

    print("[INFO] Adding status tracking columns")
    exdata['status'] = ""
    exdata['Response'] = ''

    print(f"\n[INFO] Starting to process {len(exdata)} rows from the Excel file")

    for index, row in exdata.iterrows():
        index : int
        print(f"\n[ROW {index + 1}] Processing row")

        try:
            # --- Extract fields from Excel ---
            plan_id = row["plan_id"]
            plan_name = row["plan_name"]
            plantype_id = row["plantype_id"]
            schedule_type = row["schedule_type"]
            no_of_days = safe_int(row["no_of_days"])
            execute_when = row["execute_when"]
            reference_date = row["reference_date"]
            required_days = safe_int(row["required_days"])
            recuring = safe_bool(row["recuring"])
            repeat_after = safe_int(row["repeat_after"])
            timePeriod = row["timePeriod"]
            hasRecuringEndDate = safe_bool(row["hasRecuringEndDate"])
            recuringEndDate = row["recuringEndDate"]
            recNoOfDays = safe_int(row["recNoOfDays"])
            recExecuteWhen = row["recExecuteWhen"]
            recReferenceDate = row["recReferenceDate"]

            # --- GET existing plan ---
            get_url = f"{api_url}/{plan_id}"
            headers = {"Authorization": f"Bearer {token}"}
            print(f"[ROW {index + 1}] Sending GET request: {get_url}")

            get_response = requests.get(get_url, headers=headers)

            if get_response.status_code == 200:
                try:
                    plan_response = get_response.json()
                except ValueError:
                    print(f"[ROW {index + 1}] ❌ Invalid JSON in GET response")
                    exdata.at[index, 'status'] = "Invalid JSON"
                    continue
                print(f"[ROW {index + 1}] GET request successful")
            else:
                print(f"[ROW {index + 1}] ❌ GET failed with status: {get_response.status_code}")
                exdata.at[index, 'status'] = f"Failed GET: {get_response.status_code}"
                continue

            # "schedule": {
            #     "clientId": "8445d3af-5985-4f9b-b3cf-527a8528bb8a",
            #     "id": 3554918,
            #     "data": null,
            #     "type": "Scheduled",
            #     "fixedExecutionDates": [],
            #     "fixedDate": false,
            #     "fixedExecutionDate": null,
            #     "noOfDays": 5,
            #     "executeWhen": "Before",
            #     "referenceDate": "sowing",
            #     "referencePlanId": null,
            #     "requiredDays": 30,
            #     "recuring": true,
            #     "repeats": 3,
            #     "timePeriod": "MONTH",
            #     "hasRecuringEndDate": false,
            #     "recuringEndDate": null,
            #     "recNoOfDays": 3,
            #     "recExecuteWhen": "Before",
            #     "recReferenceDate": "harvest",
            #     "deleted": false
            # }

            # --- Update schedule fields ---
            if "schedule" not in plan_response:
                plan_response["schedule"] = {}

            plan_response["schedule"]["type"] = schedule_type
            plan_response["schedule"]["noOfDays"] = no_of_days
            plan_response["schedule"]["executeWhen"] = execute_when
            plan_response["schedule"]["referenceDate"] = reference_date
            plan_response["schedule"]["requiredDays"] = required_days
            plan_response["schedule"]["recuring"] = recuring
            plan_response["schedule"]["repeats"] = repeat_after
            plan_response["schedule"]["timePeriod"] = timePeriod
            plan_response["schedule"]["hasRecuringEndDate"] = hasRecuringEndDate
            plan_response["schedule"]["recuringEndDate"] = recuringEndDate
            plan_response["schedule"]["recNoOfDays"] = recNoOfDays
            plan_response["schedule"]["recExecuteWhen"] = recExecuteWhen
            plan_response["schedule"]["recReferenceDate"] = recReferenceDate

            # --- PUT request to update ---
            multipart_data = {
                "dto": (None, json.dumps(plan_response), "application/json")
            }

            put_url = f"{api_url}/{plan_id}"
            print(f"[ROW {index + 1}] Sending PUT request: {put_url}")

            put_response = requests.put(
                put_url,
                headers=headers,
                files=multipart_data
            )

            if put_response.status_code in [200, 201]:
                print(f"[ROW {index + 1}] ✅ PUT successful")
                exdata.at[index, 'status'] = "Success"
                exdata.at[index, 'Response'] = f"Code: {put_response.status_code}, Message: {put_response.text}"
            else:
                print(f"[ROW {index + 1}] ❌ PUT failed, Status: {put_response.status_code}")
                exdata.at[index, 'status'] = f"Failed PUT: {put_response.status_code}"
                exdata.at[index, 'Response'] = f"Reason: {put_response.reason}, Message: {put_response.text}"

            time.sleep(0.3)  # Throttle API calls

        except Exception as e:
            print(f"[ROW {index + 1}] ❌ Error: {str(e)}")
            exdata.at[index, 'status'] = f"Error: {str(e)}"

    print(f"\n[INFO] Saving results to output Excel: {output_file}")
    exdata.to_excel(output_file, index=False)
    print("[INFO] Process completed successfully ✅")


# --- Main program ---
if __name__ == "__main__":
    print("\n========== PLAN API AUTOMATION SCRIPT STARTED ==========")
    api_url = "https://cloud.cropin.in/services/farm/api/plans"
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\API_Plan_Template.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\API_Plan_Template_output.xlsx"
    tenant_code = "asp"

    print("[INFO] Requesting access token for tenant:", tenant_code)
    token = get_access_token(tenant_code, "9649964096", "123456", "prod1")

    if token:
        print("[INFO] Access token retrieved successfully ✅")
        put_data_to_api(api_url, token, input_excel, output_excel)
    else:
        print("[ERROR] Failed to retrieve access token. ❌ Process terminated.")

    print("========== SCRIPT FINISHED ==========\n")
