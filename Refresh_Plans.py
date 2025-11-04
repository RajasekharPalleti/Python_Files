# Author: Rajasekhar Palleti
# ca_id is read from an Excel file. Each ca_id is processed using GET and PUT APIs with retries.
# The results are saved back to a new Excel file.

import pandas as pd
import requests
import json
import time
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from GetAuthtoken import get_access_token

MAX_RETRIES = 1  # Number of retries for GET/PUT
TIME_OUT = 5  # seconds


def request_with_retry(method, url, headers=None, data=None, timeout=TIME_OUT, retries=MAX_RETRIES):
    """
    Generic request with retry mechanism.
    """
    for attempt in range(1, retries + 1):
        try:
            if method.upper() == "GET":
                resp = requests.get(url, headers=headers, timeout=timeout)
            elif method.upper() == "PUT":
                resp = requests.put(url, headers=headers, data=data, timeout=timeout)
            else:
                raise ValueError("Unsupported HTTP method")

            if resp.status_code in [200, 201]:
                return resp
            else:
                print(f"⚠️ Attempt {attempt} failed for URL: {url} | Status Code: {resp.status_code}")
        except Exception as e:
            print(f"⚠️ Attempt {attempt} exception for URL: {url} | {e}")
        time.sleep(1)  # wait 1 second before retry
    return None


def process_croppable_area(ca_id, token, row_number=None):
    """
    Fetch croppable area data using GET API and send it to the PUT API.
    Accepts an optional row_number to indicate which Excel row is being processed.
    Returns a dict with CA_id, row_number, status, response, and timestamps.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_ts = time.time()

    # Print the Excel row number (1-based) if provided
    print(f"🟢 Started processing CA_id: {ca_id} at {start_time} | Row: {row_number}")

    try:
        # Step 1️⃣: GET croppable area data with retries
        get_url = f"{GET_URL_TEMPLATE}{ca_id}"
        get_resp = request_with_retry("GET", get_url, headers=headers)

        if not get_resp:
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            duration = round(time.time() - start_ts, 2)
            print(f"🔴 Completed CA_id: {ca_id} | Status: Failed | Duration: {duration}s\n")
            return {
                "ca_id": ca_id,
                "row_number": row_number,
                "status": "Failed",
                "response": f"GET failed after {MAX_RETRIES} attempts",
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration
            }

        ca_data = get_resp.json()

        # Step 2️⃣: Wait before PUT call
        time.sleep(WAIT_TIME)

        print(f"🟡 Sending PUT request for CA_id: {ca_id}...")

        # Step 3️⃣: PUT API call with retries
        put_resp = request_with_retry("PUT", PUT_URL, headers=headers, data=json.dumps(ca_data))

        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = round((time.time()-WAIT_TIME) - start_ts, 2)

        if put_resp:
            print(f"✅ Completed CA_id: {ca_id} | Status: Success | Duration: {duration}s\n")
            return {
                "ca_id": ca_id,
                "row_number": row_number,
                "status": "Success",
                "response": put_resp.text,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration
            }
        else:
            print(f"🔴 Completed CA_id: {ca_id} | Status: Failed | Duration: {duration}s\n")
            return {
                "ca_id": ca_id,
                "row_number": row_number,
                "status": "Failed",
                "response": f"PUT failed after {MAX_RETRIES} attempts",
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration
            }

    except Exception as e:
        traceback.print_exc()
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = round((time.time()-WAIT_TIME) - start_ts, 2)
        print(f"⚠️ Error processing CA_id: {ca_id} | Duration: {duration}s\n")
        return {
            "ca_id": ca_id,
            "row_number": row_number,
            "status": "Failed",
            "response": str(e),
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration
        }


def post_croppable_area_data(excel_file, sheet_name, token):

    print("📘 Loading Excel file...")
    df = pd.read_excel(excel_file, sheet_name=sheet_name)

    if "ca_id" not in df.columns:
        raise Exception("❌ 'ca_id' column not found in Excel sheet!")

    print(f"🔄 Starting with {len(df)} croppable areas using {MAX_WORKERS} workers...\n")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Pass the Excel row number (1-based) to the worker so it can print it.
        futures = {
            executor.submit(process_croppable_area, ca_id, token, idx): ca_id
            for idx, ca_id in enumerate(df["ca_id"], start=1)
        }

        for future in as_completed(futures):
            result = future.result()
            results.append(result)

    # Merge results and save
    results_df = pd.DataFrame(results)
    df = df.merge(results_df, how="left", on="ca_id")

    output_file = excel_file.replace(".xlsx", "_output.xlsx")
    df.to_excel(output_file, index=False)
    print(f"\n📁 Execution complete. Output saved to: {output_file}")


# ================== CONSTANTS ==================
EXCEL_FILE = r"C:\Users\rajasekhar.palleti\Downloads\21601 Project CA details.xlsx"
SHEET_NAME = "Sheet4"

GET_URL_TEMPLATE = "https://cloud.cropin.in/services/farm/api/croppable-areas/"
PUT_URL = "https://cloud.cropin.in/services/farm/api/croppablearea/tasks?sort=lastModifiedDate,desc"

WAIT_TIME = 1  # seconds between PUT calls
MAX_WORKERS = 4  # number of threads
# =================================================

if __name__ == "__main__":
    print("🔐 Fetching access token...")
    token = get_access_token("gbtogo", "m.blaser@gebana.com", "Cropin@123", "prod1")

    if token:
        print("✅ Token acquired. Proceeding with execution...\n")
        post_croppable_area_data(EXCEL_FILE, SHEET_NAME, token)
    else:
        print("❌ Token not available. Stopping execution.")
