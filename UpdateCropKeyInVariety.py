#Author: Rajasekhar Palleti

import requests
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from GetAuthtoken import get_access_token


def process_variety_chunk(df_chunk, api_url, token, thread_id):
    headers = {
        "Authorization": f"Bearer {token}"
    }
    results = []

    for index, row in df_chunk.iterrows():
        print(f"[Thread {thread_id}] Processing row index: {index}")

        variety_id = row.iloc[0]  # Column 1: variety_id
        crop_key = row.iloc[3]  # Column D: crop_key to update

        status = ""
        response_str = ""

        if pd.isna(variety_id) or pd.isna(crop_key):
            status = "Skipped: Missing Data"
            results.append((index, status, response_str))
            continue

        try:
            print(f"[Thread {thread_id}] Getting variety for: {variety_id}")
            get_response = requests.get(f"{api_url}/{variety_id}", headers=headers)
            get_response.raise_for_status()
            variety_data = get_response.json()

            if "cropKey" in variety_data:
                variety_data["cropKey"] = crop_key
                print(f"[Thread {thread_id}] Modified data for: {variety_id}")
            else:
                status = "Failed: No data"
                results.append((index, status, response_str))
                continue

            time.sleep(0.2)

            put_response = requests.put(api_url, headers=headers, json=variety_data)
            put_response.raise_for_status()

            status = "Success"
            response_str = put_response.text

            print(f"[Thread {thread_id}] Updated: {variety_id}")

        except requests.exceptions.RequestException as e:
            status = f"Failed: {str(e)}"
            response_str = str(e)
            print(f"[Thread {thread_id}] Failed for: {variety_id}")

        time.sleep(0.2)
        results.append((index, status, response_str))

    return results

def update_crop_key_with_multithreading(api_url, token, input_excel, output_excel):
    df = pd.read_excel(input_excel).iloc[:2] # For testing, limit to first 2 rows
    # df = pd.read_excel(input_excel)
    df["Status"] = ""
    df["Response"] = ""

    # Split the DataFrame into two chunks
    mid_index = len(df) // 2
    chunk1 = df.iloc[:mid_index]
    chunk2 = df.iloc[mid_index:]

    with ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(process_variety_chunk, chunk1, api_url, token, 1)
        future2 = executor.submit(process_variety_chunk, chunk2, api_url, token, 2)

        results = future1.result() + future2.result()

    for idx, status, response in results:
        df.at[idx, "Status"] = status
        df.at[idx, "Response"] = response

    df.to_excel(output_excel, index=False)
    print(f"Updated Excel saved at: {output_excel}")

if __name__ == "__main__":
    input_excel = r"C:\Users\rajasekhar.palleti\Downloads\Crop Key updation.xlsx"
    output_excel = r"C:\Users\rajasekhar.palleti\Downloads\Crop Key updation_output.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/varieties"

    print("Retrieving token...")
    token = get_access_token("ews1", "philius.trinidad.vn@eastwestseed.com", "3wsGrm@0123!", "prod1")

    if token:
        print("Token retrieved successfully.")
        update_crop_key_with_multithreading(api_url, token, input_excel, output_excel)
    else:
        print("Token retrieval failed.")
