# Author: Rajasekhar Palleti

import json
import requests
import pandas as pd
import time
import threading
from GetAuthtoken import get_access_token

lock = threading.Lock()  # To avoid race conditions when writing to DataFrame

def process_rows(df, start_idx, end_idx, api_url, token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    for index in range(start_idx, end_idx):
        row = df.iloc[index]
        CA_id = row.iloc[0]
        sowing_Date = row.iloc[1]

        if pd.isna(CA_id) or pd.isna(sowing_Date):
            with lock:
                df.at[index, "Status"] = "Skipped: Missing Data"
            continue

        try:
            get_response = requests.get(f"{api_url}/{CA_id}", headers=headers)
            if get_response.status_code != 200:
                with lock:
                    df.at[index, "Status"] = f"GET Failed: {get_response.status_code}"
                print(f"GET failed for CA_ID: {CA_id} — Status Code: {get_response.status_code}")
                continue

            CA_data = get_response.json()
            print(f"\nThread {threading.current_thread().name} — Row {index + 2} — CA_ID: {CA_id}")

            if "sowingDate" not in CA_data:
                print("sowingDate key missing. Adding...")
                CA_data["sowingDate"] = None

            CA_data["sowingDate"] = sowing_Date

            time.sleep(0.4)

            put_response = requests.put(f"{api_url}", headers=headers, data=json.dumps(CA_data))
            with lock:
                if put_response.status_code != 200:
                    df.at[index, "Status"] = f"PUT Failed: {put_response.status_code}"
                    df.at[index, "CA_Response"] = put_response.text
                    print(f"PUT failed for CA_ID: {CA_id} — Status Code: {put_response.status_code}")
                    continue

                df.at[index, "Status"] = "Success"
                df.at[index, "CA_Response"] = put_response.text
                print(f"Successfully updated CA_ID: {CA_id}")

        except requests.exceptions.RequestException as e:
            with lock:
                df.at[index, "Status"] = f"Failed: {str(e)}"
            print(f"Update failed for CA_ID: {CA_id} — {e}")

        time.sleep(0.4)


def post_data_to_api_multithreaded(api_url, token, input_excel, output_excel, sheet_name):
    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    if "Status" not in df.columns:
        df["Status"] = ""
    if "CA_Response" not in df.columns:
        df["CA_Response"] = ""

    total_rows = len(df)
    num_threads = 4
    chunk_size = (total_rows + num_threads - 1) // num_threads

    threads = []

    for i in range(num_threads):
        start_idx = i * chunk_size
        end_idx = min(start_idx + chunk_size, total_rows)
        t = threading.Thread(
            target=process_rows,
            args=(df, start_idx, end_idx, api_url, token),
            name=f"Worker-{i+1}"
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\nExcel file updated and saved as {output_excel}")


if __name__ == "__main__":
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\7151.xlsx"
    sheet_name = "result"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\7151_output.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/croppable-areas"

    print("Retrieving access token...")
    token = get_access_token("walmart", "12345012345", "Cropin@123", "prod1")

    if token:
        print("Access token retrieved successfully.")
        post_data_to_api_multithreaded(api_url, token, input_excel, output_excel, sheet_name)
    else:
        print("Failed to retrieve access token. Process terminated.")
