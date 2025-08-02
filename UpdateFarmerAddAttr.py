import json
import requests
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from GetAuthtoken import get_access_token

def process_chunk(df_chunk, api_url, token, thread_id):
    headers = {
        "Authorization": f"Bearer {token}"
    }
    results = []

    for index, row in df_chunk.iterrows():
        print(f"[Thread {thread_id}] Processing row index: {index}")

        farmer_id = row[0]  # Column A: Farmer ID
        addlattr_name = row[3]  # Column D: Additional attribute name


        status = ""
        response_str = ""

        if pd.isna(farmer_id) or pd.isna(addlattr_name):
            status = "Skipped: Missing Data"
            results.append((index, status, response_str))
            continue

        try:
            print(f"[Thread {thread_id}] Getting asset for: {farmer_id}")
            get_response = requests.get(f"{api_url}/{farmer_id}", headers=headers)
            get_response.raise_for_status()
            farmer_data = get_response.json()

            if "data" in farmer_data and isinstance(farmer_data["data"], dict):
                farmer_data["data"]["farmerProject"] = addlattr_name
                print(f"[Thread {thread_id}] Modified data for: {farmer_id}")
            else:
                status = "Failed: No data"
                results.append((index, status, response_str))
                continue

            time.sleep(0.2)

            multipart_data = {
                "dto": (None, json.dumps(farmer_data), "application/json")
            }

            put_response = requests.put(api_url, headers=headers, files=multipart_data)
            put_response.raise_for_status()

            status = "Success"
            response_str = put_response.text[:300]  # Limit long responses

            print(f"[Thread {thread_id}] Updated: {farmer_id}")

        except requests.exceptions.RequestException as e:
            status = f"Failed: {str(e)}"
            response_str = str(e)
            print(f"[Thread {thread_id}] Failed for: {farmer_id}")

        time.sleep(0.2)
        results.append((index, status, response_str))

    return results

def post_data_with_multithreading(api_url, token, input_excel, output_excel):
    df = pd.read_excel(input_excel)
    df["Status"] = ""
    df["Response"] = ""

    # Split the DataFrame into two chunks using iloc[]
    mid_index = len(df) // 2
    chunk1 = df.iloc[:mid_index]
    chunk2 = df.iloc[mid_index:]

    # Use ThreadPoolExecutor to run 2 threads
    with ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(process_chunk, chunk1, api_url, token, 1)
        future2 = executor.submit(process_chunk, chunk2, api_url, token, 2)

        results = future1.result() + future2.result()

    # Update the original dataframe with the results
    for idx, status, response in results:
        df.at[idx, "Status"] = status
        df.at[idx, "Response"] = response

    # Save the updated DataFrame
    df.to_excel(output_excel, index=False)
    print(f"Updated Excel saved at: {output_excel}")

if __name__ == "__main__":
    tenant_code = "gpi"
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\Farmer Data_Project GPI 3.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\Farmer Data_Project GPI_Updated 3.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/farmers"

    print("Retrieving token...")
    token = get_access_token(tenant_code, "9120232024", "Anilkumarkolla", "prod1")

    if token:
        print("Token retrieved successfully.")
        post_data_with_multithreading(api_url, token, input_excel, output_excel)
    else:
        print("Token retrieval failed.")
