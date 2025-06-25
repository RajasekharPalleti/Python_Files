import requests
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import ast
from GetAuthtoken import get_access_token

def process_chunk(df_chunk, api_url, token, thread_id):
    headers = {
        "Authorization": f"Bearer {token}"
    }
    results = []

    for index, row in df_chunk.iterrows():
        print(f"[Thread {thread_id}] Processing row index: {index}")

        ca_id = row.iloc[0]  # Column A: CA ID
        raw_tags = row.iloc[2]  # Example: " [1, 2, 3] "

        # Convert string to actual list
        if isinstance(raw_tags, str):
            ca_tags = ast.literal_eval(raw_tags.strip())  # Result: [1, 2, 3]
        else:
            ca_tags = raw_tags  # If already a list, just use it

        status = ""
        response_str = ""

        if pd.isna(ca_id):
            status = "Skipped: Missing Data"
            results.append((index, status, response_str))
            continue

        try:
            print(f"[Thread {thread_id}] Getting asset for: {ca_id}")
            get_response = requests.get(f"{api_url}/{ca_id}", headers=headers)
            get_response.raise_for_status()
            ca_data = get_response.json()

            if "data" in ca_data and isinstance(ca_data["data"], dict):
                ca_data["data"]["tags"] = ca_tags
                print(f"[Thread {thread_id}] Modified data for: {ca_id}")
            else:
                status = "Failed: No data"
                results.append((index, status, response_str))
                continue

            time.sleep(0.2)

            # multipart_data = {
            #     "dto": (None, json.dumps(asset_data), "application/json")
            # }

            put_response = requests.put(api_url, headers=headers, json=ca_data)
            put_response.raise_for_status()

            status = "Success"
            response_str = put_response.text[:500]  # Limit long responses

            print(f"[Thread {thread_id}] Updated: {ca_id}")

        except requests.exceptions.RequestException as e:
            status = f"Failed: {str(e)}"
            response_str = str(e)
            print(f"[Thread {thread_id}] Failed for: {ca_id}")

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
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\QAZONE5 CA list.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\3918_2025_06_18_updated.xlsx"
    api_url = "https://af-v2-gcp.cropin.co.in/qa5/services/farm/api/croppable-areas"

    print("Retrieving token...")
    # token = get_access_token(tenant_code, "9120232024", "Anilkumarkolla", "prod1")
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJDeVNZVFNPWW95Q19DcENmaTNNZHBTdHFvd0VPczFBS0ZSZE1YMXVVYkgwIn0.eyJleHAiOjE3NTA2NzgyMDksImlhdCI6MTc1MDY1NjYwOSwiYXV0aF90aW1lIjoxNzUwNjU2NjA5LCJqdGkiOiJkNzdmMTI0Yi04NDA1LTRlZWItYjg3Mi1kM2RiZTVmMmJmODUiLCJpc3MiOiJodHRwczovL3Yyc3NvLWdjcC5jcm9waW4uY28uaW4vYXV0aC9yZWFsbXMvcWF6b25lNSIsImF1ZCI6WyJyZXNvdXJjZV9zZXJ2ZXIiLCJhY2NvdW50Il0sInN1YiI6IjM5OWQ3MWEyLWVjZGUtNGZiMi04ZDhmLTY1YjY2MDEwZDM1YSIsInR5cCI6IkJlYXJlciIsImF6cCI6IndlYl9hcHAiLCJub25jZSI6Il9jbGllbnRfd2ViX2FwcCIsInNlc3Npb25fc3RhdGUiOiIxNjljMGIzYi1mYjI0LTRkNDAtYWZjNS01YWQwOWUzMTE1NzgiLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJyZXNvdXJjZV9zZXJ2ZXIiOnsicm9sZXMiOlsiUmFqYSBUZXN0IHVzZXIgcm9sZV8zODUxIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBlbWFpbCBwcm9maWxlIG9mZmxpbmVfYWNjZXNzIHBob25lIGFkZHJlc3MgbWljcm9wcm9maWxlLWp3dCIsInNpZCI6IjE2OWMwYjNiLWZiMjQtNGQ0MC1hZmM1LTVhZDA5ZTMxMTU3OCIsInVzZXJfcm9sZSI6WyJSYWphIFRlc3QgdXNlciByb2xlXzM4NTEiXSwidXBuIjoiNzM4ODg4ODg4OCIsImFkZHJlc3MiOnt9LCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImdyb3VwcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl0sInByZWZlcnJlZF91c2VybmFtZSI6IjczODg4ODg4ODgiLCJ0ZW5hbnQiOiJxYXpvbmU1IiwiZW1haWwiOiJyYWphdGVzdHFhNUBnbWFpbC5jb20ifQ.CNSJVSAxEFrwpC2_GsHvr3Ba9Xn_ypGy0UCER9Ba15cTgkVlje_EKeqvelB7QAgFpCK9im1DUsimGFud-uGPQdV4X-SxM33fJdI3UPG9O8nVo3mFBUs0RZ7nMPNUSY3p7iXeAUZwf-RSKW4FBjSOgCwFNUz3qswMQd4OgvTikJdO9toW8uwPKoOjRPZY0uqhlnG4fclaWAtgYSJGnMyGIfz5ez8oGONMYkxl8AWlnCU7-Be4PDsg6Wd2sNL76Sz173iQ93ViVUgM2rlJSppCOLqc8kck9FJEuO2GR8w2qgih7PbMwoYs7purB5aZS7kpG0CdSOqUZjx1z2SzH-MHAQ"

    if token:
        print("Token retrieved successfully.")
        post_data_with_multithreading(api_url, token, input_excel, output_excel)
    else:
        print("Token retrieval failed.")
