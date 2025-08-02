import json
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

        asset_id = row.iloc[0]  # Column A: Asset ID
        raw_tags = row.iloc[2]  # Example: " [1, 2, 3] "

        # Convert string to actual list
        if isinstance(raw_tags, str):
            asset_tags = ast.literal_eval(raw_tags.strip())  # Result: [1, 2, 3]
        else:
            asset_tags = raw_tags  # If already a list, just use it

        status = ""
        response_str = ""

        if pd.isna(asset_id):
            status = "Skipped: Missing Data"
            results.append((index, status, response_str))
            continue

        try:
            print(f"[Thread {thread_id}] Getting asset for: {asset_id}")
            get_response = requests.get(f"{api_url}/{asset_id}", headers=headers)
            get_response.raise_for_status()
            asset_data = get_response.json()

            if "data" in asset_data and isinstance(asset_data["data"], dict):
                asset_data["data"]["tags"] = asset_tags
                print(f"[Thread {thread_id}] Modified data for: {asset_id}")
            else:
                status = "Failed: No data"
                results.append((index, status, response_str))
                continue

            time.sleep(0.2)

            multipart_data = {
                "dto": (None, json.dumps(asset_data), "application/json")
            }

            put_response = requests.put(api_url, headers=headers, files=multipart_data)
            put_response.raise_for_status()

            status = "Success"
            response_str = put_response.text[:500]  # Limit long responses

            print(f"[Thread {thread_id}] Updated: {asset_id}")

        except requests.exceptions.RequestException as e:
            status = f"Failed: {str(e)}"
            response_str = str(e)
            print(f"[Thread {thread_id}] Failed for: {asset_id}")

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
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\UATZONE1 assets.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\UATZONE1 assets_updated.xlsx"
    api_url = "https://v2uat-gcp.cropin.co.in/uat1/services/farm/api/assets"

    print("Retrieving token...")
    # token = get_access_token(tenant_code, "9120232024", "Anilkumarkolla", "prod1")
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJaS08wSFZ2OGlVVmxzQTl6THFBUjhEOWVMc3NYNlVYTERWRkUzdkJ1N0lJIn0.eyJqdGkiOiI2MDVjNTc2OS05ODFkLTQ4NjQtYmU5MC03NTg0MjdhOGQ4MTYiLCJleHAiOjE3NTE0NjQzMjMsIm5iZiI6MCwiaWF0IjoxNzUxMjkxNTIzLCJpc3MiOiJodHRwczovL3Yyc3NvLXVhdC1nY3AuY3JvcGluLmNvLmluL2F1dGgvcmVhbG1zL3VhdHpvbmUxIiwiYXVkIjpbInJlc291cmNlX3NlcnZlciIsImFjY291bnQiXSwic3ViIjoiMTZlYjliYWQtY2Y1OS00N2Y0LWI2YzAtYjdhNTAxNjEzYjhlIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoid2ViX2FwcCIsIm5vbmNlIjoiX2NsaWVudF93ZWJfYXBwIiwiYXV0aF90aW1lIjoxNzUxMjkxNTIzLCJzZXNzaW9uX3N0YXRlIjoiMjFjZjVjNWItYWI2Yi00ZDU2LTlhMjAtYWQ4Y2U2NTliMmY2IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsicmVzb3VyY2Vfc2VydmVyIjp7InJvbGVzIjpbIlJhamEgUm9sZV9BZG1pbiByb2xlXzM2MjYwMSJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgbWljcm9wcm9maWxlLWp3dCBwaG9uZSBwcm9maWxlIG9mZmxpbmVfYWNjZXNzIGVtYWlsIGFkZHJlc3MiLCJ1c2VyX3JvbGUiOlsiUmFqYSBSb2xlX0FkbWluIHJvbGVfMzYyNjAxIl0sInVwbiI6IjczODIyMTEzMDIiLCJhZGRyZXNzIjp7fSwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJncm91cHMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiI3MzgyMjExMzAyIiwidGVuYW50IjoidWF0em9uZTEiLCJlbWFpbCI6InJhamFzZWtoYXIucGFsbGV0aTFAY3JvcGluLmNvbSJ9.AGkI0ty_NkNLJ7m65t4TTbrtoiuy3RF2egRCA2ZMViKvixcDjYqHiSYaFUYq8C87wEQH0a0QWJN2WySrLuJDq01rg3s2WnTGgErh42y2ym93UY4oNBC5xNSpXV12j6SbUsoF3Q8xNuZceoho9cXAj8UNBsCYtDAt6TS2WR8cGl4a8YJowMfwClqsPZDS93p3UtYIW64HT-TA8pwfz0fjw7dQSq0dmeImq8hqK9tjZNYcZR0Va_qzy1DtihNxOWybKSJkZMpNJ4Dm5CTCNDJa4-rJr1sd3fJ_1SX9YmXA7ONC7RSo3QU959cc-oJj-6JW8pLHuEAZKb7yjcibJ-y1Ng"

    if token:
        print("Token retrieved successfully.")
        post_data_with_multithreading(api_url, token, input_excel, output_excel)
    else:
        print("Token retrieval failed.")
