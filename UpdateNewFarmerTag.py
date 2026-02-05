import json
import requests
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from GetAuthtoken import get_access_token

def parse_comma_ids(cell):
    """
    Accept input formats like:
      "121,122" or "121"
    Returns list of ints. Empty list for NaN/empty.
    """
    if pd.isna(cell):
        return []
    s = str(cell).strip()
    if s == "":
        return []
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    ids = []
    for p in parts:
        try:
            ids.append(int(p))
        except ValueError:
            # ignore non-integer parts
            continue
    return ids

def process_chunk(df_chunk, api_url, token, thread_id, timeout=30):
    headers = {"Authorization": f"Bearer {token}"}
    results = []

    for index, row in df_chunk.iterrows():
        print(f"[Thread {thread_id}] Processing row index: {index}")
        farmer_id = row.iloc[0]   # Column A: Farmer ID
        raw_tags_cell = row.iloc[2]  # Column C : Farmer Tag IDs

        status = ""
        response_str = ""

        if pd.isna(farmer_id):
            status = "Skipped: Missing Farmer ID"
            results.append((index, status, response_str))
            continue

        new_ids = parse_comma_ids(raw_tags_cell)
        if not new_ids:
            status = "Skipped: No tag IDs to add"
            results.append((index, status, response_str))
            continue

        try:
            # GET existing farmer
            get_resp = requests.get(f"{api_url}/{farmer_id}", headers=headers, timeout=timeout)
            get_resp.raise_for_status()
            farmer_json = get_resp.json()

            data = farmer_json.get("data", {})
            existing_tags = data.get("tags", []) or []
            # normalize existing tags to ints when possible
            existing_ints = []
            for t in existing_tags:
                try:
                    existing_ints.append(int(t))
                except Exception:
                    # ignore non-integer existing tags
                    continue

            # determine which IDs to actually add (skip those already present)
            to_add = [i for i in new_ids if i not in existing_ints]
            if not to_add:
                status = "Skipped: All IDs already present"
                results.append((index, status, response_str))
                continue

            # update data["tags"] by appending new ints
            updated_tags = existing_ints + to_add
            data["tags"] = updated_tags
            farmer_json["data"] = data

            # prepare multipart dto and PUT to specific farmer URL
            multipart = {"dto": (None, json.dumps(farmer_json), "application/json")}
            put_resp = requests.put(f"{api_url}", headers=headers, files=multipart, timeout=timeout)
            put_resp.raise_for_status()

            status = "Success"
            response_str = put_resp.text[:500]

            print(f"[Thread {thread_id}] Farmer {farmer_id} updated, added IDs: {to_add}")

        except requests.exceptions.RequestException as e:
            status = f"Failed: {str(e)}"
            response_str = str(e)
            print(f"[Thread {thread_id}] Request error for farmer {farmer_id}: {e}")

        time.sleep(1)
        results.append((index, status, response_str))

    return results

def post_data_with_multithreading(api_url, token, input_excel, output_excel):
    df = pd.read_excel(input_excel)
    if "Status" not in df.columns:
        df["Status"] = ""
    if "Response" not in df.columns:
        df["Response"] = ""

    n = len(df)
    size = n // 4

    chunk1 = df.iloc[:size]
    chunk2 = df.iloc[size:size * 2]
    chunk3 = df.iloc[size * 2:size * 3]
    chunk4 = df.iloc[size * 3:]

    with ThreadPoolExecutor(max_workers=4) as ex:
        f1 = ex.submit(process_chunk, chunk1, api_url, token, 1)
        f2 = ex.submit(process_chunk, chunk2, api_url, token, 2)
        f3 = ex.submit(process_chunk, chunk3, api_url, token, 3)
        f4 = ex.submit(process_chunk, chunk4, api_url, token, 4)

        results = f1.result() + f2.result() + f3.result() + f4.result()

    for idx, status, resp in results:
        df.at[idx, "Status"] = status
        df.at[idx, "Response"] = resp

    df.to_excel(output_excel, index=False)
    print(f"Updated Excel saved at: {output_excel}")

#Constants / Inputs
if __name__ == "__main__":
    INPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\new tags template.xlsx"
    OUTPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\new tags template_output.xlsx"
    API_URL = "https://cloud.cropin.in/services/farm/api/farmers"

    print("Retrieving token...")
    token = get_access_token("asp", "9649964096", "123456", "prod1")
    if token:
        print("Token retrieved successfully.")
        post_data_with_multithreading(API_URL, token, INPUT_EXCEL, OUTPUT_EXCEL)
    else:
        print("Token retrieval failed. Exiting.")
