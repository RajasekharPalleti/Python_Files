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
    parts = [p.strip() for p in s.split(",") if p.strip()]
    ids = []
    for p in parts:
        try:
            ids.append(int(p))
        except ValueError:
            continue
    return ids


def process_chunk(df_chunk, api_url, token, thread_id, timeout=30):
    headers = {"Authorization": f"Bearer {token}"}
    results = []

    for index, row in df_chunk.iterrows():
        print(f"[Thread {thread_id}] Processing row index: {index}")

        asset_id = row.iloc[0]        # Column A: Asset ID
        raw_tags_cell = row.iloc[2]   # Column C: Asset Tag IDs

        status = ""
        response_str = ""

        if pd.isna(asset_id):
            status = "Skipped: Missing Asset ID"
            results.append((index, status, response_str))
            continue

        new_ids = parse_comma_ids(raw_tags_cell)
        if not new_ids:
            status = "Skipped: No tag IDs to add"
            results.append((index, status, response_str))
            continue

        try:
            # GET existing asset
            get_resp = requests.get(
                f"{api_url}/{asset_id}",
                headers=headers,
                timeout=timeout
            )
            get_resp.raise_for_status()
            asset_json = get_resp.json()

            data = asset_json.get("data", {})
            existing_tags = data.get("tags", []) or []

            # normalize existing tags to ints
            existing_ints = []
            for t in existing_tags:
                try:
                    existing_ints.append(int(t))
                except Exception:
                    continue

            # determine which IDs to add
            to_add = [i for i in new_ids if i not in existing_ints]
            if not to_add:
                status = "Skipped: All IDs already present"
                results.append((index, status, response_str))
                continue

            # update tags
            data["tags"] = existing_ints + to_add
            asset_json["data"] = data

            # PUT updated asset
            multipart = {
                "dto": (None, json.dumps(asset_json), "application/json")
            }

            put_resp = requests.put(
                f"{api_url}",
                headers=headers,
                files=multipart,
                timeout=timeout
            )
            put_resp.raise_for_status()

            status = "Success"
            response_str = put_resp.text[:500]

            print(
                f"[Thread {thread_id}] Asset {asset_id} updated, added IDs: {to_add}"
            )

        except requests.exceptions.RequestException as e:
            status = f"Failed: {str(e)}"
            response_str = str(e)
            print(
                f"[Thread {thread_id}] Request error for asset {asset_id}: {e}"
            )

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

    chunks = [
        df.iloc[:size],
        df.iloc[size:size * 2],
        df.iloc[size * 2:size * 3],
        df.iloc[size * 3:]
    ]

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(process_chunk, chunks[i], api_url, token, i + 1)
            for i in range(4)
        ]

        results = []
        for f in futures:
            results.extend(f.result())

    for idx, status, resp in results:
        df.at[idx, "Status"] = status
        df.at[idx, "Response"] = resp

    df.to_excel(output_excel, index=False)
    print(f"✅ Updated Excel saved at: {output_excel}")


# ================== INPUTS ==================
if __name__ == "__main__":
    INPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\new_asset_tags_template.xlsx"
    OUTPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\new_asset_tags_template_output.xlsx"
    API_URL = "https://cloud.cropin.in/services/farm/api/assets"

    print("🔐 Retrieving token...")
    token = get_access_token("asp", "9649964096", "123456", "prod1")

    if token:
        print("✅ Token retrieved successfully.")
        post_data_with_multithreading(API_URL, token, INPUT_EXCEL, OUTPUT_EXCEL)
    else:
        print("❌ Token retrieval failed. Exiting.")
