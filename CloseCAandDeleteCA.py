# Author: Rajasekhar Palleti
# Purpose: Close CA in batches then delete project-assets for same project.

import pandas as pd
import requests
import json
import time
from GetAuthtoken import get_access_token


def chunk_list(lst, size):
    """Split list into batches"""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def ensure_col(df, col_name):
    """Ensure column exists in dataframe"""
    if col_name not in df.columns:
        df[col_name] = ""


delay_time = 5


def close_and_delete_batch(base_url, token, project_id, asset_ids_chunk, ca_ids_chunk, idx_chunk, df):

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    ca_ids_param = ",".join(map(str, ca_ids_chunk))

    # -------------------------------------------------------------
    # CLOSE CA API
    # -------------------------------------------------------------
    print("\n🔒 Closing croppable areas...")

    close_url = f"{base_url}/croppable-areas/closed"
    close_params = {"reasonId": 4, "ids": ca_ids_param}

    ca_status_map = {}

    start_time_close = time.time()

    try:

        resp_close = requests.get(close_url, headers=headers, params=close_params, timeout=600)

        close_status_code = resp_close.status_code

        try:
            close_json = resp_close.json()
        except:
            close_json = None

        def extract(obj):
            if isinstance(obj, dict):
                if "id" in obj and "status" in obj:
                    ca_status_map[str(obj["id"])] = str(obj["status"])
                for v in obj.values():
                    extract(v)
            elif isinstance(obj, list):
                for x in obj:
                    extract(x)

        if isinstance(close_json, (dict, list)):
            extract(close_json)

        processed_ids = set(map(str, ca_ids_chunk))
        response_ids = set(ca_status_map.keys())
        missed_ids = processed_ids - response_ids

        missing_ids_str = ",".join(missed_ids) if missed_ids else ""

        print(f"📤 Processed IDs ({len(processed_ids)}): {', '.join(processed_ids)}")
        print(f"📥 Response IDs  ({len(response_ids)}): {', '.join(response_ids) if response_ids else 'None'}")
        print(f"📝 IDs missed in response ({len(missed_ids)}): {', '.join(missed_ids) if missed_ids else 'None'}")

        elapsed_close = time.time() - start_time_close

        if close_status_code == 200:
            print(f"✅ Close Success (HTTP {close_status_code}) in {elapsed_close:.2f}s")
        else:
            print(f"⚠️ Close Warning (HTTP {close_status_code}) in {elapsed_close:.2f}s")

    except Exception as e:

        elapsed_close = time.time() - start_time_close
        close_status_code = "Error"

        missing_ids_str = ",".join(ca_ids_chunk)

        print(f"❌ Close Error: {str(e)} in {elapsed_close:.2f}s")

    # -------------------------------------------------------------
    # SAVE CLOSE STATUS
    # -------------------------------------------------------------
    for row_idx, df_index in enumerate(idx_chunk):

        current_ca = str(ca_ids_chunk[row_idx])

        df.at[df_index, "closed_api_http_status"] = close_status_code

        if current_ca in ca_status_map:
            df.at[df_index, "closed API status"] = ca_status_map[current_ca]
        else:
            df.at[df_index, "closed API status"] = "Not found in response"

        df.at[df_index, "CA ids missing in response"] = missing_ids_str

    time.sleep(delay_time)

    # -------------------------------------------------------------
    # FILTER CLOSED CA FOR DELETE
    # -------------------------------------------------------------
    ca_delete = []
    asset_delete = []
    idx_delete = []

    for row_idx, df_index in enumerate(idx_chunk):

        status = str(df.at[df_index, "closed API status"]).strip().lower()

        if status == "closed":
            ca_delete.append(ca_ids_chunk[row_idx])
            asset_delete.append(asset_ids_chunk[row_idx])
            idx_delete.append(df_index)
        else:
            df.at[df_index, "delete_api_http_status"] = "Skipped"
            df.at[df_index, "delete API status"] = "Skipped (Not Closed)"

    if not ca_delete:
        print("⚠️ No croppable areas were closed. Skipping delete.")
        return

    # -------------------------------------------------------------
    # DELETE PROJECT ASSETS
    # -------------------------------------------------------------
    print("\n🗑 Deleting project-assets...")

    delete_url = f"{base_url}/projects/{project_id}/project-assets/selected-ids"

    delete_params = {
        "ids": ",".join(map(str, asset_delete)),
        "croppableAreaIds": ",".join(map(str, ca_delete))
    }

    start_time_delete = time.time()

    try:

        resp_delete = requests.delete(delete_url, headers=headers, params=delete_params, timeout=600)

        delete_status_code = resp_delete.status_code

        try:
            delete_json = resp_delete.json()
        except:
            delete_json = None

        deletable = None
        non_deletable = None
        is_deleted = False

        if delete_status_code in (200, 204) and isinstance(delete_json, dict):

            deletable = delete_json.get("deletable")
            non_deletable = delete_json.get("nonDeletable")

            if deletable is not None and non_deletable is not None:
                is_deleted = True

        delete_info = {
            "http_status": delete_status_code,
            "deletable": deletable,
            "nonDeletable": non_deletable,
            "deleted": is_deleted
        }

        for df_index in idx_delete:
            df.at[df_index, "delete_api_http_status"] = delete_status_code
            df.at[df_index, "delete API status"] = json.dumps(delete_info)

        elapsed_delete = time.time() - start_time_delete

        if is_deleted:
            print(f"✅ Delete Success (deletable={deletable}, nonDeletable={non_deletable}) in {elapsed_delete:.2f}s")
        else:
            print(f"❌ Delete Failed (HTTP {delete_status_code}) in {elapsed_delete:.2f}s")

    except Exception as e:

        elapsed_delete = time.time() - start_time_delete

        print(f"❌ Delete Error: {str(e)} in {elapsed_delete:.2f}s")

        for df_index in idx_delete:
            df.at[df_index, "delete_api_http_status"] = "Error"
            df.at[df_index, "delete API status"] = str(e)


def process_excel_batches(base_url, token, input_excel, sheet_name, output_excel, batch_size=100):

    print("📂 Loading Excel file...")

    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    expected = ["project_id", "project_asset_id", "croppable_area_id"]

    lower_cols = {c.lower(): c for c in df.columns}

    for exp in expected:
        if exp not in lower_cols:
            raise ValueError(f"Missing required column: {exp}")

    project_col = lower_cols["project_id"]
    asset_col = lower_cols["project_asset_id"]
    ca_col = lower_cols["croppable_area_id"]

    ensure_col(df, "closed API status")
    ensure_col(df, "delete API status")
    ensure_col(df, "closed_api_http_status")
    ensure_col(df, "delete_api_http_status")
    ensure_col(df, "CA ids missing in response")

    df = df[df[ca_col].notna() & (df[ca_col] != "")]
    df.reset_index(drop=True, inplace=True)

    total_ca = len(df)
    processed_ca = 0

    print(f"\n📊 Total CA to process: {total_ca}")

    grouped = df.groupby(project_col)

    total_projects = len(grouped)

    print(f"🔄 Found {total_projects} projects to process.")

    for p_idx, (project_id, group) in enumerate(grouped, 1):

        asset_ids_all = group[asset_col].tolist()
        ca_ids_all = group[ca_col].tolist()
        indices_all = group.index.tolist()

        print(f"\n📁 Project {project_id} ({p_idx}/{total_projects}) | Total Items: {len(ca_ids_all)}")

        for offset, (asset_chunk, ca_chunk, idx_chunk) in enumerate(
                zip(chunk_list(asset_ids_all, batch_size),
                    chunk_list(ca_ids_all, batch_size),
                    chunk_list(indices_all, batch_size))):

            print(f"\n🔁 Batch {offset+1} | Items: {len(asset_chunk)}")

            for row_idx, (ca_id, asset_id) in enumerate(zip(ca_chunk, asset_chunk)):

                excel_row = idx_chunk[row_idx] + 2

                print(
                    f"📌 Row {excel_row} | "
                    f"project_id: {project_id} | "
                    f"croppable_area_id: {ca_id} | "
                    f"project_asset_id: {asset_id}"
                )

            close_and_delete_batch(
                base_url,
                token,
                project_id,
                asset_chunk,
                ca_chunk,
                idx_chunk,
                df
            )

            processed_ca += len(ca_chunk)

            print(f"📊 Progress: {processed_ca} / {total_ca} CA processed")

            time.sleep(delay_time)

    print(f"\n💾 Saving output file to: {output_excel}")

    df.to_excel(output_excel, index=False)

    print("🎯 Process completed successfully.")


# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
if __name__ == "__main__":

    BASE_URL = "https://cloud.cropin.in/services/farm/api"

    INPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\Delete_Project_Assets.xlsx"

    SHEET_NAME = "Sheet1"

    OUTPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\Delete_Project_Assets_Output.xlsx"

    print("🔐 Fetching access token...")

    token = get_access_token("asp", "9148981108", "cropin@123", "prod1")

    if not token:
        print("❌ Failed to retrieve access token.")
        exit()

    print("✅ Token retrieved. Starting execution...")

    process_excel_batches(
        BASE_URL,
        token,
        INPUT_EXCEL,
        SHEET_NAME,
        OUTPUT_EXCEL,
        batch_size=50
    )