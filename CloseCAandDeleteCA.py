# Author: Rajasekhar Palleti
# Purpose: Close CA in batches of 100, then delete project-assets for same project and CA ids.

import pandas as pd
import requests
import json
import time
from GetAuthtoken import get_access_token


def chunk_list(lst, size):
    """Yield successive chunks from list `lst` of length `size`."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def ensure_col(df, col_name):
    if col_name not in df.columns:
        df[col_name] = ""


# Time interval between calls
delay_time = 5  # seconds


def close_and_delete_batch(base_url, token, project_id, asset_ids_chunk, ca_ids_chunk, first_row_index, df):
    """
    - Close CA API
    - Delete project-assets API
    - Store responses in the Excel sheet
    """

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    ca_ids_param = ",".join(map(str, ca_ids_chunk))
    asset_ids_param = ",".join(map(str, asset_ids_chunk))

    # -------------------------------------------------------------
    # 1) CLOSE CA API
    # -------------------------------------------------------------
    print("Closing the croppable areas... please wait")
    close_url = f"{base_url}/croppable-areas/closed"
    close_params = {"reasonId": 4, "ids": ca_ids_param}

    try:
        resp_close = requests.get(close_url, headers=headers, params=close_params, timeout=60)
        close_status_code = resp_close.status_code

        try:
            close_json = resp_close.json()
        except:
            close_json = None

        # Try extracting id/status pairs if present
        id_status_list = []

        if isinstance(close_json, dict):
            def extract(obj):
                if isinstance(obj, dict):
                    if "id" in obj and "status" in obj:
                        id_status_list.append({"id": obj["id"], "status": obj["status"]})
                    for v in obj.values():
                        extract(v)
                elif isinstance(obj, list):
                    for x in obj:
                        extract(x)

            extract(close_json)

        elif isinstance(close_json, list):
            for it in close_json:
                if isinstance(it, dict) and "id" in it and "status" in it:
                    id_status_list.append({"id": it["id"], "status": it["status"]})

        closed_api_summary = id_status_list or close_json or resp_close.text

    except Exception as e:
        close_status_code = "Error"
        closed_api_summary = str(e)

    time.sleep(delay_time)

    # -------------------------------------------------------------
    # 2) DELETE PROJECT-ASSETS API
    # -------------------------------------------------------------
    print("Deleting the croppable areas... please wait")
    delete_url = f"{base_url}/projects/{project_id}/project-assets/selected-ids"
    delete_params = {"ids": asset_ids_param, "croppableAreaIds": ca_ids_param}

    delete_status_code = None
    delete_json = None
    delete_text = None
    deletable = None
    non_deletable = None
    is_deleted = False

    try:
        resp_delete = requests.delete(delete_url, headers=headers, params=delete_params, timeout=120)

        delete_status_code = resp_delete.status_code
        delete_text = resp_delete.text

        try:
            delete_json = resp_delete.json()
        except:
            delete_json = None

        # SUCCESS logic — ONLY accept status 200/204 and JSON containing expected fields
        if delete_status_code in (200, 204) and isinstance(delete_json, dict):
            deletable = delete_json.get("deletable")
            non_deletable = delete_json.get("nonDeletable")

            if deletable is not None and non_deletable is not None:
                is_deleted = True

    except Exception as e:
        delete_status_code = "Error"
        delete_json = None
        delete_text = str(e)
        is_deleted = False

    # -------------------------------------------------------------
    # SAVE RESULTS INTO EXCEL
    # -------------------------------------------------------------
    # Close API status
    try:
        df.at[first_row_index, "closed API status"] = json.dumps(closed_api_summary, default=str)
    except:
        df.at[first_row_index, "closed API status"] = str(closed_api_summary)

    # Delete API status
    delete_info = {
        "http_status": delete_status_code,
        "deletable": deletable,
        "nonDeletable": non_deletable,
        "deleted": is_deleted
    }

    try:
        df.at[first_row_index, "delete API status"] = json.dumps(delete_info, default=str)
    except:
        df.at[first_row_index, "delete API status"] = str(delete_info)

    df.at[first_row_index, "closed_api_http_status"] = close_status_code
    df.at[first_row_index, "delete_api_http_status"] = delete_status_code

    # -------------------------------------------------------------
    # PRINT RESULTS TO CONSOLE
    # -------------------------------------------------------------
    print(f"➡ PROJECT {project_id} | Close Status: {close_status_code}")

    if is_deleted:
        print(f"   ✔ DELETE SUCCESS → deletable={deletable}, nonDeletable={non_deletable}")
    else:
        print(f"   ❌ DELETE FAILED → status={delete_status_code}, response={delete_text}")


def process_excel_batches(base_url, token, input_excel, sheet_name, output_excel, batch_size=100):
    print("📂 Loading input Excel file...")
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

    df[project_col] = df[project_col].astype(str).str.strip()
    df[asset_col] = df[asset_col].astype(str).str.strip()
    df[ca_col] = df[ca_col].astype(str).str.strip()

    df = df[df[ca_col].notna() & (df[ca_col] != "")]
    df.reset_index(drop=True, inplace=True)

    grouped = df.groupby(project_col)

    for project_id, group in grouped:
        asset_ids_all = group[asset_col].tolist()
        ca_ids_all = group[ca_col].tolist()
        indices_all = group.index.tolist()

        for offset, (asset_chunk, ca_chunk, idx_chunk) in enumerate(
                zip(chunk_list(asset_ids_all, batch_size),
                    chunk_list(ca_ids_all, batch_size),
                    chunk_list(indices_all, batch_size))):

            first_row_index = idx_chunk[0]

            print(f"\n🔁 Processing Project {project_id} | Batch {offset+1} | Items: {len(asset_chunk)}")

            close_and_delete_batch(
                base_url, token, project_id,
                asset_chunk, ca_chunk, first_row_index, df
            )

            time.sleep(delay_time)

    print(f"\n💾 Saving output to: {output_excel}")
    df.to_excel(output_excel, index=False)
    print("✅ Process completed successfully.")


# ------------------ MAIN ------------------

if __name__ == "__main__":
    BASE_URL = "https://cloud.cropin.in/services/farm/api"
    INPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\Delete_Project_Assets.xlsx"
    SHEET_NAME = "Sheet1"
    OUTPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\Delete_Project_Assets_Output.xlsx"

    token = get_access_token("asp", "9148981108", "cropin@123", "prod1")

    if not token:
        print("❌ Failed to retrieve access token. Exiting.")
        raise SystemExit(1)

    print("✅ Token retrieved. Starting execution...")
    process_excel_batches(BASE_URL, token, INPUT_EXCEL, SHEET_NAME, OUTPUT_EXCEL, batch_size=100)
