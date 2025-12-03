import sys
from typing import Tuple, Optional

import requests
import pandas as pd
# from GetAuthtoken import get_access_token  # your existing auth method

# ==========================
# CONFIG / CONSTANTS (moved up so main can use them)
# ==========================
PLACE_API_BASE_URL = "https://ca-v2-gcp.cropin.co.in/qa6/services/farm/api/place"

# 🔧 Excel config
EXCEL_FILE_PATH = r"C:\Users\rajasekhar.palleti\Downloads\place_ids.xlsx"   # input file
OUTPUT_EXCEL_FILE_PATH = r"C:\Users\rajasekhar.palleti\Downloads\place_ids_output.xlsx"
SHEET_NAME = "Sheet1"
PLACE_ID_COLUMN_NAME = "placeId"   # column header containing place IDs


def delete_place(session: requests.Session, token: str, place_id: int) -> Tuple[str, Optional[int], str]:

    url = f"{PLACE_API_BASE_URL}/{place_id}"

    headers = {
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
    }

    print(f"🗑 Deleting place id: {place_id} → {url}")

    try:
        resp = session.delete(url, headers=headers, timeout=30)
    except requests.RequestException as exc:
        msg = f"Request error: {exc}"
        print(f"   ❌ {msg}", file=sys.stderr)
        return "FAILED", None, msg

    status_code = resp.status_code

    if status_code in (200, 202, 204):
        print(f"   ✅ Deleted successfully (status {status_code})")
        return "SUCCESS", status_code, "Deleted successfully"
    elif status_code == 404:
        print(f"   ⚠️ Not found (404) for id {place_id}")
        return "NOT_FOUND", status_code, "Place not found"
    else:
        body = resp.text
        if len(body) > 300:
            body = body[:300] + "...(truncated)"
        msg = f"Failed (status {status_code}) → {body}"
        print(f"   ❌ {msg}", file=sys.stderr)
        return "FAILED", status_code, body


def main() -> None:
    # 1) Get token first; don't proceed if it fails
    # token = get_access_token()
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJqZ1JBYTkxSVg5RUxveExYdlp0S0RLTVlJbmhlY20zdHZCNW1GMm5YZGhFIn0.eyJleHAiOjE3NjM1MzIyOTksImlhdCI6MTc2MzQ0NTg5OSwiYXV0aF90aW1lIjoxNzYzNDQ1ODk5LCJqdGkiOiIyNGE3MzBlOC01MTUzLTRhNDctYTc4Mi1mMmNlODg4ZDBjNjYiLCJpc3MiOiJodHRwczovL3Yyc3NvLWdjcC5jcm9waW4uY28uaW4vYXV0aC9yZWFsbXMvcWF6b25lNiIsImF1ZCI6WyJyZXNvdXJjZV9zZXJ2ZXIiLCJhY2NvdW50Il0sInN1YiI6ImRmOThiNTI5LTg1Y2YtNGU0Ny1hNmQyLWJjYjc2ZDVlMDdjMSIsInR5cCI6IkJlYXJlciIsImF6cCI6IndlYl9hcHAiLCJub25jZSI6Il9jbGllbnRfd2ViX2FwcCIsInNlc3Npb25fc3RhdGUiOiJjN2E2ZDBiZi0zODRmLTQ0NWEtOGRkOC05ZTE3NDFiZTQ0MTgiLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJyZXNvdXJjZV9zZXJ2ZXIiOnsicm9sZXMiOlsiUk9MRV9BRE1JTl8xMzUxIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIHBob25lIG1pY3JvcHJvZmlsZS1qd3Qgb2ZmbGluZV9hY2Nlc3MgYWRkcmVzcyIsInNpZCI6ImM3YTZkMGJmLTM4NGYtNDQ1YS04ZGQ4LTllMTc0MWJlNDQxOCIsInVzZXJfcm9sZSI6WyJST0xFX0FETUlOXzEzNTEiXSwidXBuIjoiNzM4MjIxMTIzMSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiYWRkcmVzcyI6e30sImdyb3VwcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl0sInByZWZlcnJlZF91c2VybmFtZSI6IjczODIyMTEyMzEiLCJlbWFpbCI6InJhamFAZ21haWwuY29tIiwidGVuYW50IjoicWF6b25lNiJ9.A2x4yXgAinJ7cGwcBcCj4gQdpZ8gpes2SRkhon3k8xRmstvu9Dv0dEsBD9MpjVZSV25QLEFNOzf606rNI9MlVi32GmICAekg7UFesVn12GZ_wWGf7_RTwAxIfuu_A6_YJZ4izof6ivmzPzf4VKdx1oE8iX4uvw60zGJCTjzYiywpxP0WYxrlRaG-xcyfnHu_BypDXAy6mI6UfYqDDgJi7P0MCtRmafnY-Iig60qwdhbH0skBoCF1QHxYfdC6IbqvUVb_5Er2LBVKmjNkccjmG5yFHUFjJSrcjw7MBazS1UyjUKZOwP3laoDJwM5ROpQlq8tbqhmnDNDmEiro_GU1UA"
    if not token:
        print("❌ Failed to generate access token. Aborting.", file=sys.stderr)
        return

    print("🔑 Access token generated successfully. Proceeding with deletions...")

    # 2) Read Excel
    try:
        df = pd.read_excel(EXCEL_FILE_PATH, sheet_name=SHEET_NAME)
    except Exception as exc:
        print(f"❌ Failed to read Excel file: {exc}", file=sys.stderr)
        return

    if PLACE_ID_COLUMN_NAME not in df.columns:
        print(f"❌ Column '{PLACE_ID_COLUMN_NAME}' not found in Excel.", file=sys.stderr)
        print(f"   Available columns: {list(df.columns)}", file=sys.stderr)
        return

    # Prepare output columns
    if "delete_status" not in df.columns:
        df["delete_status"] = ""
    if "delete_http_status" not in df.columns:
        df["delete_http_status"] = ""
    if "delete_message" not in df.columns:
        df["delete_message"] = ""

    total_rows = len(df)
    print(f"📄 Loaded {total_rows} rows from Excel.")

    with requests.Session() as session:
        for idx, row in df.iterrows():
            raw_val = row[PLACE_ID_COLUMN_NAME]

            print(f"\n[{idx + 1}/{total_rows}] Processing row with raw place id value: {raw_val!r}")

            # Handle NaN / empty / invalid
            if pd.isna(raw_val) or str(raw_val).strip() == "":
                print("   ⚠️ Empty place id. Skipping.")
                df.at[idx, "delete_status"] = "INVALID_ID"
                df.at[idx, "delete_http_status"] = ""
                df.at[idx, "delete_message"] = "Empty or NaN place id"
                continue

            try:
                # handle float like 731378.0 or string "731378"
                place_id = int(str(raw_val).strip().split(".")[0])
            except ValueError:
                print(f"   ⚠️ Invalid place id format: {raw_val!r}. Skipping.", file=sys.stderr)
                df.at[idx, "delete_status"] = "INVALID_ID"
                df.at[idx, "delete_http_status"] = ""
                df.at[idx, "delete_message"] = f"Invalid place id format: {raw_val!r}"
                continue

            status, http_status, message = delete_place(session, token, place_id)

            df.at[idx, "delete_status"] = status
            df.at[idx, "delete_http_status"] = http_status if http_status is not None else ""
            df.at[idx, "delete_message"] = message

    # 3) Write back to Excel (same file or different output file)
    try:
        df.to_excel(OUTPUT_EXCEL_FILE_PATH, sheet_name=SHEET_NAME, index=False)
        print(f"\n💾 Status written to Excel: {OUTPUT_EXCEL_FILE_PATH}")
    except Exception as exc:
        print(f"❌ Failed to write Excel file: {exc}", file=sys.stderr)

    print("\n🎉 Deletion run completed.")


if __name__ == "__main__":
    main()
