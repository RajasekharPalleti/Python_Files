# Author: Rajasekhar Palleti

import json
import requests
import pandas as pd
import time
from GetAuthtoken import get_access_token


# -------------------------------------------------
# Robust sowing date parser (handles ALL formats)
# -------------------------------------------------
def parse_sowing_date(raw_date):
    """
    Supported formats:
    - Excel Timestamp
    - dd/MM/yyyy
    - dd-MM-yyyy
    - yyyy-MM-dd
    - yyyy-MM-ddTHH:mm:ss
    - yyyy-MM-dd HH:mm:ss
    """

    if pd.isna(raw_date):
        return None

    try:
        parsed_date = pd.to_datetime(
            raw_date,
            dayfirst=True,   # IMPORTANT for 06/10/1994
            errors="raise"
        )

        # Keep time if provided, else normalize to 00:00:00
        return parsed_date.strftime("%Y-%m-%dT%H:%M:%S.000+0000")

    except Exception as e:
        print(f"❌ Invalid sowing date format: {raw_date} | Error: {e}")
        return None


# -------------------------------------------------
# Main processing function
# -------------------------------------------------
def post_data_to_api(api_url, token, input_excel, output_excel, sheet_name):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    # Ensure output columns exist
    if "Status" not in df.columns:
        df["Status"] = ""
    if "CA_Response" not in df.columns:
        df["CA_Response"] = ""

    for index, row in df.iterrows():
        CA_id = row.iloc[0]        # Column A
        CA_name = row.iloc[1]      # Column B
        variety_id = row.iloc[2]   # Column C
        raw_sowing_date = row.iloc[3]  # Column D

        # Validate mandatory fields
        if pd.isna(CA_id) or pd.isna(variety_id) or pd.isna(raw_sowing_date):
            df.at[index, "Status"] = "Skipped: Missing Data"
            continue

        # Parse sowing date
        sowingDate = parse_sowing_date(raw_sowing_date)
        if not sowingDate:
            df.at[index, "Status"] = "Skipped: Invalid Sowing Date"
            continue

        try:
            # -----------------------
            # GET croppable area
            # -----------------------
            get_response = requests.get(f"{api_url}/{CA_id}", headers=headers)
            if get_response.status_code != 200:
                df.at[index, "Status"] = f"GET Failed: {get_response.status_code}"
                print(f"❌ GET failed for CA_ID {CA_id}")
                continue

            CA_data = get_response.json()
            print(f"\n🔄 Processing Row {index + 2} | CA_ID: {CA_id}")

            # -----------------------
            # Update sowing date
            # -----------------------
            CA_data["sowingDate"] = sowingDate
            print(f"🌱 Updated sowingDate: {sowingDate}")
            # -----------------------
            # Update variety
            # -----------------------
            CA_data["varietyId"] = variety_id
            print(f"🌾 Updated varietyId: {variety_id}")

            time.sleep(1)

            # -----------------------
            # PUT update CA
            # -----------------------
            put_response = requests.put(
                api_url,
                headers=headers,
                data=json.dumps(CA_data)
            )

            if put_response.status_code != 200:
                df.at[index, "Status"] = f"PUT Failed: {put_response.status_code}"
                df.at[index, "CA_Response"] = put_response.text
                print(f"❌ PUT failed for CA_ID {CA_id}")
                continue

            df.at[index, "Status"] = "Success"
            df.at[index, "CA_Response"] = put_response.text
            print(f"✅ Successfully updated CA_ID: {CA_id}")

        except requests.exceptions.RequestException as e:
            df.at[index, "Status"] = f"Failed: {str(e)}"
            print(f"❌ Exception for CA_ID {CA_id}: {e}")

        time.sleep(2)

    # Save output
    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\n📁 Excel file updated successfully: {output_excel}")


# -------------------------------------------------
# Execution
# -------------------------------------------------
if __name__ == "__main__":
    input_excel = r"C:\Users\rajasekhar.palleti\Downloads\bat tenant second set.xlsx"
    sheet_name = "Sheet1"
    output_excel = r"C:\Users\rajasekhar.palleti\Downloads\bat tenant second set_DOS_Crop_output.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/croppable-areas"

    print("🔐 Retrieving access token...")
    token = get_access_token("bat", "6543345612", "Cropin123", "prod1")

    if token:
        print("✅ Access token retrieved successfully.")
        post_data_to_api(api_url, token, input_excel, output_excel, sheet_name)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")
