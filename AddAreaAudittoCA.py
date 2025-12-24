# Author: Rajasekhar Palleti
# Purpose: Update Area Audit for Croppable Areas
# Supports geoInfo in BOTH formats:
# 1) Full GeoJSON FeatureCollection
# 2) Raw coordinates list [[lng, lat], ...]

import json
import requests
import pandas as pd
import time
from GetAuthtoken import get_access_token


# ============================================================
# GEOINFO NORMALIZATION
# ============================================================
def normalize_geo_info(area_Audit_DTO):
    """
    Accepts either:
    1) Full GeoJSON FeatureCollection
    2) Raw coordinates list [[lng, lat], ...]

    Returns:
        Valid GeoJSON FeatureCollection with MultiPolygon
    """

    geo = json.loads(area_Audit_DTO)

    # Case 1: Already valid FeatureCollection
    if isinstance(geo, dict) and geo.get("type") == "FeatureCollection":
        return geo

    # Case 2: Raw coordinates list
    if isinstance(geo, list):
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {},
                    "geometry": {
                        "type": "MultiPolygon",
                        "coordinates": [[geo]]
                    }
                }
            ]
        }

    raise ValueError("Unsupported geoInfo format use [[long, lat], [...]])")


# ============================================================
# MAIN FUNCTION
# ============================================================
def post_data_to_api(api_url, token, input_excel, output_excel, sheet_name):

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "channel": "mobile"
    }

    time_value = 0.5  # seconds
    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    # Ensure output columns exist
    for col in ["Status", "CA_Response"]:
        if col not in df.columns:
            df[col] = ""

    for index, row in df.iterrows():

        CA_id = row.iloc[0]           # Column A
        CA_Name = row.iloc[1]         # Column B
        area_Audit_DTO = row.iloc[2]  # Column C
        Latitude = row.iloc[3]        # Column D
        Longitude = row.iloc[4]       # Column E
        audited_count = row.iloc[5]   # Column F

        # Basic validation
        if pd.isna(CA_id) or pd.isna(area_Audit_DTO):
            df.at[index, "Status"] = "Skipped: Missing Data"
            continue

        # Normalize geoInfo
        try:
            geo_info = normalize_geo_info(area_Audit_DTO)
        except Exception as e:
            df.at[index, "Status"] = f"Invalid GeoInfo: {e}"
            continue

        try:
            print(f"\n🔄 Processing CA_ID: {CA_id}")

            # ---------------- GET CA ----------------
            get_response = requests.get(f"{api_url}/{CA_id}", headers=headers)

            if get_response.status_code != 200:
                df.at[index, "Status"] = f"GET Failed: {get_response.status_code}"
                df.at[index, "CA_Response"] = get_response.text
                continue
            else: print(f"✅ Fetched CA data of  {CA_Name}")

            CA_data = get_response.json()

            # ---------------- PREPARE PAYLOAD ----------------
            areaAudit = {
                "id": None,
                "geoInfo": geo_info,
                "latitude": Latitude,
                "longitude": Longitude,
                "altitude": None
            }

            auditedArea = {
                "count": audited_count,
                "unit": "Hectare"
            }

            CA_data["areaAudit"] = areaAudit
            CA_data["auditedArea"] = auditedArea
            CA_data["latitude"] = None
            CA_data["longitude"] = None
            # CA_data["cropAudited"] = True  # uncomment this line if a script got the cropAudited is not null error

            # ---------------- PUT UPDATE ----------------
            time.sleep(time_value)

            put_response = requests.put(
                f"{api_url}/area-audit",
                headers=headers,
                data=json.dumps(CA_data)
            )

            if put_response.status_code != 200:
                df.at[index, "Status"] = f"PUT Failed: {put_response.status_code}"
                df.at[index, "CA_Response"] = put_response.text
                continue

            df.at[index, "Status"] = "Success"
            df.at[index, "CA_Response"] = put_response.text
            print(f"✅ Updated area audit for CA  {CA_Name}")

        except requests.exceptions.RequestException as e:
            df.at[index, "Status"] = f"Request Failed: {e}"
            df.at[index, "CA_Response"] = str(e)

        time.sleep(time_value)

    # Save output
    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\n📁 Output saved to: {output_excel}")


# ============================================================
# EXECUTION
# ============================================================
if __name__ == "__main__":

    input_excel = r"C:\Users\rajasekhar.palleti\Downloads\Tobacco_plots_detected_output_main2.xlsx"
    output_excel = r"C:\Users\rajasekhar.palleti\Downloads\Tobacco_plots_detected_output_main2_output.xlsx"
    sheet_name = "Sheet1"
    api_url = "https://cloud.cropin.in/services/farm/api/croppable-areas"

    print("🔐 Fetching access token...")
    token = get_access_token("productdemo", "9108896131", "pmproductdemo", "prod1")

    if token:
        print("✅ Access token retrieved")
        post_data_to_api(api_url, token, input_excel, output_excel, sheet_name)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")
