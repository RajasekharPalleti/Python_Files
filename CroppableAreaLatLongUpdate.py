# Author: Rajasekhar Palleti

import json
import requests
import pandas as pd
import time

from GetAuthtoken import get_access_token


def post_data_to_api(api_url, token, input_excel, output_excel, sheet_name):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "channel": "mobile"
    }

    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    if "Status" not in df.columns:
        df["Status"] = ""
    if "CA_Response" not in df.columns:
        df["CA_Response"] = ""

    for index, row in df.iterrows():
        CA_id = row.iloc[0]  # Column A
        Latitude = row.iloc[2]  # Column C
        Longitude = row.iloc[3]  # Column D

        if pd.isna(CA_id) or pd.isna(Latitude) or pd.isna(Longitude):
            df.at[index, "Status"] = "Skipped: Missing Data"
            continue

        try:
            get_response = requests.get(f"{api_url}/{CA_id}", headers=headers)
            if get_response.status_code != 200:
                df.at[index, "Status"] = f"GET Failed: {get_response.status_code}"
                print(f"GET failed for CA_ID: {CA_id} — Status Code: {get_response.status_code}")
                continue
            get_response.raise_for_status()
            CA_data = get_response.json()
            print(f"\nRow {index + 2} — CA_ID: {CA_id}")

            CA_data["latitude"] = Latitude
            CA_data["longitude"] = Longitude

            # Condition to check if CA is area audited then updating accordingly
            if (
                "areaAudit" in CA_data
                and CA_data["areaAudit"] is not None
                and CA_data["areaAudit"].get("latitude") is not None
                and CA_data["areaAudit"].get("longitude") is not None):

                CA_data["areaAudit"]["latitude"] = Latitude
                CA_data["areaAudit"]["longitude"] = Longitude
                CA_data["cropAudited"] = True
            else:
                CA_data["areaAudit"] = {
                    "geoInfo": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {},
                                "geometry": {
                                    "type": "MultiPolygon",
                                    "coordinates": []
                                }
                            }
                        ]
                    },
                    "latitude": Latitude,
                    "longitude": Longitude,
                    "altitude": None
                }
                CA_data["cropAudited"] = False

            print(f"Updated Lat: {Latitude}, Long: {Longitude}")

            time.sleep(1)
            put_response = requests.put(f"{api_url}/area-audit", headers=headers, data=json.dumps(CA_data))
            if put_response.status_code not in (200, 204):
                df.at[index, "Status"] = "Failed"
                print(put_response)
                df.at[index, "CA_Response"] = put_response.text
                print(f"PUT failed for CA_ID: {CA_id} — Status Code: {put_response.status_code}")
                continue
            put_response.raise_for_status()

            df.at[index, "Status"] = "Success"
            df.at[index, "CA_Response"] = put_response.text
            print(f"Successfully updated CA_ID: {CA_id}")

        except requests.exceptions.RequestException as e:
            df.at[index, "Status"] = f"Failed: {str(e)}"
            print(f"Update failed for CA_ID: {CA_id} — {e}")

        time.sleep(2)

    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\nExcel file updated and saved as {output_excel}")


if __name__ == "__main__":
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\GEO_TAG_WALMART.xlsx"
    sheet_name = "result"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\GEO_TAG_WALMART_report.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/croppable-areas"

    print("Retrieving access token...")
    token = get_access_token("walmart", "12345012345", "Cropin@123", "prod1")

    if token:
        print("Access token retrieved successfully.")
        post_data_to_api(api_url, token, input_excel, output_excel, sheet_name)
    else:
        print("Failed to retrieve access token. Process terminated.")
