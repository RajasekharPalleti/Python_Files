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
    time_value = 0.5  # seconds

    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    if "Status" not in df.columns:
        df["Status"] = ""
    if "CA_Response" not in df.columns:
        df["CA_Response"] = ""

    for index, row in df.iterrows():
        CA_id = row.iloc[0]  # Column A
        area_Audit_DTO = row.iloc[2]  # Column C
        Latitude = row.iloc[3]  # Column D
        Longitude = row.iloc[4]  # Column E
        audited_count = row.iloc[5]  # Column F

        geo_info = json.loads(area_Audit_DTO) #area_Audit_DTO is a JSON string


        if pd.isna(CA_id) or pd.isna(Latitude) or pd.isna(Longitude) or pd.isna(area_Audit_DTO):
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

            # Ensure areAudit Key exist
            if "areaAudit" not in CA_data:
                print("areaAudit key missing. Adding...")
                CA_data["areaAudit"] = None

            areaAudit =   {
                "id": None,
                "geoInfo" : geo_info,
                "latitude": Latitude,
                "longitude": Longitude,
                "altitude": None
            }
            auditedArea  = {
                "count": audited_count,
                "unit": "Hectare" #As per the user preferred unit
            }

            # Set values from Excel
            CA_data["areaAudit"] = areaAudit
            CA_data["latitude"] = None
            CA_data["longitude"] = None
            CA_data["auditedArea"]= auditedArea
            CA_data["cropAudited"] = True  # uncomment this line if a script got the cropAudited is not null error
            print(f"Updated the Area audit in CA DTO", )

            time.sleep(time_value)

            put_response = requests.put(f"{api_url}/area-audit", headers=headers, data=json.dumps(CA_data))
            if put_response.status_code != 200:
                df.at[index, "Status"] = f"PUT Failed: {put_response.status_code}"
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

        time.sleep(time_value)

    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\nExcel file updated and saved as {output_excel}")


if __name__ == "__main__":
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\cangucu_tobacco_plots.geojson_output1.xlsx"
    sheet_name = "Sheet2"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\cangucu_tobacco_plots.geojson_output_final.xlsx"
    api_url = "https://cloud.cropin.in/services/farm/api/croppable-areas"

    print("Retrieving access token...")
    token = get_access_token("productdemo", "9108896131", "pmproductdemo", "prod1")
    if token:
        print("Access token retrieved successfully.")
        post_data_to_api(api_url, token, input_excel, output_excel, sheet_name)
    else:
        print("Failed to retrieve access token. Process terminated.")
