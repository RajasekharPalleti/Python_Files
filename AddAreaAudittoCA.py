# Author: Rajasekhar Palleti

import json
import requests
import pandas as pd
import time

# from GetAuthtoken import get_access_token


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
        area_Audit_DTO = row.iloc[1]  # Column B
        Latitude = row.iloc[2]  # Column C
        Longitude = row.iloc[3]  # Column D
        audited_count = row.iloc[4]  # Column E
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
                "unit": "Acre"
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
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\22 plots area audit file.xlsx"
    sheet_name = "result"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\22 plots area audit file_report.xlsx"
    api_url = "https://sf-v2-gcp.cropin.co.in/qa2/services/farm/api/croppable-areas"

    print("Retrieving access token...")
    # token = get_access_token("walmart", "12345012345", "Cropin@123", "prod1")
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJlQnI0SDh0NmF1UjZydE9hTFVqZWpPNmprcmszbEpnRDhlbl81UzRncFFvIn0.eyJleHAiOjE3NTM0MzUxNzYsImlhdCI6MTc1MzQyMDc3NiwiYXV0aF90aW1lIjoxNzUzNDIwNzc2LCJqdGkiOiI4ODQwOWJiMC1jNjcwLTQwNWQtOTdjZC0wM2MyYzg5ZDhlZDciLCJpc3MiOiJodHRwczovL3Yyc3NvLWdjcC5jcm9waW4uY28uaW4vYXV0aC9yZWFsbXMvcWF6b25lMiIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiI1NzgwY2E2NC0xOGRjLTQwYjAtYTdmMC00ODE3NzZjNTRjNWMiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJ3ZWJfYXBwIiwibm9uY2UiOiJfY2xpZW50X3dlYl9hcHAiLCJzZXNzaW9uX3N0YXRlIjoiYTA1MWMwMmMtNzcyYS00OGFlLTkxYTMtY2YwNWRjOWVhOTQ2IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsImRlZmF1bHQtcm9sZXMtcWF6b25lMiIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgcGhvbmUgcHJvZmlsZSBhZGRyZXNzIGVtYWlsIG9mZmxpbmVfYWNjZXNzIG1pY3JvcHJvZmlsZS1qd3QiLCJzaWQiOiJhMDUxYzAyYy03NzJhLTQ4YWUtOTFhMy1jZjA1ZGM5ZWE5NDYiLCJ1cG4iOiI3MzgyMjEwNDAzIiwiYWRkcmVzcyI6e30sImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiZ3JvdXBzIjpbIm9mZmxpbmVfYWNjZXNzIiwiZGVmYXVsdC1yb2xlcy1xYXpvbmUyIiwidW1hX2F1dGhvcml6YXRpb24iXSwicHJlZmVycmVkX3VzZXJuYW1lIjoiNzM4MjIxMDQwMyIsInRlbmFudCI6InFhem9uZTIifQ.T93a0riTI9BR_ZUCJpniZR-2elWYu3RQCWVIVqx-HyhkRX1jsYzpdLFwOqfv6p9gnPoYkDw0kr1H_rOjINsA8rV4hvpWSJeSB8oXeznGfRv-rOEiluVeODGBskeayvgA15-NTI-GaUpQcqIufqW7eXzN4wUKMwcsS4IdoCvChqDx5wq1Irknq_KdAx7gbnlACAstEFUyp85rrtHKu0MozhBpBup8_NQ-nHfJZBTb1tgJpmVTi4A1rKP6z580Xt02rgWeWdJLlR-vfRLsQ3i4zO9t7ElZkyGIYn-cH65OjDdAA7Ga7GPdXHZGmLw4bZI11CyEIK0kOkTMF-GqpDZDGw"
    if token:
        print("Access token retrieved successfully.")
        post_data_to_api(api_url, token, input_excel, output_excel, sheet_name)
    else:
        print("Failed to retrieve access token. Process terminated.")
