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
                "unit": "Acre" #As per the user preferred unit
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
    # token = get_access_token("asp", "9649964096", "123456", "prod1")
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJlQnI0SDh0NmF1UjZydE9hTFVqZWpPNmprcmszbEpnRDhlbl81UzRncFFvIn0.eyJleHAiOjE3NTUwMDE4NTcsImlhdCI6MTc1NDk4NzQ1NywiYXV0aF90aW1lIjoxNzU0OTg3NDM3LCJqdGkiOiJiNjk2OWY2My0yNGI3LTQ3OTctODA1MC03YTEzYmNiZWVhZmEiLCJpc3MiOiJodHRwczovL3Yyc3NvLWdjcC5jcm9waW4uY28uaW4vYXV0aC9yZWFsbXMvcWF6b25lMiIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiI1NzgwY2E2NC0xOGRjLTQwYjAtYTdmMC00ODE3NzZjNTRjNWMiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJ3ZWJfYXBwIiwibm9uY2UiOiJfY2xpZW50X3dlYl9hcHAiLCJzZXNzaW9uX3N0YXRlIjoiYzIzYjkzYzctYTM3Yy00YjIwLThkNjUtM2NhNjk0OTFjMmU2IiwiYWNyIjoiMCIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsImRlZmF1bHQtcm9sZXMtcWF6b25lMiIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgcGhvbmUgcHJvZmlsZSBhZGRyZXNzIGVtYWlsIG9mZmxpbmVfYWNjZXNzIG1pY3JvcHJvZmlsZS1qd3QiLCJzaWQiOiJjMjNiOTNjNy1hMzdjLTRiMjAtOGQ2NS0zY2E2OTQ5MWMyZTYiLCJ1cG4iOiI3MzgyMjEwNDAzIiwiYWRkcmVzcyI6e30sImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiZ3JvdXBzIjpbIm9mZmxpbmVfYWNjZXNzIiwiZGVmYXVsdC1yb2xlcy1xYXpvbmUyIiwidW1hX2F1dGhvcml6YXRpb24iXSwicHJlZmVycmVkX3VzZXJuYW1lIjoiNzM4MjIxMDQwMyIsInRlbmFudCI6InFhem9uZTIifQ.mLRVuY6CR_uAbNnezSdyxA2ojfknZuvuXnQZE0fa8X5u_ai3i22eqohOsCTCS0p-LqcVw1Gy7Zy8EWIQTBpCSxlRx6h-DfWVhKEeyMpVUcTZKQX9hU2MJIgKmyXkRB9Cg7kwVi5Oeahae_un6HCxHv1kXm8ljJXhUCfuiupowezYZrZ447diZRmmelNMw130qAo635TeLcl57njS11gqxFbsCvZ9ZBVSxDFanMp6L_f8d5Ecl9DDh8axINQmWIS7GIiPzP_UcjnzUur5qWSVFMgCJgRJ0ckFqybh4gq4CnXcHuzKyWj1IR_thkcoBR0SjU5iB1i28WG1AqjUWD_8tA"
    if token:
        print("Access token retrieved successfully.")
        post_data_to_api(api_url, token, input_excel, output_excel, sheet_name)
    else:
        print("Failed to retrieve access token. Process terminated.")
