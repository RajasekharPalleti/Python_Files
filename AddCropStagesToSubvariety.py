import pandas as pd
import requests
import time

# Input data
crop_stages = [{"name":"Planting","description":"Planting","daysAfterSowing":0,"variety":None,"formId":None,"clientId":"7ccf3faf-43cb-4a88-84a9-cf979430a0e3","id":13602},{"name":"Flowering","description":"Flowering","daysAfterSowing":40,"variety":None,"formId":None,"clientId":"32402d35-c3c2-455f-ba65-ff9876410d59","id":13652},{"name":"Senescing","description":"Senescing","daysAfterSowing":60,"variety":None,"formId":None,"clientId":"0d77d847-eccc-4aa8-acf3-cfa634fbe21b","id":13702},{"name":"Desication","description":"Desication","daysAfterSowing":70,"variety":None,"formId":None,"clientId":"2b0500aa-3415-4586-8e8e-fedf70ec94d2","id":13653},{"name":"Harvest","description":"Harvest","daysAfterSowing":100,"variety":None,"formId":None,"clientId":"7fe2bf38-e006-442f-8370-5973645d7913","id":13752}]

# API endpoints
get_api_url = "https://cloud.cropin.in/services/farm/api/varieties"
put_api_url = "https://cloud.cropin.in/services/farm/api/varieties"

# Read Excel file
file_path = "C:\\Users\\rajasekhar.palleti\\Downloads\\CropVarietyIDs.xlsx"  # Replace with the actual file path
data = pd.read_excel(file_path)

# Ensure there is a column for status
data["status"] = None

# Access token (replace with the actual token)
access_token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI5SGpBdnNLdFZYVDdYczNjbGNWb25VbFE2OXBwZDRtUlhUa2ozODdzcTQwIn0.eyJqdGkiOiIxY2M0NTUyYy05MGRmLTQ1YTQtODM1NS1lYjA1N2Y1ODI0ZTkiLCJleHAiOjE3Mzc3MjU0OTEsIm5iZiI6MCwiaWF0IjoxNzM2NDI5NDkxLCJpc3MiOiJodHRwczovL3Nzby5zZy5jcm9waW4uaW4vYXV0aC9yZWFsbXMvZWxkZXJzIiwiYXVkIjpbInJlc291cmNlX3NlcnZlciIsImFjY291bnQiXSwic3ViIjoiZGI0Zjg5YjMtODk2Ni00YjY5LTllZjEtNGFhYjgzOWI2NWE5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoid2ViX2FwcCIsIm5vbmNlIjoiX2NsaWVudF93ZWJfYXBwIiwiYXV0aF90aW1lIjoxNzM2NDI5NDkxLCJzZXNzaW9uX3N0YXRlIjoiMzZmNWZiZDMtYmY0OS00N2UyLWFhZGUtZTJkMjlmMzhhNDNiIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsicmVzb3VyY2Vfc2VydmVyIjp7InJvbGVzIjpbIlJPTEVfQURNSU5fMTM1MSJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgbWljcm9wcm9maWxlLWp3dCBwaG9uZSBvZmZsaW5lX2FjY2VzcyBwcm9maWxlIGVtYWlsIGFkZHJlc3MiLCJ1c2VyX3JvbGUiOlsiUk9MRV9BRE1JTl8xMzUxIl0sInVwbiI6Ijk5OTk5MDAwMDAiLCJhZGRyZXNzIjp7fSwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJuYW1lIjoiQ2hyaXMgV2lsbGlzIiwiZ3JvdXBzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXSwicHJlZmVycmVkX3VzZXJuYW1lIjoiOTk5OTkwMDAwMCIsImdpdmVuX25hbWUiOiJDaHJpcyBXaWxsaXMiLCJlbWFpbCI6ImNocmlzLndpbGxpc0BlbGRlcnMuY29tLmF1IiwidGVuYW50IjoiZWxkZXJzIn0.Qz9AvQmuMlPRFmsfUHVCCsoZCmaWkTyV54KSBN0pWF6iV4Rqr1OLfCD4Vzazs1Q4wGOL_Y4fVb4Pi7kKF42vtXRe7-7RMfPsxFSslLR0r8qBPnvRvuUJWBUHhzoqJugRpxQchFGFTXitIW1PNhs4_qH9M0kyqN9Yv7hzjX9ioZ5RGHY9aHzYqXkkohZSSC7KqWO1pSlbEaJ39GNCUq8nJ0CMTcShF6MlsaFOukrwuEHXH2QB_xIC9tZFF0Mjj7mZCPo_R1xhoB4KnGyTr1S7X2xzoV_HCM11El9Fdyabw1rDlAuVlRkwPPYz7IIyf0py0lO9RCTcwi11MgZllGRXcg"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# Iterate through each row in the Excel file
for index, row in data.iterrows():
    variety_id = row["VarietyID"]  # Replace with the correct column name for IDs

    if pd.isna(variety_id):
        continue

    iterations = 0
    print(f"Processing ID: {iterations+1}")
    print(f"Processing ID: {variety_id}")

    # GET request
    response = requests.get(f"{get_api_url}/{variety_id}", headers=headers)
    if response.status_code != 200:
        print(f"GET request failed for ID {variety_id}: {response.status_code}")
        data.at[index, "status"] = f"GET failed: {response.status_code}"
        continue

    variety_data = response.json()

    # Modify the cropStages in the response
    if "cropStages" in variety_data:
        variety_data["cropStages"] = crop_stages

    # PUT request
    put_response = requests.put(put_api_url, headers=headers, json=variety_data)
    if put_response.status_code == 200:
        print(f"PUT request succeeded for ID {variety_id}")
        data.at[index, "status"] = "Success"
    else:
        print(f"PUT request failed for ID {variety_id}: {put_response.status_code}")
        data.at[index, "status"] = f"PUT failed: {put_response.status_code}"

    # Wait for 1 second before the next iteration
    time.sleep(1)

# Save the updated Excel file
data.to_excel(file_path, index=False)

print("Process completed. Updated statuses are saved in the Excel file.")
