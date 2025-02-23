import pandas as pd
import requests
import json
import time
from GetAuthtoken import get_access_token

def post_data_to_api(post_api_url, access_token_bearer, input_excel_file, output_excel_file):
    # Read the Excel file
    df = pd.read_excel(input_excel_file)

    # Add a status column to track the response of each iteration
    df['Status'] = ''
    df['Response'] = ''  # Add a column to store the full response

    # Set up headers for the API request
    headers = {
        'Authorization': f'Bearer {access_token_bearer}',
        'Content-Type': 'application/json'
    }

    # Iterate through each row in the Excel file
    index: int
    for index, row in df.iterrows():
        print(f"Processing iteration {index + 1}...")

        # Construct the payload from the row data
        payload = {
            "data": {
                "yieldPerLocation": [
                    {
                        "data": {},
                        "locations": {
                            "bounds": {
                                "northeast": {
                                    "lat": row.iloc[4],  # Column 5: Latitude (Northeast)
                                    "lng": row.iloc[5]  # Column 6: Longitude (Northeast)
                                },
                                "southwest": {
                                    "lat": row.iloc[6],  # Column 7: Latitude (Southwest)
                                    "lng": row.iloc[7]  # Column 8: Longitude (Southwest)
                                }
                            },
                            "country": row.iloc[8],  # Column 9: Country
                            "administrativeAreaLevel3": "",
                            "administrativeAreaLevel1": "",
                            "placeId": "",
                            "latitude": row.iloc[9],  # Column 10: Latitude
                            "longitude": row.iloc[10],  # Column 11: Longitude
                            "geoInfo": {
                                "type": "FeatureCollection",
                                "features": [
                                    {
                                        "type": "Feature",
                                        "properties": {},
                                        "geometry": {
                                            "type": "Polygon",
                                            "coordinates": [
                                                json.loads(row.iloc[11])  # Column 12: Coordinates (JSON format)
                                            ]
                                        }
                                    }
                                ]
                            },
                            "name": row.iloc[12]  # Column 13: Location Name
                        },
                        "expectedYield": row.iloc[14],  # Column 15: Expected Yield
                        "expectedYieldQuantity": "",
                        "expectedYieldUnits": row.iloc[15],  # Column 16: Yield Units
                        "refrenceAreaUnits": row.iloc[16]  # Column 17: Reference Area Units
                    }
                ]
            },
            "cropId": row.iloc[17],  # Column 18: Crop ID
            "name": row.iloc[18],  # Column 19: Name
            "nickName": row.iloc[19],  # Column 20: Nickname
            "expectedHarvestDays": row.iloc[20],  # Column 21: Expected Harvest Days
            "processStandardDeduction": None,
            "cropPrice": None,
            "cropStages": [],
            "seedGrades": [],
            "harvestGrades": [],
            "id": None
        }

        # Make the POST request
        try:
            print(f"Adding crop variety {row.iloc[18]} to the API ...")
            response = requests.post(post_api_url, headers=headers, json=payload)
            # Record the status and full response of the request
            if response.status_code == 201:
                df.at[index, 'Status'] = 'Success'
                df.at[index, 'Response'] = f"Code: {response.status_code}, Message: {response.text}"
                print(f"Added crop variety {row.iloc[18]} successfully to the API ...")
            else:
                df.at[index, 'Status'] = f"Failed: {response.status_code}"
                df.at[
                    index, 'Response'] = f"Reason: {response.reason}, Message: {response.text}"
        except Exception as e:
            df.at[index, 'Status'] = "Error"
            df.at[index, 'Response'] = str(e)

        # Wait for 1 second before the next iteration
        time.sleep(0.2)

    # Save the updated DataFrame with status to a new Excel file
    df.to_excel(output_excel_file, index=False)

# Inputs
api_url = "https://ca-v2-gcp.cropin.co.in/qa6/services/farm/api/varieties"
input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\addCropVarieties.xlsx"
output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\AddCropVarietiesOutput.xlsx"
tenant_code = "asp"
environment = "prod1"

# token = get_access_token(tenant_code, "9649964096", "123456", environment)
token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJqZ1JBYTkxSVg5RUxveExYdlp0S0RLTVlJbmhlY20zdHZCNW1GMm5YZGhFIn0.eyJleHAiOjE3NDAwMzI5NTcsImlhdCI6MTczOTk0NjU1NywiYXV0aF90aW1lIjoxNzM5OTQ2NTMzLCJqdGkiOiIxZDkwM2VhYy1kOWRjLTRjNzctYWNlNS02YWJhNmQ2ODY1ZDgiLCJpc3MiOiJodHRwczovL3Yyc3NvLWdjcC5jcm9waW4uY28uaW4vYXV0aC9yZWFsbXMvcWF6b25lNiIsImF1ZCI6WyJyZXNvdXJjZV9zZXJ2ZXIiLCJhY2NvdW50Il0sInN1YiI6ImRmOThiNTI5LTg1Y2YtNGU0Ny1hNmQyLWJjYjc2ZDVlMDdjMSIsInR5cCI6IkJlYXJlciIsImF6cCI6IndlYl9hcHAiLCJub25jZSI6Il9jbGllbnRfd2ViX2FwcCIsInNlc3Npb25fc3RhdGUiOiJiYzRkNTc4Yy0zN2UzLTQ1NDQtYWJlOC0wNzQ5NDY2OThmMzIiLCJhY3IiOiIwIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJyZXNvdXJjZV9zZXJ2ZXIiOnsicm9sZXMiOlsiUk9MRV9BRE1JTl8xMzUxIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIHBob25lIG1pY3JvcHJvZmlsZS1qd3Qgb2ZmbGluZV9hY2Nlc3MgYWRkcmVzcyIsInNpZCI6ImJjNGQ1NzhjLTM3ZTMtNDU0NC1hYmU4LTA3NDk0NjY5OGYzMiIsInVzZXJfcm9sZSI6WyJST0xFX0FETUlOXzEzNTEiXSwidXBuIjoiNzM4MjIxMTIzMSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiYWRkcmVzcyI6e30sImdyb3VwcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl0sInByZWZlcnJlZF91c2VybmFtZSI6IjczODIyMTEyMzEiLCJlbWFpbCI6InJhamFAZ21haWwuY29tIiwidGVuYW50IjoicWF6b25lNiJ9.MDqlXLOXHgQpc4pJTn8IIPAXaddFKljX7z9CdtlX815SH5xTTXy8YqlTYMdDjku_vBF08atbTABUDYG3IAHGcZWq61dQxtKmWJ840yRKLrQ7pQ-6HvKPb8nKmzBkaddAdUyCEWLpfwFCTEsoDlZKD8-8q16i4r9F4n-cFRbb8annmF3VVrzlVOpRDqt0ai0ZfICnTClYhkwRbEwDxjnTfHG54aXiJWd6KWBkF4ywk-GyRAumGsdXmm_MXNqu3wFGfznBzCJ8Ptn1_tLx3qCPE9FvqfCRIgpMVBVziVXV8fmg28NhlCc-ZjgLJc6OHN5kMY7v2b4Ce6fvy0s3CGv8ew"
if token:
    post_data_to_api(api_url, token, input_excel, output_excel)
else:
    print("Failed to retrieve access token. Process terminated.")
