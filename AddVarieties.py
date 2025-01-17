import pandas as pd
import requests
import json
import time


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
            response = requests.post(post_api_url, headers=headers, json=payload)
            # Record the status and full response of the request
            if response.status_code == 201:
                df.at[index, 'Status'] = 'Success'
                df.at[index, 'Response'] = f"Code: {response.status_code}, Message: {response.text}"
            else:
                df.at[index, 'Status'] = f"Failed: {response.status_code}"
                df.at[
                    index, 'Response'] = f"Reason: {response.reason}, Message: {response.text}"
        except Exception as e:
            df.at[index, 'Status'] = "Error"
            df.at[index, 'Response'] = str(e)

        # Wait for 1 second before the next iteration
        time.sleep(1)

    # Save the updated DataFrame with status to a new Excel file
    df.to_excel(output_excel_file, index=False)

# Inputs
api_url = "https://cloud.cropin.in/services/farm/api/varieties"
access_token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJRY2lZNERNaUpJRnNqWWx5V1VIaUpjb2x2cUU0X25MQ19HbFlGRGZwb2cwIn0.eyJqdGkiOiI3MDFhYmE1NS0xOTc0LTRhZjAtYWE1ZC02NDY2ODE2Njk1OGUiLCJleHAiOjE3Mzg0MDEwNzUsIm5iZiI6MCwiaWF0IjoxNzM3MTA1MDc1LCJpc3MiOiJodHRwczovL3Nzby5zZy5jcm9waW4uaW4vYXV0aC9yZWFsbXMvc2FrYXRhc2VlZHMiLCJhdWQiOlsicmVzb3VyY2Vfc2VydmVyIiwiYWNjb3VudCJdLCJzdWIiOiJkYTcwNjY2Ni1kNjI4LTQ2NDEtYjFkNi1lZTZhMjM0NjkzYTciLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJ3ZWJfYXBwIiwibm9uY2UiOiJfY2xpZW50X3dlYl9hcHAiLCJhdXRoX3RpbWUiOjE3MzcxMDUwNzUsInNlc3Npb25fc3RhdGUiOiI1ZjhlMDZhZS0xZTU4LTQ1OWItYmQ5ZC1mMWFlZjU3MDU3ODEiLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJyZXNvdXJjZV9zZXJ2ZXIiOnsicm9sZXMiOlsiUk9MRV9BRE1JTl8xNDAxIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBvZmZsaW5lX2FjY2VzcyBlbWFpbCBtaWNyb3Byb2ZpbGUtand0IHByb2ZpbGUgcGhvbmUgYWRkcmVzcyIsInVzZXJfcm9sZSI6WyJST0xFX0FETUlOXzE0MDEiXSwidXBuIjoiMjAyNDIwMjUwMSIsImFkZHJlc3MiOnt9LCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsIm5hbWUiOiJTYWthdGEgQWRtaW4iLCJncm91cHMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiIyMDI0MjAyNTAxIiwiZ2l2ZW5fbmFtZSI6IlNha2F0YSBBZG1pbiIsImVtYWlsIjoieWVya2FAdGVzdC5jb20iLCJ0ZW5hbnQiOiJzYWthdGFzZWVkcyJ9.exPthFQJspZNdUbkSgTlsbDwTw-_y0YvSWbRYQQyk_46PKkVSfCqRCAoRH8zKRIxhikyBWJrRgiHeQ8RzKam7nHj_2mCDE3ZE0cqUlitTeL88QNkGJ6BYLZYd86rEWm1EyOH8EBUG3-6fA3Ig5RIkNOjbTGXQueUVSSCSJAyaDelencMyTGtScQObV12jb94RUKJLPKsUVM8NH_u8MQWJ6i3vu4j8kTq6s7EpvFM6-N-mtebIqTcOwYGnwLjEM8wUpSEUDb8YmmZlzwTgFekSRrEstoOOaKARwj8VfQU875l4dFawYiqTD2LkOFBH9988Y9u9zKUJdszdlRbXHtwVw"
input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\AddCropVarieties.xlsx"
output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\AddCropVarietiesOutput.xlsx"

post_data_to_api(api_url, access_token, input_excel, output_excel)
