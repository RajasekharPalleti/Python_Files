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
                        "expectedYield": row.iloc[13],  # Column 14: Expected Yield
                        "expectedYieldQuantity": "",
                        "expectedYieldUnits": row.iloc[15],  # Column 16: Yield Units
                        "refrenceAreaUnits": row.iloc[16]  # Column 17: Reference Area Units
                    }
                ]
            },
            "cropId": row.iloc[17],  # Column 18: Crop ID
            "name": row.iloc[19],  # Column 20: Name
            "nickName": row.iloc[20],  # Column 21: Nickname
            "expectedHarvestDays": row.iloc[21],  # Column 22: Expected Harvest Days
            "processStandardDeduction": None,
            "cropPrice": None,
            "cropStages": [],
            "seedGrades": [],
            "harvestGrades": [],
            "id": None,
            "varietyAdditionalAttributeList": []
        }

        # Add parentId which is variety id only if data is available
        variety_id = row.iloc[18] # Column 19: Parent ID
        if not (pd.isna(variety_id) or str(variety_id).strip() == ''):
            payload['parentId'] = variety_id

        # Make the POST request
        try:
            print(f"Adding crop variety {row.iloc[19]} to the API ...")
            response = requests.post(post_api_url, headers=headers, json=payload)
            # Record the status and full response of the request
            if response.status_code == 201:
                df.at[index, 'Status'] = 'Success'
                df.at[index, 'Response'] = f"Code: {response.status_code}, Message: {response.text}"
                print(f"Added crop variety {row.iloc[19]} successfully to the API ...")
            else:
                df.at[index, 'Status'] = f"Failed: {response.status_code}"
                df.at[
                    index, 'Response'] = f"Reason: {response.reason}, Message: {response.text}"
        except Exception as e:
            df.at[index, 'Status'] = "Error"
            df.at[index, 'Response'] = str(e)

        # Wait for 0.5 second before the next iteration
        time.sleep(0.5)

    # Save the updated DataFrame with status to a new Excel file
    print("Saving updated DataFrame to a new Excel file...")
    df.to_excel(output_excel_file, index=False)

# Inputs
api_url = "https://cloud.cropin.in/services/farm/api/varieties"
input_excel = r"C:\Users\rajasekhar.palleti\Downloads\Add_Variety_Test.xlsx"
output_excel = r"C:\Users\rajasekhar.palleti\Downloads\Add_Variety_Test_output.xlsx"
tenant_code = "asp"
environment = "prod1"

token = get_access_token(tenant_code, "9649964096", "123456", environment)
# token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJaS08wSFZ2OGlVVmxzQTl6THFBUjhEOWVMc3NYNlVYTERWRkUzdkJ1N0lJIn0.eyJqdGkiOiIyOTM3MmExZS1hZDVlLTRlOWMtOThmZS0wOWYzOGUwMjRlZDgiLCJleHAiOjE3NDAzMDAxMTgsIm5iZiI6MCwiaWF0IjoxNzQwMTI3MzE4LCJpc3MiOiJodHRwczovL3Yyc3NvLXVhdC1nY3AuY3JvcGluLmNvLmluL2F1dGgvcmVhbG1zL3VhdHpvbmUxIiwiYXVkIjpbInJlc291cmNlX3NlcnZlciIsImFjY291bnQiXSwic3ViIjoiMTZlYjliYWQtY2Y1OS00N2Y0LWI2YzAtYjdhNTAxNjEzYjhlIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoid2ViX2FwcCIsIm5vbmNlIjoiX2NsaWVudF93ZWJfYXBwIiwiYXV0aF90aW1lIjoxNzQwMTI3MzE4LCJzZXNzaW9uX3N0YXRlIjoiYjk5YmY2YTUtNDg2NS00ZDUyLTgwYTYtZjc2OTk2NGE3ZDU4IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsicmVzb3VyY2Vfc2VydmVyIjp7InJvbGVzIjpbIlJhamEgUm9sZV9BZG1pbiByb2xlXzM2MjYwMSJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgbWljcm9wcm9maWxlLWp3dCBwaG9uZSBwcm9maWxlIG9mZmxpbmVfYWNjZXNzIGVtYWlsIGFkZHJlc3MiLCJ1c2VyX3JvbGUiOlsiUmFqYSBSb2xlX0FkbWluIHJvbGVfMzYyNjAxIl0sInVwbiI6IjczODIyMTEzMDIiLCJhZGRyZXNzIjp7fSwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJncm91cHMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiI3MzgyMjExMzAyIiwidGVuYW50IjoidWF0em9uZTEiLCJlbWFpbCI6InJhamFzZWtoYXIucGFsbGV0aTFAY3JvcGluLmNvbSJ9.U36jTyQAoBPfn2wJMS6cPqcIit0XhzMtiuAnFeBSO6EYRi5ygljd8qielSL_DboZ-zLQphEgZp_FoV2LDbYxy5CLF_EEvnsPy4VHOJPF42Thv45OBBglmy0jKhOgsIrS8YVFjPWCr6bYnA0scHTAa1fihdEgQXiJBas8-G6PuJsNMZiXq7dGPZ-zEehHMKSIBiM9EE0uenAuAFLsnm_nkcs20SHOA90gMUhsNRqrfAHg3iEuTF2XVGh3ulz-x8s_hifT1Q-VNb0pfffKoxhDBvRnGTzxXcQqWPe6jpjhchIQWFcU-UaaytbHa8xHK_4P6Qg4jVfXeso0xjGg1X-_hw"
if token:
    post_data_to_api(api_url, token, input_excel, output_excel)
else:
    print("Failed to retrieve access token. Process terminated.")
