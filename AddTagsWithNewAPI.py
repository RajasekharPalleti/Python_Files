import pandas as pd
import requests
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
              "name" : row.iloc[0],
              "tagType" : row.iloc[1],
              "validFrom" : row.iloc[2],
              "validTill" : row.iloc[3],
              "description" : row.iloc[4],
              "status" : "Active",
            }

        # Make the POST request
        try:
            print(f"Adding Tag {row.iloc[0]} to the API ...")
            response = requests.post(post_api_url, headers=headers, json=payload)
            # Record the status and full response of the request
            if response.status_code == 201:
                df.at[index, 'Status'] = 'Success'
                df.at[index, 'Response'] = f"Code: {response.status_code}, Message: {response.text}"
                print(f"Added Tag {row.iloc[0]} successfully to the API ...")
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
api_url = "https://af-v2-gcp.cropin.co.in/qa5/services/master/api/tags"
input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\NewTagsAdding.xlsx"
output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\NewTagsAdding_Output.xlsx"
tenant_code = "sakataseeds"
environment = "prod1"

#token = get_access_token(tenant_code, "2024202501", "Cropin12345", environment)
token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJDeVNZVFNPWW95Q19DcENmaTNNZHBTdHFvd0VPczFBS0ZSZE1YMXVVYkgwIn0.eyJleHAiOjE3NTAwNzQ1MTksImlhdCI6MTc1MDA1MjkxOSwiYXV0aF90aW1lIjoxNzUwMDUyOTEwLCJqdGkiOiIzNTM2YjQ0My03NjQyLTQzZTYtOGFiMS1lZTRkYTBkYmY1OTMiLCJpc3MiOiJodHRwczovL3Yyc3NvLWdjcC5jcm9waW4uY28uaW4vYXV0aC9yZWFsbXMvcWF6b25lNSIsImF1ZCI6WyJyZXNvdXJjZV9zZXJ2ZXIiLCJhY2NvdW50Il0sInN1YiI6IjM5OWQ3MWEyLWVjZGUtNGZiMi04ZDhmLTY1YjY2MDEwZDM1YSIsInR5cCI6IkJlYXJlciIsImF6cCI6IndlYl9hcHAiLCJub25jZSI6Il9jbGllbnRfd2ViX2FwcCIsInNlc3Npb25fc3RhdGUiOiI4ZTU5OTg5Mi1kZGU0LTRhNTUtYTBiYy0zMmI5ZTk0ZGZjZjciLCJhY3IiOiIwIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJyZXNvdXJjZV9zZXJ2ZXIiOnsicm9sZXMiOlsiUmFqYSBUZXN0IHVzZXIgcm9sZV8zODUxIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBlbWFpbCBwcm9maWxlIG9mZmxpbmVfYWNjZXNzIHBob25lIGFkZHJlc3MgbWljcm9wcm9maWxlLWp3dCIsInNpZCI6IjhlNTk5ODkyLWRkZTQtNGE1NS1hMGJjLTMyYjllOTRkZmNmNyIsInVzZXJfcm9sZSI6WyJSYWphIFRlc3QgdXNlciByb2xlXzM4NTEiXSwidXBuIjoiNzM4ODg4ODg4OCIsImFkZHJlc3MiOnt9LCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImdyb3VwcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl0sInByZWZlcnJlZF91c2VybmFtZSI6IjczODg4ODg4ODgiLCJ0ZW5hbnQiOiJxYXpvbmU1IiwiZW1haWwiOiJyYWphdGVzdHFhNUBnbWFpbC5jb20ifQ.DxhRKZjivbwPneuPEFAqnwzFv-HlNQaYLzsZy-xr_qJC34XJmuV5Kyn6k-DXo9OxxGxWphdEE-OsZsnJX2tUXi3tEMkaweg8wdqesDpONgb_bx9m2hfnBuUIg-Q4QFNMHjVvBdFNLuwF0qr7ZOaDDnJrl7jqu6615Ap-nKQwD7AVi0NzoEymy8oM73EAmJAiswYesMT6A49L2AHZTAAXG_E7l3tGop-BeTqTNSOYHk3xvsQLPK8THK96xIBP-mSq5PGCQWLU6-UfrzDgU9ck2BKH5NYlTkma_TUfgwYH60V01NPYJE5l_fXMyL8j-cf86EfpyOCpdsJJNcgBfR-3Ug"

if token:
    post_data_to_api(api_url, token, input_excel, output_excel)
else:
    print("Failed to retrieve access token. Process terminated.")
