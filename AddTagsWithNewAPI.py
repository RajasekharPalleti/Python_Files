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
api_url = "https://v2uat-gcp.cropin.co.in/uat1/services/master/api/tags"
input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\NewTagsAdding.xlsx"
output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\NewTagsAdding_Output.xlsx"
tenant_code = "sakataseeds"
environment = "prod1"

#token = get_access_token(tenant_code, "2024202501", "Cropin12345", environment)
token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJaS08wSFZ2OGlVVmxzQTl6THFBUjhEOWVMc3NYNlVYTERWRkUzdkJ1N0lJIn0.eyJqdGkiOiI2MDVjNTc2OS05ODFkLTQ4NjQtYmU5MC03NTg0MjdhOGQ4MTYiLCJleHAiOjE3NTE0NjQzMjMsIm5iZiI6MCwiaWF0IjoxNzUxMjkxNTIzLCJpc3MiOiJodHRwczovL3Yyc3NvLXVhdC1nY3AuY3JvcGluLmNvLmluL2F1dGgvcmVhbG1zL3VhdHpvbmUxIiwiYXVkIjpbInJlc291cmNlX3NlcnZlciIsImFjY291bnQiXSwic3ViIjoiMTZlYjliYWQtY2Y1OS00N2Y0LWI2YzAtYjdhNTAxNjEzYjhlIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoid2ViX2FwcCIsIm5vbmNlIjoiX2NsaWVudF93ZWJfYXBwIiwiYXV0aF90aW1lIjoxNzUxMjkxNTIzLCJzZXNzaW9uX3N0YXRlIjoiMjFjZjVjNWItYWI2Yi00ZDU2LTlhMjAtYWQ4Y2U2NTliMmY2IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsicmVzb3VyY2Vfc2VydmVyIjp7InJvbGVzIjpbIlJhamEgUm9sZV9BZG1pbiByb2xlXzM2MjYwMSJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgbWljcm9wcm9maWxlLWp3dCBwaG9uZSBwcm9maWxlIG9mZmxpbmVfYWNjZXNzIGVtYWlsIGFkZHJlc3MiLCJ1c2VyX3JvbGUiOlsiUmFqYSBSb2xlX0FkbWluIHJvbGVfMzYyNjAxIl0sInVwbiI6IjczODIyMTEzMDIiLCJhZGRyZXNzIjp7fSwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJncm91cHMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiI3MzgyMjExMzAyIiwidGVuYW50IjoidWF0em9uZTEiLCJlbWFpbCI6InJhamFzZWtoYXIucGFsbGV0aTFAY3JvcGluLmNvbSJ9.AGkI0ty_NkNLJ7m65t4TTbrtoiuy3RF2egRCA2ZMViKvixcDjYqHiSYaFUYq8C87wEQH0a0QWJN2WySrLuJDq01rg3s2WnTGgErh42y2ym93UY4oNBC5xNSpXV12j6SbUsoF3Q8xNuZceoho9cXAj8UNBsCYtDAt6TS2WR8cGl4a8YJowMfwClqsPZDS93p3UtYIW64HT-TA8pwfz0fjw7dQSq0dmeImq8hqK9tjZNYcZR0Va_qzy1DtihNxOWybKSJkZMpNJ4Dm5CTCNDJa4-rJr1sd3fJ_1SX9YmXA7ONC7RSo3QU959cc-oJj-6JW8pLHuEAZKb7yjcibJ-y1Ng"

if token:
    post_data_to_api(api_url, token, input_excel, output_excel)
else:
    print("Failed to retrieve access token. Process terminated.")
