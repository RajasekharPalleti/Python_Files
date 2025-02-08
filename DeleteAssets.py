import pandas as pd
import requests
import time

# File path
file_path = "C:\\Users\\rajasekhar.palleti\\Downloads\\assets_data.xlsx"
# Access token
access_token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJLMDB0YV9CX3Q0Z3NUUDN5dngtUlpmTHZaZGNkQnJfSmwzQ0JVbEtXREdzIn0.eyJqdGkiOiIwM2ZkMDdmNS04MjI1LTRlNDktOWU2MS1lM2ZmZGMyMjRlMmEiLCJleHAiOjE3MzcxODk1MTcsIm5iZiI6MCwiaWF0IjoxNzM1ODkzNTE3LCJpc3MiOiJodHRwczovL3Nzby5zZy5jcm9waW4uaW4vYXV0aC9yZWFsbXMvY2luaWpoIiwiYXVkIjpbInJlc291cmNlX3NlcnZlciIsImFjY291bnQiXSwic3ViIjoiOGZkZDljZmQtZDM1ZC00MTNkLWExMjAtNDkxMzZlNDkyNjc5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoid2ViX2FwcCIsIm5vbmNlIjoiX2NsaWVudF93ZWJfYXBwIiwiYXV0aF90aW1lIjoxNzM1ODkzNTE3LCJzZXNzaW9uX3N0YXRlIjoiYjk4Njk4NTItMzEwNC00YjQ1LThkZWQtZjczMDFiMDRhOTkyIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsicmVzb3VyY2Vfc2VydmVyIjp7InJvbGVzIjpbIlJPTEVfQURNSU5fMTM1MSJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgbWljcm9wcm9maWxlLWp3dCBhZGRyZXNzIHBob25lIHByb2ZpbGUgZW1haWwgb2ZmbGluZV9hY2Nlc3MiLCJ1c2VyX3JvbGUiOlsiUk9MRV9BRE1JTl8xMzUxIl0sInVwbiI6IjU5NTk1OTU5NTkiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImFkZHJlc3MiOnt9LCJuYW1lIjoiUyBQYXVsIiwiZ3JvdXBzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXSwicHJlZmVycmVkX3VzZXJuYW1lIjoiNTk1OTU5NTk1OSIsImdpdmVuX25hbWUiOiJTIFBhdWwiLCJ0ZW5hbnQiOiJjaW5pamgiLCJlbWFpbCI6InNwYXVsQHRhdGF0cnVzdHMub3JnIn0.YzhQ_l5UMcyMxRxnxRJDlJjLssY9M8KLgPKlbleo8lRJg3zTcz5552FHtu5PId5z-nWtJDHGLGCL3MD9F28G5hPDFooC9ZCE-nUuxHzhiyL4zmRieK1qSZDu7PHjC-7XEMD-W1fkIzsBcj9jnkmIDAt1t6zAtvDvGAZo-z7VNbJ_A8Jn-3C4_gMjT_F2KBUvKZ2uX31XtdXZM17I0Bf0NTcVa3S7yp_IRsBZsEpYYeXkgeQ5keuNxjMPAieAlKdtzhMopP3cu-59w4tRkf5DbSPnLtuBz_mIFYJAGbV-pFenInRb7MsyexnezIwUjAoxUEWr5Qv0BTyEjsfcsqh28Q"
# Define API URL
api_url = "https://cloud.cropin.in/services/farm/api/assets/bulk"

# Function to send DELETE requests in bulk
def delete_assets(file_path, access_token):
    # Read Excel file
    df = pd.read_excel(file_path)

    # Check if the required column exists
    if 'assetIDs' not in df.columns:
        print("Error: 'assetIDs' column is missing in the Excel file.")
        return

    # Prepare the DataFrame to store results
    df['Status'] = ''
    df['Processed IDs'] = ''

    # Extract asset IDs
    asset_ids = df['assetIDs'].tolist()

    # Define headers with access token
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    # Process asset IDs in chunks of 100
    for iteration, i in enumerate(range(0, len(asset_ids), 100), start=1):
        chunk = asset_ids[i:i + 100]
        ids_param = ','.join(map(str, chunk))

        print(f"Iteration {iteration}: Starting processing for chunk: {ids_param}")

        try:
            # Send DELETE request
            response = requests.delete(f"{api_url}?ids={ids_param}", headers=headers)

            if response.status_code == 200:
                result = response.json()
                status = 'Success'
                processed_ids = ','.join(map(str, chunk))
            else:
                status = f"Failed: {response.status_code}"
                processed_ids = ','.join(map(str, chunk))

        except Exception as e:
            status = f"Error: {e}"
            processed_ids = ','.join(map(str, chunk))

        # Update DataFrame
        df.loc[df['assetIDs'].isin(chunk), 'Status'] = status
        # Save processed IDs only in the first row of the chunk
        if chunk:
            df.loc[df['assetIDs'] == chunk[0], 'Processed IDs'] = processed_ids

        # Save intermediate results
        df.to_excel(file_path, index=False)

        print(f"Iteration {iteration}: Completed processing for chunk: {ids_param}")

        # Wait for 1 second before the next iteration
        time.sleep(1)

    print("Processing complete. Results saved to", file_path)


# Call the function
delete_assets(file_path, access_token)
