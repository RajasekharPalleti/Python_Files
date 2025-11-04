#Author: Rajasekhar Palleti

import pandas as pd
import requests
import time
from GetAuthtoken import get_access_token

# File path
file_path = "C:\\Users\\rajasekhar.palleti\\Downloads\\assets_data.xlsx"

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
token = get_access_token("asp","9649964096", "123456", "prod1")  # Ensure you have a function to get the access token
if token:
    print("token retireved successfully")
    delete_assets(file_path, token)
else:
    print("Failed to retrieve access token.")
