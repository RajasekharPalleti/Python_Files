#Author: Rajasekhar Palleti
import pandas as pd
import requests
import time
from GetAuthtoken import get_access_token

# File path
file_path = "C:\\Users\\rajasekhar.palleti\\Downloads\\farmers_data.xlsx"  # update if different

# Define API URL for bulk farmer delete
api_url = "https://cloud.cropin.in/services/farm/api/farmers/bulk"

# Function to send DELETE requests in bulk for farmers
def delete_farmers(file_path, access_token):
    # Read Excel file
    df = pd.read_excel(file_path)

    # Check if the required column exists
    if 'farmerIDs' not in df.columns:
        print("Error: 'farmerIDs' column is missing in the Excel file.")
        return

    # Prepare the DataFrame to store results
    if 'Status' not in df.columns:
        df['Status'] = ''
    if 'Processed IDs' not in df.columns:
        df['Processed IDs'] = ''
    if 'Response' not in df.columns:
        df['Response'] = ''

    # Extract farmer IDs (keep as list of single values per row)
    farmer_ids = df['farmerIDs'].tolist()

    # Normalize IDs: ensure string, strip whitespace, ignore empties
    farmer_ids = [str(x).strip() for x in farmer_ids if pd.notna(x) and str(x).strip() != '']

    # Define headers with access token
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    # Process farmer IDs in chunks of 100
    for iteration, start in enumerate(range(0, len(farmer_ids), 100), start=1):
        chunk = farmer_ids[start:start + 100]
        ids_param = ','.join(map(str, chunk))

        print(f"Iteration {iteration}: Starting processing for chunk: {ids_param}")

        try:
            # Send DELETE request to bulk endpoint
            response = requests.delete(f"{api_url}?ids={ids_param}", headers=headers, timeout=120)

            if response.status_code in (200, 204):
                status = 'Success'
                processed_ids = ids_param
                resp_text = response.text
            else:
                status = f"Failed: {response.status_code}"
                processed_ids = ids_param
                resp_text = f"StatusCode: {response.status_code}, Body: {response.text}"

        except Exception as e:
            status = f"Error: {e}"
            processed_ids = ids_param
            resp_text = str(e)

        # Update DataFrame rows that belong to this chunk
        df.loc[df['farmerIDs'].astype(str).str.strip().isin(chunk), 'Status'] = status

        # Save processed IDs only in the first row of the chunk
        if chunk:
            first_id = chunk[0]
            df.loc[df['farmerIDs'].astype(str).str.strip() == first_id, 'Processed IDs'] = processed_ids
            df.loc[df['farmerIDs'].astype(str).str.strip() == first_id, 'Response'] = resp_text

        # Save intermediate results
        try:
            df.to_excel(file_path, index=False)
        except Exception as save_err:
            print(f"Warning: could not save Excel file at this moment: {save_err}")

        print(f"Iteration {iteration}: Completed processing for chunk: {ids_param}")

        # Wait for 1 second before the next iteration
        time.sleep(2)

    print("Processing complete. Results saved to", file_path)


# Call the function
if __name__ == "__main__":
    token = get_access_token("asp","9649964096", "123456", "prod1")  # Ensure you have a function to get the access token
    if token:
        print("token retrieved successfully")
        delete_farmers(file_path, token)
    else:
        print("Failed to retrieve access token.")
