import requests
import pandas as pd
import time

# Define file path
FILE_PATH = "C:/Users/ruchitha.p/Desktop/SmartFarmAPI_Automation/Excel_Files/Area_Audit_Removal.xlsx"

# Load the Excel file into a DataFrame
df = pd.read_excel(FILE_PATH)

# Ensure necessary columns exist
for col in ["status", "Response"]:
    if col not in df.columns:
        df[col] = ""  # Add missing columns
    df[col] = df[col].astype(str)  # Explicitly convert to string to avoid dtype issues

# Authorization Token
TOKEN = "your_actual_token_here"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# Check if the 'ca_id' column exists
# if "ca_id" not in df.columns:
#     print("Error: 'ca_id' column is missing in the Excel file.")
#     exit()

# Iterate over each row and process CA IDs
success_count, failure_count = 0, 0

# Define range for iteration
start_index = 1  # Should be at least 1 which starts from 2nd row as headers in first row
end_index = 50  # Suppose last row is on 100, we need to pass 99 here

for index, row in df.iloc[start_index:end_index].iterrows():
    ca_id = str(row["ca_id"]).strip()

    if not ca_id:  # Skip empty CA IDs
        continue

    url = f"https://cloud.cropin.in/services/farm/api/croppable-areas/{ca_id}/area-audit"
    print(f"Processing {ca_id}: {url}")

    try:
        response = requests.delete(url, headers=HEADERS)
        status_code = response.status_code

        if status_code == 200:
            df.at[index, "status"] = "Success"
            success_count += 1
        else:
            df.at[index, "status"] = f"Failed: {status_code}"
            df.at[index, "Response"] = response.text
            failure_count += 1

        print(f"{ca_id}: Status {status_code}, {'Success' if status_code == 200 else 'Failed'}")

    except requests.RequestException as e:
        print(f"Error processing {ca_id}: {e}")
        df.at[index, "status"] = "Error"
        df.at[index, "Response"] = str(e)
        failure_count += 1

    # Wait for 1 second to avoid rate limits
    time.sleep(1)

# Save the updated DataFrame back to Excel
df.to_excel(FILE_PATH, index=False)
print(f"Process Completed: {success_count} Success, {failure_count} Failures")
