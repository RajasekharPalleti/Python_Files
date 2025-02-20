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
        df[col] = ""

# Authorization Token
TOKEN = "your_actual_token_here"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# Check if 'ca_id' column exists
if "ca_id" not in df.columns:
    print("Error: 'ca_id' column is missing in the Excel file.")
    exit()

# Define range for iteration
start_index = 10  # Adjust as needed
end_index = 50    # Adjust as needed

# Counters for tracking success and failure
success_count, failure_count = 0, 0

# Iterate over the specified range
for index, row in df.iloc[start_index:end_index].iterrows():
    croppable_area_id = str(row.iloc[0]).strip()  # Extract CA ID from column 0

    if not croppable_area_id:  # Skip empty CA IDs
        continue

    url = f"https://cloud.cropin.in/services/farm/api/croppable-areas/{croppable_area_id}/area-audit"
    print(f"Processing {croppable_area_id}: {url}")

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

        print(f"{croppable_area_id}: Status {status_code}, {'Success' if status_code == 200 else 'Failed'}")

    except requests.RequestException as e:
        print(f"Error processing {croppable_area_id}: {e}")
        df.at[index, "status"] = "Error"
        df.at[index, "Response"] = str(e)
        failure_count += 1

    # Wait for 1 second to avoid rate limits
    time.sleep(1)

# Save the updated DataFrame back to Excel
df.to_excel(FILE_PATH, index=False)
print(f"Process Completed: {success_count} Success, {failure_count} Failures")
