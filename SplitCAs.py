import time

import pandas as pd
import requests
from GetAuthtoken import get_access_token  # your existing file

# === Config ===
input_excel = r"C:\Users\rajasekhar.palleti\Downloads\Driscoll_Bulk_output3.xlsx"   # your Excel file
sheet_name = "Sheet1"

# === Read Excel ===
df = pd.read_excel(input_excel, sheet_name=sheet_name)

# === Get token once ===
token = get_access_token("auxoaidriscolls", "9148981108", "cropin@123", "prod1")
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# === Process each row with iloc ===
total_area = 11

for i in range(len(df)):
    try:
        croppable_area_id = int(df.iloc[i, 0])   # Column 1
        project_id = int(df.iloc[i, 1])          # Column 2
        split_count = int(df.iloc[i, 4])         # Column 6

        if split_count < 1 or split_count > 50:
            print(f"Skipping row {i+2} → split_count is 0 or invalid")
            continue

        # Build URL
        base_url = f"https://cloud.cropin.in/services/farm/api/projects/{project_id}/croppable-areas/{croppable_area_id}/split"

        # Calculate split area & percentage
        split_area = round(total_area / split_count, 2)
        split_percentage = round(100 / split_count, 2)

        # Build payload
        payload = [
            {
                "entities": [],
                "splitArea": split_area,
                "data": None,
                "areaAuditDto": None,
                "splitPercentage": split_percentage,
                "name": None
            }
            for _ in range(split_count)
        ]

        print(f"\n➡ Processing Row {i+2} | Project {project_id}, CA {croppable_area_id}, Splits {split_count}")
        print("Payload:", payload)

        # API call
        response = requests.post(base_url, json=payload, headers=headers)

        print("Response Status:", response.status_code)
        print("Response Data:", response.text)
        time.sleep(10)  # Sleep to avoid rate limiting

        if response.status_code == 200:
            print(f"✅ Successfully split CA {croppable_area_id} in Project {project_id}")
        else:
            print(f"❌ Failed to split CA {croppable_area_id} in Project {project_id}, Status: {response.status_code}")
    except Exception as e:
        print(f"❌ Error processing row {i+2}: {e}")
