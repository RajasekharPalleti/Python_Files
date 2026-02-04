# Author: Rajasekhar Palleti

import json
import requests
import pandas as pd
import time

from GetAuthtoken import get_access_token


def post_data_to_api(api_url, token, input_excel, output_excel, sheet_name):
    #️⃣ 1. Set headers for API call
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    #️⃣ 2. Load Excel data
    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    #️⃣ 3. Ensure required columns exist
    if "Status" not in df.columns:
        df["Status"] = ""
    if "CA_Response" not in df.columns:
        df["CA_Response"] = ""

    #️⃣ 4. Extract CA IDs (first column) and drop empty ones
    ca_ids = df.iloc[:, 0].dropna().tolist()

    #️⃣ 5. Process CA IDs in chunks of 100
    chunk_size = 100
    for i in range(0, len(ca_ids), chunk_size):
        chunk_ids = ca_ids[i:i + chunk_size]

        #️⃣ 6. Construct payload for bulk update
        payload = {
            "sowingDate": "2025-06-18T00:00:00.000Z",
            "croppableArea": None,
            "varietyId": 3422152,
            "ids": chunk_ids
        }

        try:
            #️⃣ 7. Send PUT request to bulk API
            response = requests.put(api_url, headers=headers, data=json.dumps(payload))

            #️⃣ 8. Update status and response for each CA ID in the chunk
            for ca_id in chunk_ids:
                idx = df[df.iloc[:, 0] == ca_id].index
                if response.status_code == 200:
                    df.loc[idx, "Status"] = "Success"
                else:
                    df.loc[idx, "Status"] = f"PUT Failed: {response.status_code}"
                df.loc[idx, "CA_Response"] = response.text

            print(f"✅ Processed CA_IDs {i + 1} to {i + len(chunk_ids)} — Status: {response.status_code}")

        except requests.exceptions.RequestException as e:
            #️⃣ 9. Log exception for each ID in case of failure
            for ca_id in chunk_ids:
                idx = df[df.iloc[:, 0] == ca_id].index
                df.loc[idx, "Status"] = f"Failed: {str(e)}"
                df.loc[idx, "CA_Response"] = str(e)
            print(f"❌ Exception occurred for CA_IDs {i + 1} to {i + len(chunk_ids)}: {str(e)}")

        #️⃣ 10. Optional delay to avoid throttling
        time.sleep(30)

    #️⃣ 11. Save the updated Excel with status and responses
    df.to_excel(output_excel, sheet_name=sheet_name, index=False)
    print(f"\n📁 Excel file updated and saved as: {output_excel}")


if __name__ == "__main__":
    #️⃣ 12. File and API configuration
    input_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\GEO_TAG_WALMART.xlsx"
    output_excel = "C:\\Users\\rajasekhar.palleti\\Downloads\\GEO_TAG_WALMART_report.xlsx"
    sheet_name = "result"
    api_url = "https://cloud.cropin.in/services/farm/api/croppable-areas/bulk"

    #️⃣ 13. Get access token before making API calls
    print("🔐 Retrieving access token...")
    token = get_access_token("walmart", "12345012345", "Cropin@123", "prod1")

    if token:
        print("✅ Access token retrieved successfully.\n")
        post_data_to_api(api_url, token, input_excel, output_excel, sheet_name)
    else:
        print("❌ Failed to retrieve access token. Process terminated.")
