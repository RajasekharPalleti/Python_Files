#Author: Rajasekhar Palleti

import pandas as pd
import requests
import json
import time
from GetAuthtoken import get_access_token

def delete_project_assets_from_excel(api_url, token, input_excel, sheet_name, output_excel):
    """
    Reads Excel data, performs DELETE calls for each record, and writes results back.
    """
    print("📂 Loading input Excel file...")
    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    # Ensure required columns exist
    required_columns = ["project_id", "project_asset_ids", "croppableAreaIds"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"❌ Missing required column: {col}")

    # Add output columns if not present
    if "status" not in df.columns:
        df["status"] = ""
    if "response" not in df.columns:
        df["response"] = ""

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    for index, row in df.iterrows():
        project_id = row["project_id"]
        ids = str(row["project_asset_ids"]).split(",") if pd.notna(row["project_asset_ids"]) else []
        croppable_area_ids = str(row["croppableAreaIds"]).split(",") if pd.notna(row["croppableAreaIds"]) else []

        ids_param = ",".join(ids)
        ca_ids_param = ",".join(croppable_area_ids)

        delete_url = f"{api_url}/projects/{project_id}/project-assets/selected-ids"
        params = {"ids": ids_param, "croppableAreaIds": ca_ids_param}

        print(f"\n🧾 Row {index + 1} → Deleting assets for Project ID: {project_id}")
        print(f"➡️ Params: {params}")

        try:
            response = requests.delete(delete_url, headers=headers, params=params)
            df.at[index, "status"] = response.status_code

            try:
                response_data = response.json()
                df.at[index, "response"] = json.dumps(response_data)
            except json.JSONDecodeError:
                df.at[index, "response"] = response.text

            if response.status_code == 200:
                print(f"✅ Successfully deleted assets for project {project_id}")
            else:
                print(f"⚠️ Failed for project {project_id} → Status {response.status_code}")

        except Exception as e:
            print(f"❌ Exception while deleting project {project_id}: {e}")
            df.at[index, "status"] = "Error"
            df.at[index, "response"] = str(e)

        time.sleep(1)  # Add small delay between requests

    # Save final output Excel
    print(f"\n💾 Saving output to: {output_excel}")
    df.to_excel(output_excel, index=False)
    print("✅ Process completed successfully.")


# ------------------- MAIN EXECUTION -------------------

if __name__ == "__main__":
    BASE_URL = "https://cloud.cropin.in/services/farm/api"
    input_excel = r"C:\Users\rajasekhar.palleti\Downloads\Delete_Project_Assets.xlsx"
    sheet_name = "Sheet1"
    output_excel = r"C:\Users\rajasekhar.palleti\Downloads\Delete_Project_Assets_Output.xlsx"

    token = get_access_token("asp", "9148981108", "cropin@123", "prod1")
    if token:
        print("✅ Access token retrieved successfully.")
        delete_project_assets_from_excel(BASE_URL, token, input_excel, sheet_name, output_excel)

    else:
        print("❌ Failed to retrieve access token. Process terminated.")
