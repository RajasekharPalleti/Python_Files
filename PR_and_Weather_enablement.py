#Author: Rajasekhar Palleti

import pandas as pd
import requests
import time
import json
from GetAuthtoken import get_access_token  # Ensure this module is available

# Get access token
token = get_access_token("solidaridad", "anil.kolla@cropin.com", "Cropin@123", "prod1")

if token:
    print("✅ Access token retrieved successfully.")
else:
    print("❌ Failed to retrieve access token. Process terminated.")
    exit()

# File path and sheet name
file_path = r"C:\Users\rajasekhar.palleti\Downloads\PR_for_Solidaridad_Details_2025_03_05 Pr to be enabled.xlsx"
sheet_name = "result"  # Change this to your actual sheet name

# Load Excel file with the specific sheet
df = pd.read_excel(file_path, sheet_name=sheet_name)

# Ensure necessary columns exist if not add
columns_to_check = ["status","Failed in Response", "srPlotid", "Plot_risk_response", "Weather_response"]
for col in columns_to_check:
    if col not in df.columns:
        df[col] = ""
    df[col] = df[col].astype(str)  # Convert to string to prevent dtype issues

# API endpoints
plot_risk_url = "https://cloud.cropin.in/services/farm/api/croppable-areas/plot-risk/batch"
# sustainability_url = "https://cloud.cropin.in/services/farm/api/croppable-areas/sustainability/batch?features=WEATHER"

# API Headers with Authorization Token
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}


# Function to extract srPlotId from known response structures
def extract_sr_plot_id(response_json):
    if "srPlotDetails" in response_json:
        for details in response_json["srPlotDetails"].values():
            return details.get("srPlotId")

    for key, value in response_json.items():
        if isinstance(value, list):
            for item in value:
                if "srPlotId" in item:
                    return item["srPlotId"]
    return "N/A"


# Iterate over rows
for index, row in df.iloc[:].iterrows():
    try:
        croppable_area_id = str(row.iloc[0]).strip()
        # farmer_id = str(row.iloc[1]).strip()

        print(f"🔄 Processing row {index + 1}: CroppableAreaId = {croppable_area_id}")

        # Construct payloads
        plot_risk_payload = [{"croppableAreaId": croppable_area_id, "farmerId": None}]
        # sustainability_payload = [croppable_area_id]

        # Send Plot Risk API request
        print(f"📡 Sending Plot Risk API request for CroppableAreaId: {croppable_area_id}")
        try:
            plot_risk_response = requests.post(plot_risk_url, json=plot_risk_payload, headers=headers)
            plot_risk_response.raise_for_status()  # Raise error for HTTP failures
            plot_risk_json = plot_risk_response.json()
            df.at[index, "Plot_risk_response"] = json.dumps(plot_risk_json)
            df.at[index, "srPlotid"] = extract_sr_plot_id(plot_risk_json)
            print(f"✅ Extracted srPlotId: {df.at[index, 'srPlotid']}")
        except requests.exceptions.RequestException as req_err:
            error_message = str(req_err)
            df.at[index, "Plot_risk_response"] = error_message
            df.at[index, "srPlotid"] = "N/A"
            print(f"❌ Plot Risk API request failed: {error_message}")

        time.sleep(0.5)

        # # Send Weather API request
        # print(f"📡 Sending Weather API request for CroppableAreaId: {croppable_area_id}")
        # try:
        #     sustainability_response = requests.post(sustainability_url, json=sustainability_payload, headers=headers)
        #     sustainability_response.raise_for_status()
        #     sustainability_json = sustainability_response.json()
        #     df.at[index, "Weather_response"] = json.dumps(sustainability_json)
        # except requests.exceptions.RequestException as req_err:
        #     error_message = str(req_err)
        #     df.at[index, "Weather_response"] = error_message
        #     print(f"❌ Weather API request failed: {error_message}")

        # Update the status column
        if (plot_risk_response.status_code == 200):
                # (plot_risk_response.status_code == 200): of only plot risk
                # (sustainability_response.status_code == 200): of only Sustainability Weather
            df.at[index, "status"] = "✅ Success"
        else:
            df.at[index, "status"] = "❌ Failed"

        # Check if Failed in response of plotrisk API
        sr_plot_details = plot_risk_json.get("srPlotDetails", {})

        failed_found = False  # Flag to track any failed status

        for key, value in sr_plot_details.items():
            if value.get("status") == "FAILED":
                failed_found = True
                print(f"❌ Status for {key}: {value['status']} - {value.get('message', 'No message provided')}")
                df.at[
                    index, "Failed in Response"] = f"❌ Failed: {value.get('message', 'No details')}"  # Store failure message in status

        # Update status column if no failure was found
        if not failed_found:
            df.at[index, "Failed in Response"] = "✅ Success"


    except Exception as e:
        error_message = str(e)
        df.at[index, "status"] = f"⚠️ Error: {error_message}"
        print(f"⚠️ Error in row {index + 1}: {error_message}")

    time.sleep(4.5)

# Save the updated Excel file
print("🎯 Processing completed. Data updating to the Excel file.")
with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df.to_excel(writer, sheet_name=sheet_name, index=False)

print("🎯 Process complete. Data updated in the Excel file.")
