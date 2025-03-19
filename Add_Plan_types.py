# Author: Rajasekhar Palleti

import pandas as pd
import requests
import time
import json
from GetAuthtoken import get_access_token

# Get API access token
# token = get_access_token("solidaridad", "anil.kolla@cropin.com", "Cropin@123", "prod1")
token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJqZ1JBYTkxSVg5RUxveExYdlp0S0RLTVlJbmhlY20zdHZCNW1GMm5YZGhFIn0.eyJleHAiOjE3NDIwMjUwNDYsImlhdCI6MTc0MTkzODY0NiwiYXV0aF90aW1lIjoxNzQxOTM4NjI0LCJqdGkiOiJlMDM1ZmU1Mi1jOThiLTQ0ZDQtODkwZi01ZDRhM2YyOGU5MWUiLCJpc3MiOiJodHRwczovL3Yyc3NvLWdjcC5jcm9waW4uY28uaW4vYXV0aC9yZWFsbXMvcWF6b25lNiIsImF1ZCI6WyJyZXNvdXJjZV9zZXJ2ZXIiLCJhY2NvdW50Il0sInN1YiI6ImRmOThiNTI5LTg1Y2YtNGU0Ny1hNmQyLWJjYjc2ZDVlMDdjMSIsInR5cCI6IkJlYXJlciIsImF6cCI6IndlYl9hcHAiLCJub25jZSI6Il9jbGllbnRfd2ViX2FwcCIsInNlc3Npb25fc3RhdGUiOiJjNmYwMjAyNy03ODhjLTQxNmQtYTdhZi0xOWQyY2YyMDM1NDciLCJhY3IiOiIwIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJyZXNvdXJjZV9zZXJ2ZXIiOnsicm9sZXMiOlsiUk9MRV9BRE1JTl8xMzUxIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIHBob25lIG1pY3JvcHJvZmlsZS1qd3Qgb2ZmbGluZV9hY2Nlc3MgYWRkcmVzcyIsInNpZCI6ImM2ZjAyMDI3LTc4OGMtNDE2ZC1hN2FmLTE5ZDJjZjIwMzU0NyIsInVzZXJfcm9sZSI6WyJST0xFX0FETUlOXzEzNTEiXSwidXBuIjoiNzM4MjIxMTIzMSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiYWRkcmVzcyI6e30sImdyb3VwcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl0sInByZWZlcnJlZF91c2VybmFtZSI6IjczODIyMTEyMzEiLCJlbWFpbCI6InJhamFAZ21haWwuY29tIiwidGVuYW50IjoicWF6b25lNiJ9.FVfxp4uuUpws7_W1Kg_rkUwcXurvj3x5XFioUaF8Tq8Q3_9z4yriD-bj7yvs61tnKWSBVwJQyK45aoe1-1_aTEvS--ItlFEqTXrrPRhaYL6COwcqoBHawadhstcfnZ1kvBf7hU-AO74HrbbMkjw14jKES8SCwiDeEiYOeafhY7ufp1MiVvH6b9Yw_7Fy7HP4FIItuSi9uvzmDB0urs0vZU6Lt1C1XLy7fx_7m52N_vHcal1uYgUxD_x8FgXmPveBkDXRZgJ0lmYCafmhk97BZ9OMRc8MY5T6pDk4aUTsG6QkK7M-YGJTL17ijJ73pnKxBKrYLWg87Jk_F2vRgBQlwg"

if not token:
    print("❌ Failed to retrieve access token. Process terminated.")
    exit()
else:
    print("✅ Access token retrieved successfully.")

# File path and sheet name
file_path = r"C:\Users\rajasekhar.palleti\Downloads\addplantypes.xlsx"
sheet_name = "Sheet1"  # Update this with the actual sheet name

# Load Excel file with the specific sheet
df = pd.read_excel(file_path, sheet_name=sheet_name)

# Ensure necessary columns exist, else add them
required_columns = ["status", "Response"]
for col in required_columns:
    if col not in df.columns:
        df[col] = ""

df = df.astype(str)  # Convert to string to avoid dtype issues

# API URL
api_url = "https://ca-v2-gcp.cropin.co.in/qa6/services/farm/api/plan-types"

# API Headers with Authorization Token
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Iterate over rows
for index, row in df.iterrows():
    try:
        plantype_name = str(row.iloc[0]).strip()
        cust_attribute_name1 = str(row.iloc[1]).strip()
        cust_attribute_name2 = str(row.iloc[2]).strip()
        cust_attribute_label1 = str(row.iloc[3]).strip()
        cust_attribute_label2 = str(row.iloc[4]).strip()

        # Skip empty plantype_name
        if not plantype_name:
            print(f"⚠️ Skipping row {index + 1}: Plantype Name is missing.")
            df.at[index, "status"] = "⚠️ Skipped - Missing Name"
            continue

        print(f"🔄 Processing row {index + 1}: PlanType = {plantype_name}")

        # Construct payload
        payload = {
	        "data": {
		"standardAttributes": [
			"PLAN_NAME",
			"DESCRIPTION",
			"FARM_RESOURCE",
			"GOOD_PRACTICES",
			"ATTACH_IMAGE",
			"UPLOAD_PDF",
			"VIDEO_URL",
			"CAPTURE_GEO_LOCATION",
			"CAPTURE_SIGNATURE",
			"CADENCE_TYPE",
			"DATE_OF_ACTION",
			"REFERENCE_DATE",
			"ACTION_TO_BE_COMPLETED_IN_ DAYS"
		],
		"customAttributes": [
			{
				"name": cust_attribute_name1,
				"type": "Text",
				"label": cust_attribute_label1,
				"options": [],
				"condition": False,
				"hasCalculation": False,
				"validation": {
					"fixed": False,
					"required": False,
					"multiple": False
				},
				"hidden": False
			},
			{
				"name": cust_attribute_name2,
				"type": "Drop down",
				"label": cust_attribute_label2,
				"options": [
					"1",
					"2",
					"3",
					"4",
					"5",
					"6"
				],
				"condition": False,
				"hasCalculation": False,
				"validation": {
					"fixed": False,
					"required": False,
					"multiple": False
				},
				"hidden": False
			}
		],
		"columnHeaderAttributes": [
			{
				"type": "text",
				"style": {},
				"addUrl": "",
				"apiUrl": "",
				"controls": "",
				"validation": {
					"fixed": True,
					"mandatory": True,
					"selected": True
				},
				"hidden": False,
				"display_value": "tripName",
				"display_name": "TRIP_NAME"
			},
			{
				"type": "select",
				"style": {},
				"apiUrl": "master/api/batch-details/fetchCutting",
				"controls": "",
				"validation": {
					"fixed": True,
					"mandatory": True,
					"selected": True
				},
				"valueToSave": "parentPlan",
				"masterDataKey": "cutting_plan",
				"valueDependent": "params",
				"valueToDisplay": "name",
				"hidden": False,
				"display_value": "cuttingPlanID",
				"display_name": "CUTTING_PLAN"
			},
			{
				"type": "text",
				"style": {},
				"addUrl": "",
				"apiUrl": "",
				"controls": "",
				"validation": {
					"fixed": False,
					"mandatory": True,
					"selected": True
				},
				"hidden": False,
				"display_value": "vehicleNumber",
				"display_name": "VEHICLE_NUMBER"
			},
			{
				"type": "select",
				"style": {},
				"apiUrl": "master/api/sku-types",
				"controls": "",
				"validation": {
					"fixed": False,
					"mandatory": True,
					"selected": True
				},
				"valueToSave": "id",
				"masterDataKey": "sku_type",
				"valueToDisplay": "name",
				"hidden": False,
				"display_value": "skuType",
				"display_name": "SKU_TYPE"
			},
			{
				"type": "number",
				"style": {},
				"apiUrl": "",
				"controls": "",
				"validation": {
					"fixed": False,
					"mandatory": True,
					"selected": True
				},
				"hidden": False,
				"display_value": "noOfSKU",
				"display_name": "NUMBER_OF_SKUS"
			},
			{
				"type": "units",
				"style": {},
				"apiUrl": "master/api/constants?name=quantityunit",
				"controls": "",
				"isEditable": True,
				"validation": {
					"fixed": False,
					"mandatory": True,
					"selected": True
				},
				"hasCalculation": True,
				"constantDataKey": "quantity_units",
				"calculationFunction": "function calculateFormula(capacity,unit,skuCount){let obj= new Object({});if(skuCount){obj.count=skuCount*capacity;obj.count = parseFloat(parseFloat(obj.count).toFixed(2))}if(unit){obj.unit=unit} return obj;}",
				"calculationAttributes": [
					"skut.livedbfield-skuType.capacity",
					"skut.livedbfield-skuType.unitId.$unit.unitName",
					"pt.this.data.planHeaderAttributesEntered.noOfSKU"
				],
				"hidden": False,
				"display_value": "totalQuantity",
				"display_name": "TOTAL_QUANTITY"
			}
		],
		"columnHeaderGroup": {
			"groupName": "Trip Name",
			"groupList": [
				{
					"type": "text",
					"style": {},
					"addUrl": "",
					"apiUrl": "",
					"controls": "",
					"validation": {
						"fixed": True,
						"mandatory": True,
						"selected": True
					},
					"hidden": False,
					"display_value": "tripName",
					"display_name": "TRIP_NAME"
				},
				{
					"type": "select",
					"style": {},
					"addUrl": "",
					"apiUrl": "master/api/harvest-grades/varieties",
					"controls": "",
					"validation": {
						"fixed": False,
						"mandatory": False,
						"selected": False
					},
					"valueToSave": "id",
					"masterDataKey": "harvest_grade",
					"valueDependent": "params",
					"valueToDisplay": "name",
					"hidden": True,
					"display_value": "harvestGradeName",
					"display_name": "HARVEST_GRADE_NAME"
				},
				{
					"type": "select",
					"style": {},
					"apiUrl": "master/api/batch-details/fetchCutting",
					"controls": "",
					"validation": {
						"fixed": True,
						"mandatory": True,
						"selected": True
					},
					"valueToSave": "parentPlan",
					"masterDataKey": "cutting_plan",
					"valueDependent": "params",
					"valueToDisplay": "name",
					"hidden": False,
					"display_value": "cuttingPlanID",
					"display_name": "CUTTING_PLAN"
				},
				{
					"type": "text",
					"style": {},
					"addUrl": "",
					"apiUrl": "",
					"controls": "",
					"validation": {
						"fixed": False,
						"mandatory": True,
						"selected": True
					},
					"hidden": False,
					"display_value": "vehicleNumber",
					"display_name": "VEHICLE_NUMBER"
				},
				{
					"type": "select",
					"style": {},
					"apiUrl": "master/api/sku-types",
					"controls": "",
					"validation": {
						"fixed": False,
						"mandatory": True,
						"selected": True
					},
					"valueToSave": "id",
					"masterDataKey": "sku_type",
					"valueToDisplay": "name",
					"hidden": False,
					"display_value": "skuType",
					"display_name": "SKU_TYPE"
				},
				{
					"type": "number",
					"style": {},
					"apiUrl": "",
					"controls": "",
					"validation": {
						"fixed": False,
						"mandatory": True,
						"selected": True
					},
					"hidden": False,
					"display_value": "noOfSKU",
					"display_name": "NUMBER_OF_SKUS"
				},
				{
					"type": "select",
					"style": {},
					"addNew": True,
					"apiUrl": "master/api/batch-details",
					"controls": "",
					"validation": {
						"fixed": False,
						"mandatory": False,
						"selected": False
					},
					"valueToSave": "uibatchNumber",
					"masterDataKey": "batch_details",
					"valueToDisplay": "uibatchNumber",
					"hidden": True,
					"display_value": "batchNumber",
					"display_name": "LOT/BATCH_NUMBER"
				},
				{
					"type": "units",
					"style": {},
					"apiUrl": "master/api/constants?name=quantityunit",
					"controls": "",
					"isEditable": True,
					"validation": {
						"fixed": False,
						"mandatory": True,
						"selected": True
					},
					"hasCalculation": True,
					"constantDataKey": "quantity_units",
					"calculationFunction": "function calculateFormula(capacity,unit,skuCount){let obj= new Object({});if(skuCount){obj.count=skuCount*capacity;obj.count = parseFloat(parseFloat(obj.count).toFixed(2))}if(unit){obj.unit=unit} return obj;}",
					"calculationAttributes": [
						"skut.livedbfield-skuType.capacity",
						"skut.livedbfield-skuType.unitId.$unit.unitName",
						"pt.this.data.planHeaderAttributesEntered.noOfSKU"
					],
					"hidden": False,
					"display_value": "totalQuantity",
					"display_name": "TOTAL_QUANTITY"
				},
				{
					"type": "date",
					"style": {},
					"controls": "",
					"validation": {
						"fixed": False,
						"mandatory": False,
						"selected": False
					},
					"hidden": True,
					"display_value": "deliveryDate",
					"display_name": "DELIVERY_DATE"
				},
				{
					"type": "date",
					"style": {},
					"apiUrl": "",
					"controls": "",
					"validation": {
						"fixed": False,
						"mandatory": False,
						"selected": False
					},
					"hidden": True,
					"display_value": "date",
					"display_name": "DATE/DAYS_FROM_SOWING_DATE"
				}
			]
		}
	},
	"id": None,
	"dataCollection": False,
	"zoneSampleCollection": False,
	"name": plantype_name
}

        # Send API request
        print(f"📡 Sending request for: {plantype_name}")
        try:
            response = requests.post(api_url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            response_json = response.json()
            df.at[index, "Response"] = json.dumps(response_json)

            if response.status_code == 201:
                print(f"✅ PlanType '{plantype_name}' created successfully.")
                df.at[index, "status"] = "✅ Success"
            else:
                print(f"❌ PlanType creation failed for '{plantype_name}'. Status: {response.status_code}")
                df.at[index, "status"] = f"❌ Failed: {response.status_code}"

        except requests.exceptions.Timeout:
            print(f"⏳ Timeout error for {plantype_name}. Retrying...")
            df.at[index, "status"] = "⏳ Timeout - Retry needed"
        except requests.exceptions.RequestException as req_err:
            error_message = str(req_err)
            df.at[index, "status"] = f"❌ Request Failed: {error_message}"
            print(f"❌ Request error for {plantype_name}: {error_message}")

        time.sleep(0.5)  # Pause before the next iteration

    except Exception as e:
        print(f"⚠️ Unexpected error at row {index + 1}: {str(e)}")
        df.at[index, "status"] = f"⚠️ Error: {str(e)}"

# Save updated Excel file
print("Data 📁 Processing to Excel")
with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists= "replace") as writer:
    df.to_excel(writer, sheet_name=sheet_name, index=False)
print("📁 Processing complete. Results saved")
