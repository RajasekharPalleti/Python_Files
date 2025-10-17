# Author: Rajasekhar Palleti

import requests
import json

baseurl = "https://to2nq285i7.execute-api.us-west-2.amazonaws.com/prod/fetch_data/cropin"
dataset = "vw_rpt_application_blocks_v" # Replace it with the below dataset name
# "vw_mobile_receipts"
# "vw_rpt_application_blocks_v"
# "vw_rpt_transactional_ranch_numbers_v"
# "vw_sfc_exportmasterschedulestableau"
# "vw_sfc_exportvolplanforecastsubmission"

load_type = "incremental" # Replace it with the actual load type "initial" or "incremental"
headers = {
    'x-api-key': 'vMFOJ85zNW4yZFSWBAKU38JTcncxSAb057mIckMR'
}

try:
    get_response = requests.get(f"{baseurl}/{load_type}/{dataset}", headers=headers)
    print("Status Code:", get_response.status_code)
    print("Response Body", get_response.text)
    if get_response.headers.get("Content-Type", "").startswith("application/json"):
        data = get_response.json()
        print("Response Body :")
        print(json.dumps(data, indent=2)) # For pretty print JSON response
    else:
        print("Response Body (Text):")
        print(get_response.text)
    get_response.raise_for_status()
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
    print()