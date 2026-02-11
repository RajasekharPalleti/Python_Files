# Author  : Rajasekhar Palleti
# Purpose : Create Farmer + Asset + CA using Composite Dashboard API

import requests
import random
import time
import threading
from datetime import datetime, timedelta
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor

import geopandas as gpd
from shapely.geometry import Point

# =========================================================
# CONFIGURATION
# =========================================================

API_URL = "https://ca-v2-gcp.cropin.co.in/qa6/services/farm/api/composite/dashboard"

PROJECT_IDS = [761051, 761052, 761053, 761054]
VARIETY_IDS = [183951, 4854, 708851]

TOTAL_RECORDS =10000
DELAY_SECONDS = 3
MAX_WORKERS = 4  # Number of concurrent threads (adjust based on system capabilities and API rate limits)

# =========================================================
# THREAD-SAFE GLOBALS
# =========================================================

PROJECT_CYCLE = cycle(PROJECT_IDS)
VARIETY_CYCLE = cycle(VARIETY_IDS)

counter_lock = threading.Lock()
cycle_lock = threading.Lock()
current_index = 7501

# =========================================================
# INDIA POLYGON (CACHED, THREAD-SAFE)
# =========================================================

_INDIA_POLYGON = None
polygon_lock = threading.Lock()

def load_india_polygon():
    global _INDIA_POLYGON
    if _INDIA_POLYGON:
        return _INDIA_POLYGON

    with polygon_lock:
        if _INDIA_POLYGON is None:
            world = gpd.read_file(
                "https://naturalearth.s3.amazonaws.com/50m_cultural/ne_50m_admin_0_countries.zip"
            )
            india = world[world["ADMIN"] == "India"].to_crs("EPSG:4326")
            _INDIA_POLYGON = india.geometry.union_all()

    return _INDIA_POLYGON


# =========================================================
# RANDOM INDIA LAND COORDINATES
# =========================================================

def get_random_india_lat_lng():
    india_polygon = load_india_polygon()
    minx, miny, maxx, maxy = india_polygon.bounds

    while True:
        lat = random.uniform(miny, maxy)
        lng = random.uniform(minx, maxx)
        if india_polygon.contains(Point(lng, lat)):
            return round(lat, 7), round(lng, 7)


# =========================================================
# SIMPLE INDIA ADDRESS (NO GOOGLE)
# =========================================================

def build_simple_india_address(lat, lng):
    return {
        "country": "India",
        "formattedAddress": "India",
        "administrativeAreaLevel1": None,
        "locality": None,
        "postalCode": None,
        "placeId": None,
        "latitude": lat,
        "longitude": lng
    }


# =========================================================
# RANDOM SOWING DATE
# =========================================================

def random_sowing_date():
    start_date = datetime(2025, 12, 1)
    end_date = datetime.now()
    delta_days = max((end_date - start_date).days, 0)

    return (
        start_date + timedelta(days=random.randint(0, delta_days))
    ).strftime("%Y-%m-%dT00:00:00.000+0000")


# =========================================================
# BUILD PAYLOAD
# =========================================================

def build_payload(index, project_id, variety_id, sowing_date, location):
    return {
        "projectId": project_id,
        "varietyId": variety_id,
        "sowingDate": sowing_date,
        "userIds": None,
        "data": {"tags": [760054]},
        "farmer": {
            "status": "DISABLE",
            "firstName": f"Bulk upload farmer 09 02 {index}",
            "farmerCode": f"Bulk upload farmer 09 02_{index}",
            "email": f"Bulkuploadfarmer0902{index}@mail.com",
            "assignedTo": [743203,641452],
            "data": {
                "mobileNumber": f"9000000{index:03}",
                "countryCode": "+91",
                "countryIsoCode": "IN",
                "doB": "1994-10-06T00:00:00.000Z",
                "tags": [729768]
            },
            "images": {},
            "declaredArea": {
                "enableConversion": "true",
                "unit": "ACRE",
                "count": 10
            },
            "address": location
        },
        "asset": {
            "name": f"Auto bulk upload Asset {index}",
            "companyStatus": "ACTIVE",
            "soilType": {"id": 1059},
            "irrigationType": {"id": 1101},
            "data": {"tags": [729757]},
            "images": {},
            "declaredArea": {
                "enableConversion": "true",
                "unit": "ACRE",
                "count": 10
            },
            "auditedArea": {
                "enableConversion": "true",
                "unit": "ACRE"
            },
            "address": location
        }
    }


# =========================================================
# WORKER FUNCTION
# =========================================================

def worker(thread_name, token):
    global current_index

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    while True:
        with counter_lock:
            if current_index > TOTAL_RECORDS:
                return
            index = current_index
            current_index += 1

        with cycle_lock:
            project_id = next(PROJECT_CYCLE)
            variety_id = next(VARIETY_CYCLE)

        try:
            sowing_date = random_sowing_date()
            lat, lng = get_random_india_lat_lng()
            location = build_simple_india_address(lat, lng)

            payload = build_payload(
                index, project_id, variety_id, sowing_date, location
            )

            print(f" {thread_name} | Record {index}")

            resp = requests.post(API_URL, headers=headers, json=payload, timeout=30)

            if resp.status_code in (200, 201):
                data = resp.json()
                print(
                    f"✅ {thread_name} | {index} | "
                    f"F:{data['farmer']['id']} "
                    f"A:{data['asset']['id']} "
                    f"CA:{data['croppableArea']['id']}"
                )
            else:
                print(f"❌ {thread_name} | {index} | {resp.status_code} | {resp.text}")

        except Exception as e:
            print(f"❌ {thread_name} | {index} | Exception: {e}")

        time.sleep(DELAY_SECONDS)


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    print("🔐 Using existing access token...")

    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJqZ1JBYTkxSVg5RUxveExYdlp0S0RLTVlJbmhlY20zdHZCNW1GMm5YZGhFIn0.eyJleHAiOjE3NzA2OTg5OTcsImlhdCI6MTc3MDYxMjU5NywiYXV0aF90aW1lIjoxNzcwNjEyNTk3LCJqdGkiOiJmNGZjNTIyZC1lZTdkLTQwYTEtOTdjMS1hNWMzNDFmOTk5MjgiLCJpc3MiOiJodHRwczovL3Yyc3NvLWdjcC5jcm9waW4uY28uaW4vYXV0aC9yZWFsbXMvcWF6b25lNiIsImF1ZCI6WyJyZXNvdXJjZV9zZXJ2ZXIiLCJhY2NvdW50Il0sInN1YiI6ImRmOThiNTI5LTg1Y2YtNGU0Ny1hNmQyLWJjYjc2ZDVlMDdjMSIsInR5cCI6IkJlYXJlciIsImF6cCI6IndlYl9hcHAiLCJub25jZSI6Il9jbGllbnRfd2ViX2FwcCIsInNlc3Npb25fc3RhdGUiOiJjZDU1YmI1NC0yMWIwLTRkNGEtYTA0Mi1mMjNhZDE5YjRlNWQiLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJyZXNvdXJjZV9zZXJ2ZXIiOnsicm9sZXMiOlsiUk9MRV9BRE1JTl8xMzUxIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIHBob25lIG1pY3JvcHJvZmlsZS1qd3Qgb2ZmbGluZV9hY2Nlc3MgYWRkcmVzcyIsInNpZCI6ImNkNTViYjU0LTIxYjAtNGQ0YS1hMDQyLWYyM2FkMTliNGU1ZCIsInVzZXJfcm9sZSI6WyJST0xFX0FETUlOXzEzNTEiXSwidXBuIjoiNzM4MjIxMTIzMSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiYWRkcmVzcyI6e30sImdyb3VwcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl0sInByZWZlcnJlZF91c2VybmFtZSI6IjczODIyMTEyMzEiLCJlbWFpbCI6InJhamFAZ21haWwuY29tIiwidGVuYW50IjoicWF6b25lNiJ9.GdtX4p4sNMQIH1n1uHIZbtozCWRz-9EgKl6fxPU8ZfglwKBgzSc6NiASus63mY5JpA1pB50NUDeV_1zKet-tE3cKFw4-TsZep_ygbfEl2aiAyw1Rr5J1YcCmZY1oO_JO0958kzlzmh4Un0gIrhfn_R7Hy9Nf5az_hBwr2wYavRYFjqN4SvUKPNfciBZIHud5T4RNVWzQnPMHnSYcwwp6hIJiL-lhZposRfGrVnlp_0cU_DaC3Cxyjvnt2Hb2se_A5-QylgszuDnJb7Xles9wgVpOsaaOXPRMg5aC7IX46T_6rthbsCRyJtdid9BhL_HCTsBtCy05rGCL0IoTZht7PQ"

    if not token:
        print("❌ Token missing")
        exit()

    print(f" Starting multi-threaded execution ({MAX_WORKERS} threads)...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.submit(worker, "THREAD-1", token)
        executor.submit(worker, "THREAD-2", token)
        executor.submit(worker, "THREAD-3", token)
        executor.submit(worker, "THREAD-4", token)
