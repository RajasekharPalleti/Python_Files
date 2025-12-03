import requests
import json
import time
import copy

# ========== CONFIGURATION ==========
API_URL = "https://af-v2-gcp.cropin.co.in/qa5/services/user/api/user-roles"
AUTH_TOKEN = "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJDeVNZVFNPWW95Q19DcENmaTNNZHBTdHFvd0VPczFBS0ZSZE1YMXVVYkgwIn0.eyJleHAiOjE3NjI5NjE0OTUsImlhdCI6MTc2MjkzOTg5NSwiYXV0aF90aW1lIjoxNzYyOTM5ODYzLCJqdGkiOiJjNDkzY2EyZS1kOTM2LTQ1ZjEtYjdiMy00MTRmNGY3MDA2MDAiLCJpc3MiOiJodHRwczovL3Yyc3NvLWdjcC5jcm9waW4uY28uaW4vYXV0aC9yZWFsbXMvcWF6b25lNSIsImF1ZCI6WyJyZXNvdXJjZV9zZXJ2ZXIiLCJhY2NvdW50Il0sInN1YiI6IjM5OWQ3MWEyLWVjZGUtNGZiMi04ZDhmLTY1YjY2MDEwZDM1YSIsInR5cCI6IkJlYXJlciIsImF6cCI6IndlYl9hcHAiLCJub25jZSI6Il9jbGllbnRfd2ViX2FwcCIsInNlc3Npb25fc3RhdGUiOiJmYTI4NjdmZC01MTVmLTQwNWEtYTA5YS1lYjE3YzI5MWUwZjYiLCJhY3IiOiIwIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJyZXNvdXJjZV9zZXJ2ZXIiOnsicm9sZXMiOlsiUmFqYSBUZXN0IHVzZXIgcm9sZV8zODUxIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBlbWFpbCBwcm9maWxlIG9mZmxpbmVfYWNjZXNzIHBob25lIGFkZHJlc3MgbWljcm9wcm9maWxlLWp3dCIsInNpZCI6ImZhMjg2N2ZkLTUxNWYtNDA1YS1hMDlhLWViMTdjMjkxZTBmNiIsInVzZXJfcm9sZSI6WyJSYWphIFRlc3QgdXNlciByb2xlXzM4NTEiXSwidXBuIjoiNzM4ODg4ODg4OCIsImFkZHJlc3MiOnt9LCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImdyb3VwcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl0sInByZWZlcnJlZF91c2VybmFtZSI6IjczODg4ODg4ODgiLCJ0ZW5hbnQiOiJxYXpvbmU1IiwiZW1haWwiOiJyYWphdGVzdHFhNUBnbWFpbC5jb20ifQ.V-Sc846wanEmDq7xuR67yPiRmHmkqAfPeELrhWgmp-QMHxQ-YJMdrljop_0weFf6caScRiXXWopRPGEtw0e-qrsDAAO6qITJ0_tfLX1T_T2_FG4oQ3523HgSGF_1jLNnfQEuA3J-pS2O57weA6w1eICK7Hjp6JKz9iJyudToQtgTyNIIJQzq3nLa86I9_2_GDvLO-_wMlIs_PAxHr2VPVFaiER6yMDKtc0zaj7fM2kUMIPbo75jCoCWW7Z5R7lfkPvbUqGiuDrwOJUaj9gyXZthTS8R0Ja5fIFdiCP-0jats6YJBVxqznbQvAGAE7GGEda5lnmK33n5jeDcBiNnfGg"  # Replace with your real token
USER_ID = 3951
COMPANY_ID = 1251
TOTAL_ROLES = 100
DELAY_SECONDS = 1
rolestartsfrom = 101

# ========== SAMPLE PAYLOAD TEMPLATE ==========
headersweb = {
    "Authorization": AUTH_TOKEN,
    "Content-Type": "application/json",
    "channel": "web"
}
headersmobile = {
    "Authorization": AUTH_TOKEN,
    "Content-Type": "application/json",
    "channel": "mobile"
}

try:
    response = requests.get(f"{API_URL}/permissions?companyId={COMPANY_ID}", headers=headersweb)
    if response.status_code in (200, 201):
        print(f"✅ Fetched Web permissions for company {COMPANY_ID}")
        webpermissions = response.json()
    else:
        webpermissions = None
        print(f"❌ Failed to fetch permissions ({response.status_code}): {response.text}")
except Exception as e:
    webpermissions = None
    print(f"⚠️ Exception fetching permissions: {e}")

try:
    response = requests.get(f"{API_URL}/permissions?companyId={COMPANY_ID}", headers=headersmobile)
    if response.status_code in (200, 201):
        print(f"✅ Fetched mobile permissions for company {COMPANY_ID}")
        mobilepermissions = response.json()
    else:
        mobilepermissions = None
        print(f"❌ Failed to fetch permissions ({response.status_code}): {response.text}")
except Exception as e:
    mobilepermissions = None
    print(f"⚠️ Exception fetching permissions: {e}")

def enable_permissions_in_resources(data):
    """Traverse resource objects and set permission 'enabled'/'enable' to True."""
    modified = 0
    if isinstance(data, dict):
        resources = data.get("resources") if isinstance(data.get("resources"), list) else [data]
    elif isinstance(data, list):
        resources = data
    else:
        return 0

    for res in resources:
        if not isinstance(res, dict):
            continue
        perms = res.get("permissions")
        if isinstance(perms, list):
            for p in perms:
                if not isinstance(p, dict):
                    continue
                if "enabled" in p and p.get("enabled") is not True:
                    p["enabled"] = True
                    modified += 1
                if "enable" in p and p.get("enable") is not True:
                    p["enable"] = True
                    modified += 1
    return modified

modified_web = enable_permissions_in_resources(webpermissions) if webpermissions else 0
modified_mobile = enable_permissions_in_resources(mobilepermissions) if mobilepermissions else 0
print(f"✅ Enabled permissions updated: web={modified_web}, mobile={modified_mobile}")

# Combine into single JSON-serializable permissions object
permissions = {
    "webPermissions": webpermissions if webpermissions is not None else [],
    "mobilePermissions": mobilepermissions if mobilepermissions is not None else []
}
permissions_json = json.dumps(permissions)
print("✅ Combined permissions into permissions JSON")

# Normalize permission responses into a list of resource objects suitable for the payload
def _normalize_resources(obj):
    if not obj:
        return []
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        # If the response has a 'resources' key, use that
        if isinstance(obj.get("resources"), list):
            return obj.get("resources")
        # otherwise treat the dict itself as a single resource
        return [obj]
    return []

combined_resources = _normalize_resources(webpermissions) + _normalize_resources(mobilepermissions)

BASE_PAYLOAD = {
    "userId": USER_ID,
    "resources": combined_resources,
    "name": "Raja navigation check 01",
    "userRoleCode": "navigation_check",
    "userRoleStatus": "ACTIVE",
    "companyId": COMPANY_ID,
    "permissionName": "Raja navigation check 01",
    "userRoleType": "DEFAULT"
}

# ========== FUNCTION ==========
def create_user_role(role_number):
    """Create a single user role via Cropin API."""
    role_name = f"Raja navigation check {role_number:02d}"
    # use a deep copy to avoid mutating BASE_PAYLOAD across iterations
    payload = copy.deepcopy(BASE_PAYLOAD)
    payload["name"] = role_name
    payload["permissionName"] = role_name
    payload["userRoleCode"] = f"navigation_check_{role_number:02d}"

    headers = {
        "Authorization": AUTH_TOKEN,
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(API_URL, headers=headers, json=payload)
        if response.status_code in (200, 201):
            print(f"✅ Created Role: {role_name}")
        else:
            print(f"❌ Failed ({response.status_code}) for {role_name}: {response.text}")
    except Exception as e:
        print(f"⚠️ Exception creating {role_name}: {e}")

# ========== EXECUTION ==========
if __name__ == "__main__":
    print(f"🚀 Starting to create {TOTAL_ROLES} user roles...")
    for i in range(rolestartsfrom, TOTAL_ROLES+rolestartsfrom + 1):
        create_user_role(i)
        time.sleep(DELAY_SECONDS)
    print("🎯 All roles creation completed!")
