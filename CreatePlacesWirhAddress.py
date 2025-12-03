import random
import requests
import json

# from GetAuthtoken import get_access_token  # your existing auth helper


def looks_like_mountain_or_water(formatted_address: str) -> bool:
    """Heuristically reject obvious water or mountain locations by address text."""
    fa = formatted_address.lower()
    for kw in WATER_KEYWORDS:
        if kw.lower() in fa:
            return True
    for kw in MOUNTAIN_KEYWORDS:
        if kw.lower() in fa:
            return True
    return False


def random_point_region(region_key: str):
    """Return random (lat, lng) from a configured region."""
    region = REGIONS[region_key.lower()]
    lat = random.uniform(*region["lat"])
    lng = random.uniform(*region["lng"])
    return lat, lng


def get_random_address_data(session: requests.Session, max_tries: int = 20) -> dict:

    if not GOOGLE_API_KEY:
        raise RuntimeError("Google API key is missing. Set GOOGLE_API_KEY before running.")

    for attempt in range(1, max_tries + 1):
        region_key = random.choice(list(REGIONS.keys()))
        lat, lng = random_point_region(region_key)

        params = {
            "latlng": f"{lat},{lng}",
            "key": GOOGLE_API_KEY,
            # Prefer addresses/locality; avoids pure political/natural-only features
            "result_type": "street_address|premise|sublocality|locality",
        }

        try:
            resp = session.get(GEOCODE_URL, params=params, timeout=10)
        except Exception:
            # Network issue, try another point
            continue

        try:
            data = resp.json()
        except ValueError:
            # Invalid JSON, retry
            continue

        if data.get("status") != "OK" or not data.get("results"):
            # No good result, retry with another random point
            continue

        result = data["results"][0]
        formatted_address = result.get("formatted_address", "")

        # Skip obvious mountain/water/park/etc by name
        if looks_like_mountain_or_water(formatted_address):
            continue

        types = result.get("types", [])
        # natural_feature/park often represent mountains, lakes, parks etc.
        if "natural_feature" in types or "park" in types:
            continue

        # Extract useful address components
        comps_raw = result.get("address_components", [])
        comps = {
            "country": "",
            "administrativeAreaLevel1": "",
            "administrativeAreaLevel2": "",
            "locality": "",
            "sublocalityLevel1": "",
            "sublocalityLevel2": "",
            "postalCode": "",
        }

        for c in comps_raw:
            t = c.get("types", [])
            if "country" in t:
                comps["country"] = c.get("long_name", "")
            elif "administrative_area_level_1" in t:
                comps["administrativeAreaLevel1"] = c.get("long_name", "")
            elif "administrative_area_level_2" in t:
                comps["administrativeAreaLevel2"] = c.get("long_name", "")
            elif "locality" in t:
                comps["locality"] = c.get("long_name", "")
            elif "sublocality_level_1" in t:
                comps["sublocalityLevel1"] = c.get("long_name", "")
            elif "sublocality_level_2" in t:
                comps["sublocalityLevel2"] = c.get("long_name", "")
            elif "postal_code" in t:
                comps["postalCode"] = c.get("long_name", "")

        geometry = result.get("geometry", {}).get("location", {})
        glat = geometry.get("lat", lat)
        glng = geometry.get("lng", lng)

        address_data = {
            "country": comps["country"],
            "formattedAddress": formatted_address,
            "administrativeAreaLevel1": comps["administrativeAreaLevel1"],
            "administrativeAreaLevel2": comps["administrativeAreaLevel2"],
            "locality": comps["locality"],
            "sublocalityLevel1": comps["sublocalityLevel1"],
            "sublocalityLevel2": comps["sublocalityLevel2"],
            "landmark": "",
            "postalCode": comps["postalCode"],
            "houseNo": "",
            "buildingName": "",
            "placeId": result.get("place_id", ""),
            "latitude": glat,
            "longitude": glng,
        }

        return address_data

    raise RuntimeError("Could not find a suitable land address after several attempts.")


def build_payload(place_name: str, address_data: dict) -> dict:
    TYPES = ["PORT", "WAREHOUSE", "SUPPLIER", "MILLS"]
    place_type = random.choice(TYPES)
    return {
        "data": None,
        "name": place_name,
        "type": place_type,
        "address": address_data,
        "latitude": address_data["latitude"],
        "longitude": address_data["longitude"],
    }


def main():
    # Get auth token using your existing method
    # token = get_access_token()
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJaS08wSFZ2OGlVVmxzQTl6THFBUjhEOWVMc3NYNlVYTERWRkUzdkJ1N0lJIn0.eyJqdGkiOiIzNDMwNzg4OC1jODAwLTRlOWYtOTUzZC00ZTUyZmRiMDk0MjMiLCJleHAiOjE3NjQ0OTU5NzQsIm5iZiI6MCwiaWF0IjoxNzY0MzIzMTczLCJpc3MiOiJodHRwczovL3Yyc3NvLXVhdC1nY3AuY3JvcGluLmNvLmluL2F1dGgvcmVhbG1zL3VhdHpvbmUxIiwiYXVkIjpbInJlc291cmNlX3NlcnZlciIsImFjY291bnQiXSwic3ViIjoiMTZlYjliYWQtY2Y1OS00N2Y0LWI2YzAtYjdhNTAxNjEzYjhlIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoid2ViX2FwcCIsIm5vbmNlIjoiX2NsaWVudF93ZWJfYXBwIiwiYXV0aF90aW1lIjoxNzY0MzIzMTczLCJzZXNzaW9uX3N0YXRlIjoiNmFmYzJhODUtNGEyZS00YmEwLTkxYTAtMGQ3OGM2NzRhYjMyIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsicmVzb3VyY2Vfc2VydmVyIjp7InJvbGVzIjpbIlJhamEgUm9sZV9BZG1pbiByb2xlXzM2MjYwMSJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgbWljcm9wcm9maWxlLWp3dCBwaG9uZSBwcm9maWxlIG9mZmxpbmVfYWNjZXNzIGVtYWlsIGFkZHJlc3MiLCJ1c2VyX3JvbGUiOlsiUmFqYSBSb2xlX0FkbWluIHJvbGVfMzYyNjAxIl0sInVwbiI6IjczODIyMTEzMDIiLCJhZGRyZXNzIjp7fSwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJncm91cHMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiI3MzgyMjExMzAyIiwidGVuYW50IjoidWF0em9uZTEiLCJlbWFpbCI6InJhamFzZWtoYXIucGFsbGV0aTFAY3JvcGluLmNvbSJ9.dVilXDTsppUaqeFpltYtkbmxzXrac39GYLTzXsjDYpKC1imi17tENM8USMcvm8Y5KB7iWEwF2Dpz-e8IISaaybBRTHI6x-_9bGc-etbmhh7oBr5JpSKj03KmHau_3N8GI_OQbNGVe0JAEzPx_r84_TWMAF00RkFlCdlefKruFS1A3F8k6rzNYmsejw0mPugdlEwXPIO7pZv4_I_NwcuhPY4dJcSTqcV5h-y1zJ2sr2mzEA8KBPcIKrNBATgPAQ9tw4bPmQKn0-KcZmlr3MMg59LcfN8NZagMv7_jtkQFFh90fns7OxkWpEQevCam2vVvjm4TSvMDlSdp5SOvzDrq9w"
    if not token:
        print("❌ Failed to generate token. Aborting.")
        return

    headers = {
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
    }

    session = requests.Session()

    for i in range(1, TOTAL_PLACES + 1):
        place_name = f"{BASE_PLACE_NAME} {i:02d}"  # Raja place 01, 02, ...

        print(f"\n📍 Creating place {i}/{TOTAL_PLACES}: {place_name}")

        try:
            # Get a random land address via Google API
            address_data = get_random_address_data(session)
            print(f"   🌍 Address → {address_data['formattedAddress']}")

            payload = build_payload(place_name, address_data)

            resp = session.post(
                PLACE_API_URL,
                headers=headers,
                data=json.dumps(payload),
                timeout=30,
            )

            if resp.status_code in (200, 201):
                try:
                    data = resp.json()
                    pid = data.get("id") or data.get("data", {}).get("id")
                    print(f"   ✅ Success → ID: {pid}")
                except Exception:
                    print("   ⚠️ Created but JSON parse failed")
            else:
                print(f"   ❌ Failed ({resp.status_code}) → {resp.text[:200]}")

        except Exception as e:
            print(f"   ❌ Error while creating place: {e}")

    print("\n🎉 Completed creating places.")


# ---------------- CONFIG / CONSTANTS (user-editable) ---------------- #

PLACE_API_URL = "https://v2uat-gcp.cropin.co.in/uat1/services/farm/api/place"
BASE_PLACE_NAME = "Raja random address place"
TOTAL_PLACES = 100  # change to small number for testing

# Google Geocoding API
GOOGLE_API_KEY = "AIzaSyAwy--7hbQ9x-_rFT2lCi52o0rF0JvbA7E"
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

# Keywords to avoid obvious seas/oceans/water and mountains/hills/parks
WATER_KEYWORDS = (
    "sea", "ocean", "bay", "gulf", "lake", "river",
    "lagoon", "harbour", "harbor", "canal", "backwater", "beach"
)

MOUNTAIN_KEYWORDS = (
    "mountain", "mt ", "mount ", "peak", "hill",
    "hills", "ghats", "national park", "wildlife sanctuary", "forest", "reserve"
)

# Random location regions for:
#   - india
#   - brazil
#   - africa
#   - america (north & south, trimmed to avoid too much water)
#   - russia
#   - australia
REGIONS = {
    "india": {
        "lat": (8.0, 37.5),
        "lng": (68.0, 97.5),
    },
    "brazil": {
        "lat": (-34.0, 5.3),
        "lng": (-74.0, -34.0),
    },
    "africa": {
        "lat": (-35.0, 37.0),
        "lng": (-18.0, 52.0),
    },
    "america": {
        # big band across the Americas, still includes some water but we filter via geocoding
        "lat": (-55.0, 70.0),
        "lng": (-168.0, -30.0),
    },
    "russia": {
        "lat": (41.0, 82.0),
        "lng": (19.0, 180.0),
    },
    "australia": {
        "lat": (-44.0, -10.0),
        "lng": (112.0, 154.0),
    },
}


if __name__ == "__main__":
    main()
