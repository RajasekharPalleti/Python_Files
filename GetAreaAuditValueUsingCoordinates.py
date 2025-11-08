# Author: Rajasekhar Palleti
# Calculates polygon area and centroid from coordinates in Excel, with robust
# handling of coordinate order and CRS.

import pandas as pd
import ast
from pyproj import Geod, CRS, Transformer

try:
    from shapely.geometry import Polygon
    _HAS_SHAPELY = True
except ImportError:
    _HAS_SHAPELY = False

# === File Paths ===
INPUT_FILE = r"C:\Users\rajasekhar.palleti\Downloads\santacruz_tobacco_plots_output.xlsx"
OUTPUT_FILE = r"C:\Users\rajasekhar.palleti\Downloads\santacruz_tobacco_plots_output new.xlsx"

# === Config ===
ROUND_OFF_DECIMALS = 16   # area decimals
CENTER_DECIMALS = 14      # centroid decimals

# If your Excel holds degrees: keep EPSG:4326
# If your Excel holds projected meters, set the EPSG here (e.g., EPSG:32722 for UTM Zone 22S)
INPUT_CRS = "EPSG:4326"     # <-- change if your numbers are UTM/meters
INPUT_COORD_ORDER = "auto"  # "auto" | "latlon" | "lonlat"
                            # Auto only applies when INPUT_CRS is EPSG:4326 (degrees)

UNITS_TO_CALCULATE = ["hectare", "acre"]
UNIT_CONVERSION = {
    "squaremeter": 1.0,
    "hectare": 1.0 / 10000.0,
    "acre": 1.0 / 4046.8564224
}

GEOD = Geod(ellps="WGS84")

# ---- Helpers ----

def flatten_ring(coords):
    """Handles [[[...]]] -> [[...]]"""
    if coords and isinstance(coords[0], (list, tuple)) and coords and \
       len(coords[0]) > 0 and isinstance(coords[0][0], (list, tuple)):
        return coords[0]
    return coords

def looks_like_degrees(pair):
    """Return True if values plausibly look like lon/lat degrees."""
    a, b = pair
    return (-180 <= a <= 180) and (-90 <= b <= 90)

def detect_order_deg(first_pair):
    """
    Detect order when in degrees.
    Returns "lonlat" or "latlon".
    """
    a, b = first_pair
    # If (lon, lat) => a in [-180,180] and b in [-90,90]
    if (-180 <= a <= 180) and (-90 <= b <= 90):
        return "lonlat"
    # If (lat, lon) => a in [-90,90] and b in [-180,180]
    if (-90 <= a <= 90) and (-180 <= b <= 180):
        return "latlon"
    # Fallback to lonlat (GeoJSON default)
    return "lonlat"

def to_lonlat_wgs84(coords):
    """
    Normalize input coords to (lon, lat) in WGS84 degrees.
    """
    coords = flatten_ring(coords)

    if INPUT_CRS.upper() == "EPSG:4326":
        # Coordinates already in degrees. Figure out their order.
        if INPUT_COORD_ORDER == "latlon":
            lonlat = [(lon, lat) for (lat, lon) in coords]
        elif INPUT_COORD_ORDER == "lonlat":
            lonlat = [(lon, lat) for (lon, lat) in coords]
        else:  # auto
            if not coords:
                return []
            order = detect_order_deg(coords[0])
            if order == "latlon":
                lonlat = [(lon, lat) for (lat, lon) in coords]
            else:
                lonlat = [(lon, lat) for (lon, lat) in coords]
        return lonlat

    # Otherwise: transform from INPUT_CRS -> EPSG:4326
    crs_src = CRS.from_string(INPUT_CRS)
    crs_dst = CRS.from_epsg(4326)
    tr = Transformer.from_crs(crs_src, crs_dst, always_xy=True)

    # For projected CRSs, treat input pairs as (x, y) -> (lon, lat)
    xs = [p[0] for p in coords]
    ys = [p[1] for p in coords]
    lons, lats = tr.transform(xs, ys)
    return list(zip(lons, lats))

def calculate_area_sq_m(lonlat):
    """Geodesic area on WGS84 from (lon, lat) pairs."""
    try:
        if len(lonlat) < 3:
            return 0.0
        lons, lats = zip(*lonlat)
        area_m2, _ = GEOD.polygon_area_perimeter(lons, lats)
        return abs(area_m2)
    except Exception as e:
        print(f"  [ERROR] Area calculation failed: {e}")
        return 0.0

def compute_centroid(coords_wgs84_lonlat):
    """
    Geodesically sensible centroid via local equal-area projection.
    Returns (lat, lon).
    """
    try:
        if len(coords_wgs84_lonlat) < 3:
            return 0.0, 0.0

        lons = [pt[0] for pt in coords_wgs84_lonlat]
        lats = [pt[1] for pt in coords_wgs84_lonlat]
        lon0 = sum(lons) / len(lons)
        lat0 = sum(lats) / len(lats)

        if not _HAS_SHAPELY:
            return round(lat0, CENTER_DECIMALS), round(lon0, CENTER_DECIMALS)

        crs_src = CRS.from_epsg(4326)
        crs_dst = CRS.from_proj4(
            f"+proj=aeqd +lat_0={lat0} +lon_0={lon0} +datum=WGS84 +units=m +no_defs"
        )
        fwd = Transformer.from_crs(crs_src, crs_dst, always_xy=True)
        inv = Transformer.from_crs(crs_dst, crs_src, always_xy=True)

        xs, ys = fwd.transform(lons, lats)
        poly = Polygon(zip(xs, ys))
        if (not poly.is_valid) or poly.area == 0:
            return round(lat0, CENTER_DECIMALS), round(lon0, CENTER_DECIMALS)

        c = poly.centroid
        clon, clat = inv.transform(c.x, c.y)
        return round(clat, CENTER_DECIMALS), round(clon, CENTER_DECIMALS)
    except Exception as e:
        print(f"  [ERROR] Centroid calculation failed ({e}). Using average.")
        lons = [pt[0] for pt in coords_wgs84_lonlat]
        lats = [pt[1] for pt in coords_wgs84_lonlat]
        lon0 = sum(lons) / len(lons)
        lat0 = sum(lats) / len(lats)
        return round(lat0, CENTER_DECIMALS), round(lon0, CENTER_DECIMALS)

# ---- Main ----

def main():
    try:
        print("📥 Reading input Excel file...")
        df = pd.read_excel(INPUT_FILE)
        print(f"✅ Successfully read {len(df)} rows from {INPUT_FILE}")
    except Exception as e:
        print(f"[ERROR] Failed to read input Excel file: {e}")
        return

    results = []
    for idx, row in df.iterrows():
        print(f"\n📍 Processing row: {idx + 1}")
        area_m2 = 0.0
        c_lat, c_lon = 0.0, 0.0
        coord_str = ""

        try:
            coord_str = row.iloc[7]  # Assuming Raw coordinates are in column H (index 7)
            print(f"  ➡️ Raw coordinate string: {coord_str}")
            coords = ast.literal_eval(str(coord_str))

            # Normalize to Geo ring and WGS84 lon/lat
            lonlat = to_lonlat_wgs84(coords)

            # Ensure the ring is closed (optional; Geod doesn’t require, but it helps validity checks)
            if len(lonlat) >= 3 and lonlat[0] != lonlat[-1]:
                lonlat.append(lonlat[0])

            area_m2 = calculate_area_sq_m(lonlat)
            c_lat, c_lon = compute_centroid(lonlat)
            print(f"  ✅ Area (m²): {area_m2}")
            print(f"  ✅ Centroid (lat, lon): ({c_lat}, {c_lon})")

        except Exception as e:
            print(f"  [ROW {idx+1} ERROR] {e}")

        # Build result row
        result_row = [coord_str, c_lat, c_lon]
        for unit in UNITS_TO_CALCULATE:
            converted = round(area_m2 * UNIT_CONVERSION[unit], ROUND_OFF_DECIMALS)
            print(f"  ➡️ Converted area: {converted} {unit}")
            result_row.append(converted)
            result_row.append(unit)

        results.append(result_row)
        print(f"  ✅ Row {idx + 1} processed.")

    # Columns
    columns = ["Coordinates", "Center Latitude", "Center Longitude"]
    for unit in UNITS_TO_CALCULATE:
        columns += [f"Value ({unit})", "Unit"]

    print("\n📝 Creating output DataFrame...")
    out_df = pd.DataFrame(results, columns=columns)
    print(f"✅ Output DataFrame with {len(out_df)} rows.")

    print(f"💾 Saving results to {OUTPUT_FILE} ...")
    out_df.to_excel(OUTPUT_FILE, index=False)
    print(f"✅ Done. Saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    print("🚀 Starting area calculation script...")
    main()
    print("🏁 Script execution finished.")
