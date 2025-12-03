# Author: Rajasekhar Palleti
# Robust area + centroid calculator that accepts multiple input coordinate formats.
# Outputs the first column "Coordinates" as a WGS84 GeoJSON FeatureCollection (MultiPolygon).

import pandas as pd
import ast
import json
from pyproj import Geod, CRS, Transformer

try:
    from shapely.geometry import Polygon
    _HAS_SHAPELY = True
except Exception:
    Polygon = None
    _HAS_SHAPELY = False

# === File Paths ===
INPUT_FILE = r"C:\Users\rajasekhar.palleti\Downloads\Tobacco_plots_detected_output_new.xlsx"
OUTPUT_FILE = r"C:\Users\rajasekhar.palleti\Downloads\Tobacco_plots_detected_output_main2.xlsx"

# === Config ===
ROUND_OFF_DECIMALS = 16   # area decimals
CENTER_DECIMALS = 14      # centroid decimals

# Default projected CRS to assume when coords look like meters (change if needed)
INPUT_CRS = "EPSG:32722"     # <-- used for rows that look projected (e.g. UTM meters)
INPUT_COORD_ORDER = "auto"   # only used when treating a row as EPSG:4326 degrees
                            # values: "auto" | "latlon" | "lonlat"

UNITS_TO_CALCULATE = ["hectare", "acre"]
UNIT_CONVERSION = {
    "squaremeter": 1.0,
    "hectare": 1.0 / 10000.0,
    "acre": 1.0 / 4046.8564224
}

GEOD = Geod(ellps="WGS84")

# ---- Helpers ----

def is_number(x):
    try:
        float(x)
        return True
    except Exception:
        return False

def looks_like_degrees_pair(pair):
    """
    Return True if (a,b) plausibly look like lon/lat degrees in any order.
    Accepts pairs like (lon,lat) or (lat,lon).
    """
    try:
        a = float(pair[0])
        b = float(pair[1])
    except Exception:
        return False
    # quick check: if both within lon/lat ranges then likely degrees
    if (-180 <= a <= 180) and (-90 <= b <= 90):
        return True
    if (-90 <= a <= 90) and (-180 <= b <= 180):
        return True
    return False

def detect_order_deg(first_pair):
    """
    Detect order when in degrees.
    Returns "lonlat" or "latlon".
    """
    a, b = float(first_pair[0]), float(first_pair[1])
    # If (lon, lat) => a in [-180,180] and b in [-90,90]
    if (-180 <= a <= 180) and (-90 <= b <= 90):
        return "lonlat"
    # If (lat, lon) => a in [-90,90] and b in [-180,180]
    if (-90 <= a <= 90) and (-180 <= b <= 180):
        return "latlon"
    return "lonlat"

def find_coords_list(obj):
    """
    Recursively search for the first list of coordinate pairs inside nested lists/dicts.
    Returns a list of pairs like [[x,y],[x,y],...]
    If nothing found, returns [].
    Handles:
      - raw nested lists: [[[x,y],...]] or [[x,y],...]
      - GeoJSON-like dicts with 'coordinates' key (MultiPolygon/Polygon)
    """
    # If it's a dict and has 'coordinates', use that
    if isinstance(obj, dict):
        if "coordinates" in obj:
            return find_coords_list(obj["coordinates"])
        # sometimes geometry: {"type": ..., "coordinates": ...}
        if "geometry" in obj and isinstance(obj["geometry"], dict) and "coordinates" in obj["geometry"]:
            return find_coords_list(obj["geometry"]["coordinates"])
        # no coordinates found
        return []

    # If it's not a list, cannot proceed
    if not isinstance(obj, (list, tuple)):
        return []

    # If it's a list of numbers => not coordinates
    # If it's a list of pairs (numbers), return that
    if len(obj) > 0 and all(is_number(x) for x in obj):
        # single numeric list -> cannot interpret as pairs
        return []

    # If it's a list whose elements are pairs of numbers: [[x,y], [x,y], ...]
    if len(obj) > 0 and isinstance(obj[0], (list, tuple)) and len(obj[0]) >= 2 and is_number(obj[0][0]) and is_number(obj[0][1]):
        # We will take the first element level that is pairs:
        # But sometimes there are extra nesting levels like [[[x,y],...]] -> handled below
        # Confirm elements are pairs; if elements themselves are lists of pairs, recurse
        if all(isinstance(el, (list, tuple)) and len(el) >= 2 and is_number(el[0]) and is_number(el[1]) for el in obj):
            return [ [float(el[0]), float(el[1])] for el in obj ]
    # If first element is a list/tuple but not a numeric pair, dive deeper
    for el in obj:
        res = find_coords_list(el)
        if res:
            return res
    return []

def to_lonlat_wgs84(coords):
    """
    Normalize input coords (various nested forms) to list of (lon, lat) tuples in WGS84.
    Behavior per-row:
     - If the detected coordinate pairs look like degrees -> treat as EPSG:4326 (with INPUT_COORD_ORDER logic)
     - Else treat as projected coordinates in INPUT_CRS and transform -> EPSG:4326
    """
    # Extract a ring (list of [x,y]) from arbitrary nested structures
    ring = find_coords_list(coords)
    if not ring:
        return []

    # pick a sample pair (first) to decide if it's degrees
    sample = ring[0]
    if looks_like_degrees_pair(sample):
        # It's degrees. Determine order
        order = INPUT_COORD_ORDER
        if order == "auto":
            order = detect_order_deg(sample)
        if order == "latlon":
            # input is (lat,lon) -> convert to (lon,lat)
            return [(float(p[1]), float(p[0])) for p in ring]
        else:
            # input is (lon,lat)
            return [(float(p[0]), float(p[1])) for p in ring]
    else:
        # Treat as projected coordinates in INPUT_CRS -> transform to EPSG:4326
        try:
            crs_src = CRS.from_string(INPUT_CRS)
            crs_dst = CRS.from_epsg(4326)
            tr = Transformer.from_crs(crs_src, crs_dst, always_xy=True)
            xs = [float(p[0]) for p in ring]
            ys = [float(p[1]) for p in ring]
            lons, lats = tr.transform(xs, ys)
            return list(zip(list(lons), list(lats)))
        except Exception as e:
            # transform failed: fallback to treating them as lon,lat as-is
            print(f"  [WARN] CRS transform failed ({e}). Attempting to use raw values as lon,lat fallback.")
            try:
                return [(float(p[0]), float(p[1])) for p in ring]
            except Exception:
                return []

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

def find_column_ci(df, col_name):
    """Return the DataFrame column name that matches col_name case-insensitively, or None."""
    if col_name in df.columns:
        return col_name
    lower_to_col = {c.lower(): c for c in df.columns}
    return lower_to_col.get(col_name.lower())

def make_featurecollection_from_lonlat(lonlat):
    """
    Build a GeoJSON FeatureCollection containing a single MultiPolygon using WGS84 lon/lat pairs.
    The input `lonlat` is a list of (lon, lat) tuples.
    Returns a Python dict suitable for json.dumps, matching:
    {"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiPolygon","coordinates":[[[[lon,lat], ...]]]}}]}
    """
    if not lonlat or len(lonlat) < 1:
        return {"type": "FeatureCollection", "features": []}

    ring = [[float(pt[0]), float(pt[1])] for pt in lonlat]

    # Ensure closure
    if ring[0] != ring[-1]:
        ring.append(ring[0])

    # MultiPolygon expects: [ [ [ [lon,lat], ... ] ] ]
    coords = [[ring ]]
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "MultiPolygon", "coordinates": coords},
                "properties": {}
            }
        ]
    }

# ---- Processing function (callable) ----

def process_file(input_file=None, output_file=None, max_rows=None):
    """
    Read input Excel, compute area/centroid, and write output Excel.
    Returns the output DataFrame.
    - input_file/output_file default to module-level constants when None.
    - max_rows: if provided, only process the first `max_rows` rows.
    """
    in_file = input_file or INPUT_FILE
    out_file = output_file or OUTPUT_FILE

    try:
        print("📥 Reading input Excel file...")
        df = pd.read_excel(in_file)
        print(f"✅ Successfully read {len(df)} rows from {in_file}")
    except Exception as e:
        print(f"[ERROR] Failed to read input Excel file: {e}")
        return None

    geom_col = find_column_ci(df, "geometry_raw")
    if geom_col is None:
        print("[WARN] Input does not contain 'geometry_raw' column (case-insensitive). Using first column as fallback.")
        geom_col = df.columns[0]

    results = []
    rows = df.head(max_rows) if max_rows is not None else df

    for disp_idx, (_, row) in enumerate(rows.iterrows(), start=1):
        print(f"\n📍 Processing row: {disp_idx}")
        coords_geojson = ""
        area_m2 = 0.0
        c_lat, c_lon = 0.0, 0.0

        try:
            raw_val = row.get(geom_col, "")
            print(f"  ➡️ Raw input (first 200 chars): {str(raw_val)[:200]}")

            # Parse string representations safely (if needed)
            if isinstance(raw_val, str):
                try:
                    parsed = ast.literal_eval(raw_val)
                except Exception:
                    # maybe it's a plain string that isn't Python literal; leave as-is
                    parsed = raw_val
            else:
                parsed = raw_val

            # Convert parsed structure to lon/lat WGS84 tuples
            lonlat = to_lonlat_wgs84(parsed)

            # Ensure closure for calculations
            if len(lonlat) >= 3 and lonlat[0] != lonlat[-1]:
                lonlat.append(lonlat[0])

            # Compute area and centroid
            area_m2 = calculate_area_sq_m(lonlat)
            c_lat, c_lon = compute_centroid(lonlat)
            print(f"  ✅ Area (m²): {area_m2}")
            print(f"  ✅ Centroid (lat, lon): ({c_lat}, {c_lon})")

            # Build GeoJSON FeatureCollection from the WGS84 lon/lat coordinates
            if lonlat and len(lonlat) >= 1:
                fc = make_featurecollection_from_lonlat(lonlat)
            else:
                # fallback: try to build from any coords found earlier
                coords_fallback = find_coords_list(parsed)
                if coords_fallback:
                    try:
                        # try to coerce them to lonlat pairs as floats (assume order is lon,lat)
                        temp = [(float(p[0]), float(p[1])) for p in coords_fallback]
                        fc = make_featurecollection_from_lonlat(temp)
                    except Exception:
                        fc = {"type":"FeatureCollection","features":[]}
                else:
                    fc = {"type":"FeatureCollection","features":[]}

            coords_geojson = json.dumps(fc, ensure_ascii=False)

        except Exception as e:
            print(f"  [ROW {disp_idx} ERROR] {e}")
            # on error, attempt safe fallback
            try:
                parsed = ast.literal_eval(row.get(geom_col, "")) if isinstance(row.get(geom_col, ""), str) else row.get(geom_col, "")
                coords_fallback = find_coords_list(parsed) or []
                temp = []
                for p in coords_fallback:
                    try:
                        temp.append((float(p[0]), float(p[1])))
                    except Exception:
                        continue
                fc = make_featurecollection_from_lonlat(temp) if temp else {"type":"FeatureCollection","features":[]}
                coords_geojson = json.dumps(fc, ensure_ascii=False)
            except Exception:
                coords_geojson = json.dumps({"type":"FeatureCollection","features":[]})

        # Build result row: Coordinates (GeoJSON string), Center Latitude, Center Longitude, then unit values
        result_row = [coords_geojson, c_lat, c_lon]
        for unit in UNITS_TO_CALCULATE:
            converted = round(area_m2 * UNIT_CONVERSION[unit], ROUND_OFF_DECIMALS)
            print(f"  ➡️ Converted area: {converted} {unit}")
            result_row.append(converted)
            result_row.append(unit)

        results.append(result_row)
        print(f"  ✅ Row {disp_idx} processed.")

    # Columns
    columns = ["Coordinates", "Center Latitude", "Center Longitude"]
    for unit in UNITS_TO_CALCULATE:
        columns += [f"Value ({unit})", "Unit"]

    print("\n📝 Creating output DataFrame...")
    out_df = pd.DataFrame(results, columns=columns)
    print(f"✅ Output DataFrame with {len(out_df)} rows.")

    print(f"💾 Saving results to {out_file} ...")
    try:
        out_df.to_excel(out_file, index=False)
        print(f"✅ Done. Saved to: {out_file}")
    except Exception as e:
        print(f"[ERROR] Failed to save output file: {e}")

    return out_df

# ---- Legacy entrypoint ----

if __name__ == "__main__":
    print("🚀 Starting area calculation script...")
    process_file()
    print("🏁 Script execution finished.")
