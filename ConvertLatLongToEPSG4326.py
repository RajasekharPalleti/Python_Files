"""
convert_utm32722_to_latlon.py

Converts geometry strings in EPSG:32722 (UTM zone 22S) to WGS84 lat/lon.
Input Excel must have a column named 'geometry_raw' with values like:
  [[[349870.0, 6710460.0], [349870.0, 6710450.0], ...]]]

Output column: geometry_latlon (JSON string of [[[lat, lon], ...]] format)
"""

import math
import ast
import json
import pandas as pd
from pyproj import Transformer

# === CONFIG ===
INPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\Tobacco_plots_detected_output.xlsx"
OUTPUT_EXCEL = r"C:\Users\rajasekhar.palleti\Downloads\Tobacco_plots_detected_output_new.xlsx"
INPUT_COLUMN = "geometry_raw"
OUTPUT_COLUMN = "geometry_latlon"
SOURCE_EPSG = "EPSG:32722"   # from your crs: urn:ogc:def:crs:EPSG::32722
TARGET_EPSG = "EPSG:4326"    # WGS84 lon/lat

# === Prepare transformer (always_xy=True so input is x,y and output is lon,lat) ===
transformer = Transformer.from_crs(SOURCE_EPSG, TARGET_EPSG, always_xy=True)

def convert_single_geometry_string(geometry_raw_str, round_decimals=7):
    """
    Convert a single geometry string (e.g. '[[[x,y],[x,y],...]]]')
    from SOURCE_EPSG -> TARGET_EPSG.
    Returns JSON string of the converted polygons with [lat, lon] pairs:
      e.g. '[[[lat, lon], [lat, lon], ...]]'
    """
    if geometry_raw_str is None:
        return None

    # Clean and parse the geometry string into Python list
    try:
        # geometry could contain extra whitespace/newlines
        data = ast.literal_eval(geometry_raw_str)
    except Exception as e:
        # try a small normalization (replace newlines)
        try:
            cleaned = geometry_raw_str.strip()
            data = ast.literal_eval(cleaned)
        except Exception as e2:
            return f"ERROR_PARSE: {e2}"

    # Expecting data like [ [ [x,y], [x,y], ... ] ]  (list of polygons)
    converted_polygons = []
    try:
        for polygon in data:
            converted_ring = []
            for pair in polygon:
                # support either [x,y] or (x,y)
                x = float(pair[0])
                y = float(pair[1])
                lon, lat = transformer.transform(x, y)  # returns lon, lat (because always_xy=True)
                # But user asked for [lat, lon] ordering earlier — we'll follow that
                converted_ring.append([round(lon, round_decimals), round(lat, round_decimals)])
            converted_polygons.append(converted_ring)
    except Exception as e:
        return f"ERROR_CONVERT: {e}"

    # Return as JSON string (keeps same nested list structure)
    return json.dumps(converted_polygons)

def main():
    # Read Excel
    df = pd.read_excel(INPUT_EXCEL)

    if INPUT_COLUMN not in df.columns:
        raise KeyError(f"Input column '{INPUT_COLUMN}' not found in Excel.")

    # Convert each row
    df[OUTPUT_COLUMN] = df[INPUT_COLUMN].apply(convert_single_geometry_string)

    # Save to new Excel
    df.to_excel(OUTPUT_EXCEL, index=False)
    print(f"Done. Output saved to: {OUTPUT_EXCEL}")

if __name__ == "__main__":
    main()
