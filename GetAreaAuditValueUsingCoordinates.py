# Author: Rajasekhar Palleti
# This script calculates the area of polygons defined by coordinates in an Excel file
# and converts the area into multiple units (hectare, acre, etc.).
# co ordinates format: [[lat1, lon1], [lat2, lon2], ...]
# The results are saved back to a new Excel file.

import pandas as pd
import ast
from pyproj import Geod

# File Paths
INPUT_FILE = r"C:\Users\rajasekhar.palleti\Downloads\Coordinates.xlsx"
OUTPUT_FILE = r"C:\Users\rajasekhar.palleti\Downloads\Coordinates_output.xlsx"

ROUND_OFF_DECIMALS = 16  # Decimal places to round off

# Units you want to convert area into
UNITS_TO_CALCULATE = ["hectare", "acre"]  # You can add "squaremeter" here

# Conversion factors (from square meters)
UNIT_CONVERSION = {
    "squaremeter": 1,
    "hectare": 1 / 10000,
    "acre": 1 / 4046.8564224
}

# Initialize geodetic calculator
GEOD = Geod(ellps="WGS84")

# === Function to calculate area in square meters ===

def calculate_area(coords):
    try:
        if len(coords) < 3:
            print("  ⚠️ Not enough points to form a polygon. Area set to 0.0")
            return 0.0
        lons, lats = zip(*[(lon, lat) for lat, lon in coords])

        # Perimeter and area calculation
        # perimeter(_) is not used but calculated for completeness
        area_m2, _ = GEOD.polygon_area_perimeter(lons, lats)
        print(f"  ✅ Raw area (sq.m): {abs(area_m2)}")
        return abs(area_m2)
    except Exception as e:
        print(f"  [ERROR] Area calculation failed: {e}")
        return 0.0

# === Main Processing ===

def main():
    try:
        print("📥 Reading input Excel file...")
        # Read the input Excel file
        df = pd.read_excel(INPUT_FILE)
        print(f"✅ Successfully read {len(df)} rows from {INPUT_FILE}")
    except Exception as e:
        print(f"[ERROR] Failed to read input Excel file: {e}")
        return

    results = []

    for idx, row in df.iterrows():
        print(f"\n📍 Processing row: {idx + 1}")
        try:
            coord_str = row.iloc[0]
            print(f"  ➡️ Raw coordinate string: {coord_str}")
            coords = ast.literal_eval(coord_str)
            if isinstance(coords[0][0], (list, tuple)):
                print("  ➡️ Detected nested list, flattening coordinates.")
                coords = coords[0]
            area_m2 = calculate_area(coords)
        except Exception as e:
            print(f"  [ROW {idx+1} ERROR] {e}")
            area_m2 = 0.0

        result_row = [coord_str]
        for unit in UNITS_TO_CALCULATE:
            converted_value = round(area_m2 * UNIT_CONVERSION[unit], ROUND_OFF_DECIMALS)
            print(f"  ➡️ Converted area: {converted_value} {unit}")
            result_row.append(converted_value)
            result_row.append(unit)

        results.append(result_row)
        print(f"  ✅ Row {idx + 1} processed and added to results.")

    # Create column headers
    columns = ["Coordinates"]
    for unit in UNITS_TO_CALCULATE:
        columns += [f"Value ({unit})", "Unit"]

    print("\n📝 Creating output DataFrame...")
    output_df = pd.DataFrame(results, columns=columns)
    print(f"✅ Output DataFrame created with {len(output_df)} rows.")

    print(f"💾 Saving results to {OUTPUT_FILE} ...")
    output_df.to_excel(OUTPUT_FILE, index=False)
    print(f"✅ Area calculation completed. Output saved to: {OUTPUT_FILE}")

# === Run the script ===
if __name__ == "__main__":
    print("🚀 Starting area calculation script...")
    main()
    print("🏁 Script execution finished.")