import json
import pandas as pd

# File paths
input_geojson = r"C:\Users\rajasekhar.palleti\Downloads\Driscoll_wrong_shapes_removed.geojson"
# Output Excel file path
output_excel = r"C:\Users\rajasekhar.palleti\Downloads\Driscoll_Bulk_geojson_output.xlsx"

# Read the GeoJSON file
with open(input_geojson, "r", encoding="utf-8") as f:
    data = json.load(f)

# Prepare a list to store extracted data
records = []

# Loop through each feature in the GeoJSON
for idx, feature in enumerate(data.get("features", []), start=1):
    print(f"Processing feature number: {idx}")
    properties = feature.get("properties", {})
    geometry = feature.get("geometry", {})

    if geometry is None:
        print(f"Skipping feature {idx} due to missing geometry.")
        continue

    # Wrap geometry into the required FeatureCollection format
    wrapped_geometry = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": {}
            }
        ]
    }
    wrapped_geometry["features"][0]["geometry"]["type"] = "MultiPolygon"

    records.append({
        "geometry": json.dumps(wrapped_geometry),  # store as JSON string
        "ranch_latitude": properties.get("ranch_latitude"),
        "ranch_longitude": properties.get("ranch_longitude"),
        "hectares": properties.get("hectares"),
        "acres": properties.get("acres"),
        "producing_area_name": properties.get("producing_area_name"),
        "region_name": properties.get("region_name"),
        "district_name": properties.get("district_name"),
        "microclimate_name": properties.get("microclimate_name"),
        "planting_date": properties.get("planting_date")
    })

# Convert to DataFrame
print("Writing data frame to dump the required data to excel file")
df = pd.DataFrame(records)

print("Data Writing to excel file")
# Save to Excel
df.to_excel(output_excel, index=False)

print(f"Data successfully saved to {output_excel}")
