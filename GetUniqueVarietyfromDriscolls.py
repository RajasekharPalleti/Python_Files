import json
import pandas as pd

# File paths
input_file = r"C:\Users\rajasekhar.palleti\Downloads\Driscoll_wrong_shapes_removed.geojson"
# Output Excel file path
output_excel = r"C:\Users\rajasekhar.palleti\Downloads\Driscolls unique varieties and its count.xlsx"

# Load JSON
with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# Extract varietyName and varietyCode
records = []
for feature in data.get("features", []):
    props = feature.get("properties", {})
    variety_name = props.get("variety_name", None)
    variety_code = props.get("variety_code", None)

    if variety_name and variety_code:
        records.append({
            "varietyName": variety_name,
            "varietyCode": variety_code
        })

# Convert to DataFrame
df = pd.DataFrame(records)

# Group by varietyName and varietyCode to get counts
df_grouped = (
    df.groupby(["varietyName", "varietyCode"])
    .size()
    .reset_index(name="count")
    .sort_values(by="count", ascending=False)
)

# Save to Excel
df_grouped.to_excel(output_excel, index=False)

print(f"✅ Extracted {len(df_grouped)} unique variety entries.")
print(f"📄 File saved to: {output_excel}")
