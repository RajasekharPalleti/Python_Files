import pandas as pd

# File paths
input_file = r"C:\Users\rajasekhar.palleti\Downloads\cangucu_tobacco_plots.geojson_output.xlsx"       # change to your input file
output_file = r"C:\Users\rajasekhar.palleti\Downloads\cangucu_tobacco_plots.geojson_output1.xlsx"     # output file name
column_name = "pk"             # column name in your Excel containing the IDs

# Read Excel
df = pd.read_excel(input_file)

# Split the IDS column
df[['year', 'code', 'suffix']] = df[column_name].str.split('_', expand=True)

# Create required columns
df['farmer_name'] = df['year'] + "_" + df['code']
df['farmer_code'] = df['code']
df['mobile_number'] = df['year'] + df['code']

# Drop helper columns
df = df.drop(columns=['year', 'code', 'suffix'])

# Save to Excel
df.to_excel(output_file, index=False)

print("✅ Processing completed. File saved as:", output_file)
