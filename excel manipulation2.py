import pandas as pd

# Input file
input_file = r"C:\Users\rajasekhar.palleti\Downloads\Driscoll_Bulk_output1.xlsx"
output_file = r"C:\Users\rajasekhar.palleti\Downloads\Driscoll_Bulk_output3.xlsx"

# Read the Excel file (first sheet by default)
df = pd.read_excel(input_file, sheet_name="Sheet1")

# Assuming Column 2 is 'transactional_ranch_number'
# If your sheet doesn’t have headers, set header=None in read_excel
col_name = df.columns[1]   # second column

# Drop duplicates based on transactional_ranch_number, keeping the first row
df_unique = df.drop_duplicates(subset=[col_name], keep="first")

# Save to new Excel file
df_unique.to_excel(output_file, index=False)

print(f"✅ Unique transactional_ranch_number rows saved to: {output_file}")
