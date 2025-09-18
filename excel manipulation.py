import pandas as pd

# Read from Sheet2
input_excel = r"C:\Users\rajasekhar.palleti\Downloads\Driscoll_Bulk_output.xlsx"
df = pd.read_excel(input_excel, sheet_name="Sheet2")

# Group by transactional_ranch_number and aggregate objectid
result = df.groupby("grower_number").agg({
    "transactional_ranch_number": [
        lambda x: ",".join(map(str, sorted(set(x)))),  # all unique IDs as comma-separated string
        lambda x: x.nunique()  # distinct count of IDs
    ]
}).reset_index()

# Rename columns
result.columns = ["grower_number", "transactional_ranch_numbers", "count"]

# Write result into the same file, new sheet (Sheet3)
with pd.ExcelWriter(input_excel, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
    result.to_excel(writer, sheet_name="Organized", index=False)

print("✅ Done! Results added to Organized in input.xlsx")
