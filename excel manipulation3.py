import pandas as pd

# Load your Excel file
input_excel = r"C:\Users\rajasekhar.palleti\Downloads\Driscoll_Bulk_output1.xlsx"
sheet_name = "Sheet1"   # change if needed
df = pd.read_excel(input_excel, sheet_name=sheet_name)

# Assuming:
# Column 0 = Transaction_ranch_Number
# Column 1 = object ids
grouped = df.groupby(df.columns[0])[df.columns[1]].agg(
    lambda x: ",".join(map(str, x))
).reset_index()

# Add count column
grouped["Count"] = grouped[df.columns[1]].apply(lambda x: len(x.split(",")))

# Rename columns
grouped.columns = ["Transaction_ranch_Number", "object ids as comma separated", "Count"]

# Save to Excel
output_excel = r"C:\Users\rajasekhar.palleti\Downloads\Driscoll_Bulk_output3.xlsx"
grouped.to_excel(output_excel, index=False)

print("✅ Done! Grouped data saved to:", output_excel)
