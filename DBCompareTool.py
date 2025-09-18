import sqlite3
import pandas as pd

def get_tables(conn):
    """Get all table names in the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [row[0] for row in cursor.fetchall()]

def get_columns(conn, table):
    """Get all column names of a table."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table});")
    return [(row[1], row[5]) for row in cursor.fetchall()]  # (col_name, pk_flag)

def compare_databases(main_db, other_db, label):
    results = {"tables": [], "columns": [], "rows": []}

    conn1 = sqlite3.connect(main_db)
    conn2 = sqlite3.connect(other_db)

    tables1 = set(get_tables(conn1))
    tables2 = set(get_tables(conn2))

    # Compare table names
    only_in_main = tables1 - tables2
    only_in_other = tables2 - tables1
    for t in only_in_main:
        results["tables"].append(["Only in MAIN", t])
    for t in only_in_other:
        results["tables"].append([f"Only in {label}", t])

    # Compare common tables
    common_tables = tables1 & tables2
    for table in common_tables:
        cols1 = dict(get_columns(conn1, table))
        cols2 = dict(get_columns(conn2, table))

        only_in_cols1 = set(cols1) - set(cols2)
        only_in_cols2 = set(cols2) - set(cols1)
        for c in only_in_cols1:
            results["columns"].append([table, "Only in MAIN", c])
        for c in only_in_cols2:
            results["columns"].append([table, f"Only in {label}", c])

        # Compare row data only for common columns
        common_cols = set(cols1) & set(cols2)
        if common_cols:
            common_cols_list = ",".join([f'"{c}"' for c in common_cols])

            cursor1 = conn1.cursor()
            cursor2 = conn2.cursor()

            cursor1.execute(f'SELECT {common_cols_list} FROM "{table}"')
            cursor2.execute(f'SELECT {common_cols_list} FROM "{table}"')

            rows1 = cursor1.fetchall()
            rows2 = cursor2.fetchall()

            # Use primary key if available
            pk_cols = [col for col, pk in cols1.items() if pk == 1 and col in common_cols]

            if pk_cols:  # Use PK for row matching
                pk_index = [list(common_cols).index(pk) for pk in pk_cols]

                dict1 = {tuple(row[i] for i in pk_index): row for row in rows1}
                dict2 = {tuple(row[i] for i in pk_index): row for row in rows2}

                for pk, row in dict1.items():
                    if pk not in dict2:
                        results["rows"].append([table, "Only in MAIN", str(row)])
                    elif row != dict2[pk]:
                        results["rows"].append([table, "UPDATED", f"MAIN={row} | {label}={dict2[pk]}"])

                for pk, row in dict2.items():
                    if pk not in dict1:
                        results["rows"].append([table, f"Only in {label}", str(row)])

            else:  # No PK, fallback to set comparison
                set1 = set(rows1)
                set2 = set(rows2)

                only_in_rows1 = set1 - set2
                only_in_rows2 = set2 - set1

                for r in only_in_rows1:
                    results["rows"].append([table, "Only in MAIN", str(r)])
                for r in only_in_rows2:
                    results["rows"].append([table, f"Only in {label}", str(r)])

    conn1.close()
    conn2.close()
    return results


if __name__ == "__main__":
    # Input DB paths
    main_db = r"C:\Users\rajasekhar.palleti\Downloads\smartFarmDBfalse1.sqlite"
    db2 = r"C:\Users\rajasekhar.palleti\Downloads\smartFarmDBtrue11.sqlite"
    # db3 = r"C:\path\to\third.sqlite"

    # Compare
    results_db2 = compare_databases(main_db, db2, "DB2")
    # results_db3 = compare_databases(main_db, db3, "DB3")

    # Save to Excel
    output_file = r"C:\Users\rajasekhar.palleti\Downloads\sqlite_comparison_results11.xlsx"
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        # DB2 Results
        pd.DataFrame(results_db2["tables"], columns=["Location", "Table"]).to_excel(writer, sheet_name="Tables_DB2", index=False)
        pd.DataFrame(results_db2["columns"], columns=["Table", "Location", "Column"]).to_excel(writer, sheet_name="Columns_DB2", index=False)
        pd.DataFrame(results_db2["rows"], columns=["Table", "ChangeType", "Details"]).to_excel(writer, sheet_name="Rows_DB2", index=False)

        # # DB3 Results
        # pd.DataFrame(results_db3["tables"], columns=["Location", "Table"]).to_excel(writer, sheet_name="Tables_DB3", index=False)
        # pd.DataFrame(results_db3["columns"], columns=["Table", "Location", "Column"]).to_excel(writer, sheet_name="Columns_DB3", index=False)
        # pd.DataFrame(results_db3["rows"], columns=["Table", "ChangeType", "Details"]).to_excel(writer, sheet_name="Rows_DB3", index=False)

    print(f"\n✅ Comparison completed. Results saved to {output_file}")
