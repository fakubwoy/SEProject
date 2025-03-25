import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

file_path = os.getenv("FILE_PATH") or "test_data.xlsx"
output_dir = os.path.dirname(file_path)
output_path = os.path.join(output_dir, "output.xlsx")

def log(msg): print(f"[INFO] {msg}")
def warn(msg): print(f"[WARN] {msg}")
def err(msg): print(f"[ERROR] {msg}")

try:
    df_prod_lm = pd.read_excel(file_path, sheet_name='Prod.Ord Pdt LM', engine='openpyxl')
    product_numbers = df_prod_lm["Product No."].dropna().unique()

    if df_prod_lm["Rate"].isnull().any():
        warn("Missing rates detected in 'Prod.Ord Pdt LM'.")

    df_bom_lm = pd.read_excel(file_path, sheet_name='LM BOM', engine='openpyxl')
    invalid_products = set(df_bom_lm["Product Code"].unique()) - set(product_numbers)
    if invalid_products:
        warn(f"BOM has product codes not in Production Order: {invalid_products}")

    filtered_bom = df_bom_lm[df_bom_lm["Product Code"].isin(product_numbers)]
    filtered_bom = filtered_bom[["Product Code", "Item Code", "Quantity", "UoM Name"]]
    filtered_bom = filtered_bom.groupby(["Product Code", "Item Code", "UoM Name"], as_index=False)["Quantity"].sum()

    df_inventory = pd.read_excel(file_path, sheet_name='Inventory in Stock', engine='openpyxl')
    merged_df = filtered_bom.merge(df_inventory[["Item Code", "Stock On"]], on="Item Code", how="left")
    missing_inventory = merged_df["Stock On"].isnull()
    if missing_inventory.any():
        warn(f"Missing inventory data for items: {merged_df[missing_inventory]['Item Code'].tolist()}")
    merged_df = merged_df.rename(columns={"Stock On": "Inventory in Stores"})

    df_qc_stock = pd.read_excel(file_path, sheet_name='QC Stock', engine='openpyxl')
    merged_df = merged_df.merge(df_qc_stock[["Item Code", "Stock On"]], on="Item Code", how="left")
    merged_df = merged_df.rename(columns={"Stock On": "Inventory with Quality"})

    sheet_names = pd.ExcelFile(file_path).sheet_names
    if "Job Work Stock" in sheet_names:
        df_job_work = pd.read_excel(file_path, sheet_name="Job Work Stock", engine='openpyxl')
        merged_df = merged_df.merge(df_job_work[["Item Code", "Stock On"]], on="Item Code", how="left")
        merged_df = merged_df.rename(columns={"Stock On": "Inventory with Vendors outside"})
    else:
        merged_df["Inventory with Vendors outside"] = 0

    merged_df = merged_df.fillna(0)

    if (merged_df[["Inventory in Stores", "Inventory with Quality", "Inventory with Vendors outside"]] < 0).any().any():
        warn("Negative stock values found in one or more stock columns.")

    merged_df["Total Stock Available"] = (
        merged_df["Inventory in Stores"] + 
        merged_df["Inventory with Quality"] + 
        merged_df["Inventory with Vendors outside"]
    )
    merged_df["Net Inventory required"] = merged_df["Quantity"] - merged_df["Total Stock Available"]

    df_pending_po = pd.read_excel(file_path, sheet_name="Pending PO", engine='openpyxl')
    df_pending_po_grouped = df_pending_po.groupby("Item No.", as_index=False)["Open PO Qty"].sum()
    unmatched_pos = set(df_pending_po_grouped["Item No."]) - set(merged_df["Item Code"])
    if unmatched_pos:
        warn(f"Pending PO has Item No. not in BOM: {unmatched_pos}")

    merged_df = merged_df.merge(
        df_pending_po_grouped[["Item No.", "Open PO Qty"]], 
        left_on="Item Code", 
        right_on="Item No.", 
        how="left"
    )
    merged_df["Open PO Qty"] = merged_df["Open PO Qty"].fillna(0)  

    merged_df["Purchase order to be raised"] = merged_df["Net Inventory required"] - merged_df["Open PO Qty"]

    df_rates = df_prod_lm[["Product No.", "Rate"]].drop_duplicates()
    merged_df = merged_df.merge(df_rates, left_on="Product Code", right_on="Product No.", how="left")

    if merged_df["Rate"].isnull().any():
        warn("Some items have missing rates and will have Amount = 0.")

    merged_df["Amount"] = merged_df["Quantity"] * merged_df["Rate"]
    merged_df["Amount"] = merged_df["Amount"].fillna(0) 
    merged_df["Comments"] = ""

    merged_df = merged_df.rename(columns={
        "Product Code": "Inventory Code",
        "Item Code": "Inventory Name",
        "UoM Name": "UOM",
        "Open PO Qty": "Purchase order in pipeline",
        "Rate": "₹/UOM",
        "Amount": "₹"
    })

    merged_df.insert(0, "Sr No", range(1, len(merged_df) + 1))

    column_order = [
        "Sr No", "Inventory Code", "Inventory Name", "UOM",
        "Inventory in Stores", "Inventory with Quality", "Inventory with Vendors outside",
        "Net Inventory required", "Purchase order in pipeline", "Purchase order to be raised",
        "₹/UOM", "₹", "Comments"
    ]

    merged_df = merged_df[column_order]
    merged_df.to_excel(output_path, index=False)
    log(f"Final output saved at: {output_path}")

except Exception as e:
    err(f"Script failed: {e}")