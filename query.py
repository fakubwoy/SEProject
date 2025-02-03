import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

file_path = os.getenv("FILE_PATH")

output_dir = os.path.dirname(file_path)

output_path = os.path.join(output_dir, "filtered bom.xlsx")

sheet_prod = 'Prod.Ord Pdt LM'
df_prod = pd.read_excel(file_path, sheet_name=sheet_prod, engine='openpyxl')
product_numbers = df_prod["Product No."].unique()

sheet_bom = 'LM BOM'
df_bom = pd.read_excel(file_path, sheet_name=sheet_bom, engine='openpyxl')
filtered_bom = df_bom[df_bom["Product Code"].isin(product_numbers)]
filtered_bom = filtered_bom[["Product Code", "Item Code", "Quantity"]]

filtered_bom = filtered_bom.groupby(["Product Code", "Item Code"], as_index=False)["Quantity"].sum()

sheet_inventory = 'Inventory in Stock'
df_inventory = pd.read_excel(file_path, sheet_name=sheet_inventory, engine='openpyxl')

merged_df = filtered_bom.merge(df_inventory[["Item Code", "Stock On"]], on="Item Code", how="left")
merged_df.rename(columns={"Stock On": "Stock Available"}, inplace=True)

sheet_qc_stock = 'QC Stock'
df_qc_stock = pd.read_excel(file_path, sheet_name=sheet_qc_stock, engine='openpyxl')

merged_df = merged_df.merge(df_qc_stock[["Item Code", "Stock On"]], on="Item Code", how="left")
merged_df.rename(columns={"Stock On": "Stock with QC"}, inplace=True)

sheet_job_work = 'Job Work Stock'
sheet_names = pd.ExcelFile(file_path).sheet_names
job_work_sheet = next((s for s in sheet_names if s.strip().lower() == "job work stock".lower()), None)

if job_work_sheet:
    df_job_work = pd.read_excel(file_path, sheet_name=job_work_sheet, engine='openpyxl')
    merged_df = merged_df.merge(df_job_work[["Item Code", "Stock On"]], on="Item Code", how="left")
    merged_df.rename(columns={"Stock On": "Vendor Stock"}, inplace=True)

merged_df.fillna(0, inplace=True)

merged_df["Total Stock Available"] = merged_df["Stock Available"] + merged_df["Stock with QC"] + merged_df["Vendor Stock"]

merged_df["Shortage/Excess"] = merged_df["Quantity"] - merged_df["Total Stock Available"]

sheet_pending_po = 'Pending PO'
df_pending_po = pd.read_excel(file_path, sheet_name=sheet_pending_po, engine='openpyxl')

df_pending_po_grouped = df_pending_po.groupby("Item No.", as_index=False)["Open PO Qty"].sum()

merged_df = merged_df.merge(df_pending_po_grouped[["Item No.", "Open PO Qty"]], left_on="Item Code", right_on="Item No.", how="left")

merged_df["Open PO Qty"] = merged_df["Open PO Qty"].fillna(0)

merged_df["Adjusted Shortage/Excess"] = merged_df["Shortage/Excess"] - merged_df["Open PO Qty"]

merged_df = merged_df.groupby(["Product Code", "Item Code"], as_index=False).agg({
    "Quantity": "sum",
    "Stock Available": "sum",
    "Stock with QC": "sum",
    "Vendor Stock": "sum",
    "Total Stock Available": "sum",
    "Shortage/Excess": "sum",
    "Open PO Qty": "sum",
    "Adjusted Shortage/Excess": "sum"
})

merged_df.to_excel(output_path, index=False)

print(f"Final BOM with adjusted stock saved to {output_path}")
