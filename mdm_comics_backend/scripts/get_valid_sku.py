import pandas as pd
import sys

catalog_path = r"F:\apps\mdm_comics\assets\docs\implementations\bcw_implementation\20251216_mdm_comics_bcw_catalog.xlsx"

try:
    df = pd.read_excel(catalog_path, header=1)
    # Get the first valid BCW-SKU
    valid_sku = df['BCW-SKU'].dropna().iloc[0]
    print(f"VALID_SKU:{valid_sku}")
except Exception as e:
    print(f"ERROR:{e}")
