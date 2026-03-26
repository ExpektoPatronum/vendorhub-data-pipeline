import pandas as pd

def normalize_vendor_b(df: pd.DataFrame) -> pd.DataFrame:

    return pd.DataFrame({
        "vendor_sku": df.get("sku"),
        "title": df.get("name"),
        "category": df.get("category"),
        "price": df.get("price_usd"),
        "currency": df.get("currency"),
    })


