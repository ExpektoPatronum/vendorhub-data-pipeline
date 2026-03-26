import pandas as pd

def normalize_vendor_a(df: pd.DataFrame) -> pd.DataFrame:

    return pd.DataFrame({
        "vendor_sku": df.get("vendor_sku"),  #если колонки какой-либо нет то она создастся со всеми значениями NAN
        "title": df.get("title"),
        "category": df.get("category"),
        "price": df.get("price"),
        "currency": df.get("currency"),
    })