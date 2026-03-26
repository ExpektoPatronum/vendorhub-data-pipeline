import pandas as pd

def validate_unified(df: pd.DataFrame):
    df = df.copy() # работаем с копией, чтобы не менять исходный DataFrame

    df["raw_id"] = pd.to_numeric(df["raw_id"], errors="coerce")  #безопасно конвертит в float64, если мусор то в NaN
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["vendor_sku"] = df["vendor_sku"].astype("string").str.strip()
    df["title"] = df["title"].astype("string").str.strip()
    df["currency"] = df["currency"].astype("string").str.strip()

    df["error_reason"] = [[] for _ in range(len(df))]
    raw_id_mask = df['raw_id'].isna()
    vendor_sku_mask = df['vendor_sku'].isna() | (df['vendor_sku'] == '')
    title_mask = df['title'].isna() | (df['title'] == '')
    price_isna_mask = df['price'].isna()
    non_positive_price_mask = df['price'] <= 0
    currency_mask = df['currency'].isna() | (df['currency'] == '')

    df.loc[raw_id_mask, 'error_reason'] = (
        df.loc[raw_id_mask, 'error_reason'].apply(lambda x: x + ['invalid_raw_id'])
    )
    df.loc[vendor_sku_mask, 'error_reason'] = (
        df.loc[vendor_sku_mask, 'error_reason'].apply(lambda x: x + ['missing_vendor_sku'])
    )
    df.loc[title_mask, 'error_reason'] = (
        df.loc[title_mask, 'error_reason'].apply(lambda x: x + ['missing_title'])
    )
    df.loc[price_isna_mask, 'error_reason'] = (
        df.loc[price_isna_mask, 'error_reason'].apply(lambda x: x + ['missing_price'])
    )
    df.loc[non_positive_price_mask, 'error_reason'] = (
        df.loc[non_positive_price_mask, 'error_reason'].apply(lambda x: x + ['non_positive_price'])
    )
    df.loc[currency_mask, 'error_reason'] = (
        df.loc[currency_mask, 'error_reason'].apply(lambda x: x + ['missing_currency'])
    )

    # делим dataframe
    invalid_df = df[df["error_reason"].apply(len) > 0].copy()
    valid_df = df[df["error_reason"].apply(len) == 0].copy()

    # убираем колонку ошибки из валидных
    valid_df = valid_df.drop(columns=["error_reason"])

    valid_df = valid_df.reset_index(drop=True)
    invalid_df = invalid_df.reset_index(drop=True)

    valid_df["raw_id"] = valid_df["raw_id"].astype("int64")
    invalid_df["raw_id"] = invalid_df["raw_id"].astype("int64")

    return valid_df, invalid_df