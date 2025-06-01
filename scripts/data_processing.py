import pandas as pd
import os
from glob import glob

DATA_DIR = "data"
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

COLUMN_MAP = {
    "Lần cuối": "last",
    "Mở": "open",
    "Cao": "high",
    "Thấp": "low",
    "KL": "volume",
    "% Thay đổi": "change_percent"
}

def clean_numeric_column(series):
    return (
        series.astype(str)
              .str.replace("K", "e3", regex=False)
              .str.replace("M", "e6", regex=False)
              .str.replace(",", "", regex=False)
              .str.replace("%", "", regex=False)
              .replace("nan", None)
              .apply(pd.to_numeric, errors="coerce")
    )

def process_group(file_pattern, prefix):
    files = glob(os.path.join(DATA_DIR, file_pattern))
    df_list = []

    for file in files:
        try:
            df = pd.read_csv(file)
            df = df.rename(columns={"Ngày": "date", **COLUMN_MAP})
            df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
            df = df[["date"] + list(COLUMN_MAP.values())].dropna(subset=["date"])

            for col in COLUMN_MAP.values():
                df[col] = clean_numeric_column(df[col])

            df = df.rename(columns={col: f"{prefix}_{col}" for col in COLUMN_MAP.values()})
            df_list.append(df)

        except Exception as e:
            print(e)

    if not df_list:        
        return
    
    merged = pd.concat(df_list, ignore_index=True)
    merged = merged.drop_duplicates(subset="date")
    merged = merged.sort_values("date").reset_index(drop=True)
    
    full_dates = pd.date_range(start=merged["date"].min(), end=merged["date"].max(), freq="D")
    merged = merged.set_index("date").reindex(full_dates).rename_axis("date").reset_index()
    
    merged = merged.ffill()
    
    merged = merged[merged["date"] >= pd.Timestamp("1993-01-01")]
    
    missing_ratio = merged.isnull().mean()
    keep_cols = [col for col in merged.columns if missing_ratio[col] < 0.5 or col == "date"]
    merged = merged[keep_cols]
    
    out_path = os.path.join(OUTPUT_DIR, f"{prefix}_cleaned.csv")
    merged.to_csv(out_path, index=False)
        
process_group("oil_price*.csv", "oil")
process_group("gold_price*.csv", "gold")
process_group("sp500*.csv", "sp500")
process_group("dxy*.csv", "dxy")
