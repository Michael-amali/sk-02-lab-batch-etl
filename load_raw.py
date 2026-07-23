import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://dwh_user:dwh_pass@localhost:5434/warehouse"
)
df = pd.read_parquet("output/sales/")          # your Lab 05 partitioned output
df.to_sql("sales", engine, schema="raw", if_exists="replace", index=False)
print(f"Loaded {len(df):,} rows into raw.sales")