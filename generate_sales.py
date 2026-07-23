from pathlib import Path
import numpy as np
import pandas as pd

# Reproducible random data
np.random.seed(42)

n = 100

df = pd.DataFrame({
    "sale_id": range(1, n + 1),
    "customer_id": np.random.randint(1000, 1100, n),
    "product_id": np.random.randint(1, 51, n),
    "quantity": np.random.randint(1, 6, n),
    "unit_price": np.round(np.random.uniform(10, 500, n), 2),
    "sale_date": pd.date_range("2026-07-01", periods=n, freq="h")
})

df["total_amount"] = (
    df["quantity"] * df["unit_price"]
).round(2)

output_dir = Path("output")
output_dir.mkdir(parents=True, exist_ok=True)

output_file = output_dir / "sales.parquet"

df.to_parquet(
    output_file,
    engine="pyarrow",
    index=False
)

print(f"Wrote {len(df):,} sales records to {output_dir}")