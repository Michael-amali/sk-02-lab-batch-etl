from pathlib import Path
import uuid
import numpy as np
import pandas as pd

# Reproducible random data
np.random.seed(42)

n = 100

# Generate hourly timestamps
ordered_at = pd.date_range(
    start="2026-07-01 00:00:00",
    periods=n,
    freq="h"
)

df = pd.DataFrame({
    "order_id": [f"ORD-{uuid.uuid4().hex[:8].upper()}" for _ in range(n)],
    "store_id": np.random.randint(1, 11, n),         # Stores 1-10
    "product_id": np.random.randint(100, 151, n),    # Products 100-150
    "quantity": np.random.randint(1, 6, n),
    "unit_price": np.round(np.random.uniform(10, 500, n), 2),
    "ordered_at": ordered_at
})

# Derived fields
df["line_amount"] = (
    df["quantity"] * df["unit_price"]
).round(2)

df["order_date"] = df["ordered_at"].dt.date

# Write to Parquet
output_dir = Path("output")
output_dir.mkdir(parents=True, exist_ok=True)

output_file = output_dir / "sales.parquet"

df.to_parquet(
    output_file,
    engine="pyarrow",
    index=False
)

print(f"Wrote {len(df):,} sales records to {output_file}")
print(df.head())