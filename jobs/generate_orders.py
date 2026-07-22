"""Lab 01: generate one day of UrbanGear order CSVs (deterministic per date)."""
import csv, io, random, sys
import boto3

date = sys.argv[1] if len(sys.argv) > 1 else "2026-07-01"
random.seed(date)                      # same date -> same data, always (reproducible!)

PRODUCTS = [("P100", "Trail Backpack", 89.99), ("P200", "Camp Stove", 45.50),
            ("P300", "Headlamp", 19.99), ("P400", "Tent 2P", 199.00)]

rows = []
for i in range(500):
    pid, name, price = random.choice(PRODUCTS)
    qty = random.randint(1, 5)
    rows.append({
        "order_id": f"{date.replace('-','')}-{i:05d}",
        "order_ts": f"{date}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
        "product_id": pid, "product_name": name,
        "quantity": qty, "unit_price": price,
        "amount": round(qty * price, 2),
        "customer_email": f"user{random.randint(1,200)}@example.com",
    })

# --- Lab 03: inject realistic dirt (about 4% of rows) ---
for i, r in enumerate(rows):
    if i % 50 == 0:  r["amount"] = ""                        # missing amount
    if i % 61 == 0:  r["quantity"] = -2                      # impossible quantity
    if i % 73 == 0:  r["customer_email"] = "not-an-email"    # bad email
rows.append(dict(rows[10]))                                  # duplicate order row


buf = io.StringIO()
writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
writer.writeheader()
writer.writerows(rows)

s3 = boto3.client("s3", endpoint_url="http://minio:9000",
                  aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin123")
key = f"raw/orders/date={date}/orders_{date.replace('-','')}_0300.csv"
s3.put_object(Bucket="urbangear-raw", Key=key, Body=buf.getvalue().encode("utf-8"))
print(f"uploaded s3://urbangear-raw/{key} ({len(rows)} rows)")