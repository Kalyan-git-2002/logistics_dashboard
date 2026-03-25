from deltalake import DeltaTable
import pandas as pd
import os

storage_options = {
    "AWS_ACCESS_KEY_ID"     : os.environ["AWS_ACCESS_KEY"],
    "AWS_SECRET_ACCESS_KEY" : os.environ["AWS_SECRET_KEY"],
    "AWS_REGION"            : "us-east-1"
}

BUCKET = "s3://clg-demo-2026"

# Read Gold Delta tables
print("Reading fact_deliveries...")
fact_df = DeltaTable(
    f"{BUCKET}/gold/fact/",
    storage_options = storage_options
).to_pandas()

print("Reading dim_date...")
dim_df = DeltaTable(
    f"{BUCKET}/gold/dim/dim_date/",
    storage_options = storage_options
).to_pandas()

# Save CSV
fact_df.to_csv("fact_deliveries.csv", index=False)
dim_df.to_csv("dim_date.csv", index=False)

print(f"✅ fact → {len(fact_df)} rows")
print(f"✅ dim  → {len(dim_df)} rows")
