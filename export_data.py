import polars as pl
import os

storage_options = {
    "AWS_ACCESS_KEY_ID"     : os.environ["AWS_ACCESS_KEY"],
    "AWS_SECRET_ACCESS_KEY" : os.environ["AWS_SECRET_KEY"],
    "AWS_REGION"            : "us-east-1"
}

BUCKET = "s3://clg-demo-2026"

# Read Gold Delta tables
print("Reading fact_deliveries...")
fact_df = pl.read_delta(
    f"{BUCKET}/powerbi/fact",
    storage_options = storage_options
)

print("Reading dim_date...")
dim_df = pl.read_delta(
    f"{BUCKET}/powerbi/dim/dim_date/",
    storage_options = storage_options
)

# Save CSV
fact_df.write_csv("fact_deliveries.csv")
dim_df.write_csv("dim_date.csv")

print(f"✅ fact → {len(fact_df)} rows")
print(f"✅ dim  → {len(dim_df)} rows")
