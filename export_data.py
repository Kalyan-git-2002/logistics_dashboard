import pandas as pd
import os
from deltalake import DeltaTable

try:
    storage_options = {}
    
    bucket_1 = "s3://clg-demo-2026/gold/fact/"
    bucket_2 = "s3://clg-demo-2026/gold/dim/dim_date/"

    dt_1 = DeltaTable(bucket_1,storage_options = storage_options)
    dt_2 = DeltaTable(bucket_2,storage_options = storage_options)
    
    df_1 = dt_1.to_pandas()
    df_2 = dt_2.to_pandas()
    
    df_1.to_csv("fact.csv",index=False)
    df_2.to_csv("dim_date.csv",index=False)
except Exception as  e:
    print(f"ERROR : {str(e)}")
    raise
