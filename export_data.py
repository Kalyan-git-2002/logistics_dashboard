import pandas as pd
import s3fs,os

try:
    fs = s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY"),
        secret=os.getenv("AWS_SECRET_KEY"))
    
    bucket_1 = "clg-demo-2026/powerbi/fact/"
    bucket_2 = "clg-demo-2026/powerbi/dim/dim_date/"
    df_1 = pd.read_parquet(bucket_1,filesystem=fs,filters=None)
    df_2 = pd.read_parquet(bucket_2,filesystem=fs,filters=None)
    
    df_1.to_csv("fact.csv",index=False)
    df_2.to_csv("dim_date.csv",index=False)
except Exception as  e:
    print(f"ERROR : {e}")
    exit 1
    
