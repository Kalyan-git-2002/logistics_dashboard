# Databricks notebook source
import os
import boto3
import pandas as pd
from pyspark.errors import AnalysisException
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T

print("imported all requried modules ✅")


# COMMAND ----------

from datetime import date
dbutils.widgets.text("AWS_ACCESS_KEY_ID","","access_key")
dbutils.widgets.text("AWS_SECRET_ACCESS_KEY","","secret_key")
dbutils.widgets.text("bucket_name","","bucket_name")
dbutils.widgets.text("region", "us-east-1","region")
dbutils.widgets.text("today",str(date.today()),"today")

# COMMAND ----------

access_key = dbutils.widgets.get("AWS_ACCESS_KEY_ID")
secret_key = dbutils.widgets.get("AWS_SECRET_ACCESS_KEY")
bucket_name = dbutils.widgets.get("bucket_name")
region = dbutils.widgets.get("region")
today = dbutils.widgets.get("today")

assert access_key != "" ,"❌ AWS_ACCESS_KEY_ID is empty"
assert secret_key != "" ,"❌ AWS_SECRET_ACCESS_KEY is empty"
assert bucket_name != "" ,"❌ bucket_name is empty"
assert region != "" ,"❌ region is empty"
#assert today != "" ,"❌ today is empty"

print("Creditials fetched ✅")
print(f"processing data for {today}")

# COMMAND ----------

# bucket key level connection to s3 bucket...
S3_client = boto3.client('s3',
                         aws_access_key_id=access_key,
                         aws_secret_access_key=secret_key,
                         region_name=region)
try:
    S3_client.head_bucket(Bucket=bucket_name)
    print(f"boto3 client created for {bucket_name} ✅")
except Exception as e:
    raise Exception(f"Error: {e}")

# COMMAND ----------

# Folders requried .....
folders = ["raw/shipments/",
           "silver/",
           "gold/fact/",
           "gold/dim/dim_date/",
           "checkpoints/runs/"]
for folder in folders:
    S3_client.put_object(Bucket=bucket_name,Key=folder)
    print(f"folder {folder} created ✅")

# COMMAND ----------

# Check point functions and saving...
def get_last_checkpoint(bucket_name,key = 'checkpoints/last_checkpoint.txt'):
    try:
        obj = S3_client.get_object(Bucket=bucket_name,Key=key)
        return obj['Body'].read().decode('utf-8')
    except Exception as e:
        return e
def save_checkpoint(bucket_name,today,key = 'checkpoints/last_checkpoint.txt'):
                        S3_client.put_object(Bucket=bucket_name,Key=key,Body=today.encode('utf-8'))
                        print(f"checkpoint saved for {today} ✅")
                        S3_client.put_object(Bucket=bucket_name,Key=f'checkpoints/runs/{today}.txt',Body=today.encode('utf-8'))
                        print(f"{today} run Saved ✅")
def list_all_runs(bucket_name,key = 'checkpoints/runs/'):
    try:
        response = S3_client.list_objects_v2(Bucket=bucket_name,Prefix=key)
        if "Contents" in response:
            print(f"printing all runs upto {today}...")
            for obj in response['Contents']:
                print(f"Runs : {obj['Key']} ")
        else:
            print("No runs found")
    except Exception as e:
        raise Exception(f"Error: {e}")
last_run = get_last_checkpoint(bucket_name)
print(f"last run was {last_run if last_run else 'First_run_ever'}")
list_all_runs(bucket_name)

# COMMAND ----------

# bronze layer (import data from s3 bucket)
if last_run == today:
    print(f"Data already processed for {today}...❌")
    dbutils.notebook.exit(f"Alredy processed for {today}")
try:
    bronze_df = spark.read.format("parquet").load(f"s3a://clg-demo-2026/raw/shipments/{today}.parquet")
    print(f"Bronze Loaded : {bronze_df.count()} records ✅ ")
    display(bronze_df.limit(5))

except FileNotFoundError:
    print(f"No data found for {today} ❌")
except AnalysisException :
    print(f"No data found for {today} ❌")
except Exception as e:
    print(f"Unexpected error occurred while loading data for {today} ❌")

    print(f"ERROR : {e} ❌")

# COMMAND ----------

# Creating silver layer (transform data).......

silver_path = f"s3a://{bucket_name}/silver/"

silver_df = bronze_df\
            .drop("DRS No")\
            .drop("Location")\
            .drop("Created Date")\
            .drop("DRS Date")\
            .drop("DRS status")\
            .drop("Customer Name")\
            .drop("Shipper")\
            .drop("City")\
            .drop("Volumentric weight")\
            .drop("is_otp_verified")\
            .drop("Payment Transaction ID")\
            .drop("DEL from App")\
            .drop("Received by")\
            .drop("DRS Type")\
            .drop("scan time")\
            .withColumn("Employee_Name",F.lower(F.trim(F.col("Employee Name"))).cast(T.StringType()))\
            .withColumn("Employee_Number",F.col("Employee Number").cast(T.LongType()))\
            .withColumn("Waybill_No",F.col("Waybill No").cast(T.StringType()))\
            .withColumn("Attempt_Count",F.col("Attempt Count").cast(T.LongType()))\
            .withColumn("First_attempt",F.to_date(F.col("First attempt"),"d-M-yyyy, HH:mm"))\
            .withColumn("Consignee_Name",F.lower(F.trim(F.col("Consignee_Name"))).cast(T.StringType()))\
            .withColumn("Pincode",F.col("Pincode").cast(T.IntegerType()))\
            .withColumn("Amount_payable",F.regexp_replace(F.lower(F.trim(F.col("Amount payable"))),",","").cast(T.IntegerType()))\
            .withColumn("Status",F.lower(F.trim(F.col("Status"))).cast(T.StringType()))\
            .withColumn("Sub_status",F.lower(F.trim(F.col("Sub status"))).cast(T.StringType()))\
            .withColumn("POD_Date",F.to_date(F.col("POD Date"),"d-M-yyyy, HH:mm"))\
            .withColumn("Delivery_Payment_type",F.lower(F.trim(F.col("Delivery Payment type"))).cast(T.StringType()))\
            .withColumn("Pincode",F.col("Pincode").cast(T.StringType()))\
            .withColumn("RunDate",F.lit(today))\
            .select("Employee_Name","Employee_Number","Waybill_No","Attempt_Count","First_attempt","Consignee_Name","Pincode","Amount_payable","Status","Sub_status","POD_Date","Delivery_Payment_type","RunDate")

# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(f"{silver_path}{today}")
print(f"Silver written to {silver_path} ✅")
print(f"silver layer created for {today}-{silver_df.count()} rows uploaded")


# COMMAND ----------

silver_df.columns

# COMMAND ----------

# Gold fact_table ......

gold_fact_path = f"s3a://{bucket_name}/gold/fact/"

gold_fact_columns = ['Employee_Name',
 'Employee_Number',
 'Waybill_No',
 'Attempt_Count',
 'First_attempt',
 'Consignee_Name',
 'Pincode',
 'Amount_payable',
 'Status',
 'Sub_status',
 'POD_Date',
 'Delivery_Payment_type',
 'RunDate']
#gold table created.......

gold_fact_df = silver_df.select(gold_fact_columns)

# idempotency check
if DeltaTable.isDeltaTable(spark,gold_fact_path):
    dt = DeltaTable.forPath(spark,gold_fact_path)
    dt.delete(F.col("RunDate") == today)
    print(f"deleting records for {today} from gold table ✅")
else:
    print(f"creating gold table for {today} ✅")

gold_fact_df.write.format("delta").mode("append").save(gold_fact_path)
print(f"gold table created for {today} ✅")

# COMMAND ----------

display(gold_fact_df.limit(5))

# COMMAND ----------

# dim_date table creating ......
gold_df = spark.read.format("delta").load(f"s3a://{bucket_name}/gold/fact/")
min_date = gold_df.select(F.min("POD_Date")).collect()[0][0]
max_date = gold_df.select(F.max("POD_Date")).collect()[0][0]
print(f"min date is {min_date} and max date is {max_date}")
dim_date_path = f"s3a://{bucket_name}/gold/dim/dim_date/"

dim_date_df = spark.createDataFrame([(min_date,max_date)],["start","end"])\
                    .select(F.explode(F.sequence(F.col("start"),F.col("end"))).alias("date"))\
                    .withColumn("day",F.dayofmonth(F.col("date")))\
                    .withColumn("month",F.month(F.col("date")))\
                    .withColumn("year",F.year(F.col("date")))\
                    .withColumn("dayofweek",F.dayofweek(F.col("date")))\
                    .withColumn("dayofyear",F.dayofyear(F.col("date")))\
                    .withColumn("weekofyear",F.weekofyear(F.col("date")))\
                    .withColumn("monthname",F.monthname(F.col("date")))\
                    .withColumn("quarter",F.quarter(F.col("date")))\
                    .withColumn("is_weekend",F.when(F.col("dayofweek").isin([1,7]),1).otherwise(0))\
                    .withColumn("is_weekday",F.when(F.col("dayofweek").isin([2,3,4,5,6]),1).otherwise(0))\
                    .withColumn("month_year",F.date_format(F.col("date"),"MMM-yyyy"))\
                    .withColumn("quater_year",F.concat(F.lit("Q"),F.quarter(F.col("date")).cast("string"),F.lit("-"),F.year(F.col("date")).cast("string")))
                    
dim_date_df.show(5)

# COMMAND ----------

# writing to dim_date table
dim_date_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(dim_date_path)
print(f"dim_date table created for {today} ✅")

# COMMAND ----------

# Creating checkpoint...
if today == str(date.today()):
    save_checkpoint(bucket_name,today)
else:
    print(f"checkpoint already created for {today}")



# COMMAND ----------

# USE THIS CELL ONLY FOR BACKFILLING
# from datetime import date,timedelta
# start_date = date(2026,3,1)
# end_date = date(2026,3,23)
# dates_to_process = []
# d = start_date
# while d <= end_date:
#   dates_to_process.append(d.strftime('%Y-%m-%d'))
#   d += timedelta(days=1)
# dates_to_process

# COMMAND ----------

# USE THIS CELL ONLY FOR BACKFILLING
# for today in dates_to_process:
#     #Bronze_layer
#     try:
#         bronze_df = spark.read.format("parquet").load(f"s3a://clg-demo-2026/raw/shipments/{today}.parquet")
#         print(f"Bronze Loaded : {bronze_df.count()} records ✅ ")

#     except FileNotFoundError:
#         print(f"No data found for {today} ❌")
#     except AnalysisException :
#         print(f"No data found for {today} ❌")
#     except Exception as e:
#         print(f"Unexpected error occurred while loading data for {today} ❌")

#         print(f"ERROR : {e} ❌")
#     # silver_layer
#     # Creating silver layer (transform data).......

#     silver_path = f"s3a://{bucket_name}/silver/"

#     silver_df = bronze_df\
#                 .drop("DRS No")\
#                 .drop("Location")\
#                 .drop("Created Date")\
#                 .drop("DRS Date")\
#                 .drop("DRS status")\
#                 .drop("Customer Name")\
#                 .drop("Shipper")\
#                 .drop("City")\
#                 .drop("Volumentric weight")\
#                 .drop("is_otp_verified")\
#                 .drop("Payment Transaction ID")\
#                 .drop("DEL from App")\
#                 .drop("Received by")\
#                 .drop("DRS Type")\
#                 .drop("scan time")\
#                 .withColumn("Employee_Name",F.lower(F.trim(F.col("Employee Name"))).cast(T.StringType()))\
#                 .withColumn("Employee_Number",F.col("Employee Number").cast(T.LongType()))\
#                 .withColumn("Waybill_No",F.col("Waybill No").cast(T.StringType()))\
#                 .withColumn("Attempt_Count",F.col("Attempt Count").cast(T.LongType()))\
#                 .withColumn("First_attempt",F.to_date(F.col("First attempt"),"d-M-yyyy, HH:mm"))\
#                 .withColumn("Consignee_Name",F.lower(F.trim(F.col("Consignee_Name"))).cast(T.StringType()))\
#                 .withColumn("Pincode",F.col("Pincode").cast(T.IntegerType()))\
#                 .withColumn("Amount_payable",F.regexp_replace(F.lower(F.trim(F.col("Amount payable"))),",","").cast(T.IntegerType()))\
#                 .withColumn("Status",F.lower(F.trim(F.col("Status"))).cast(T.StringType()))\
#                 .withColumn("Sub_status",F.lower(F.trim(F.col("Sub status"))).cast(T.StringType()))\
#                 .withColumn("POD_Date",F.to_date(F.col("POD Date"),"d-M-yyyy, HH:mm"))\
#                 .withColumn("Delivery_Payment_type",F.lower(F.trim(F.col("Delivery Payment type"))).cast(T.StringType()))\
#                 .withColumn("Pincode",F.col("Pincode").cast(T.StringType()))\
#                 .withColumn("RunDate",F.lit(today))\
#                 .select("Employee_Name","Employee_Number","Waybill_No","Attempt_Count","First_attempt","Consignee_Name","Pincode","Amount_payable","Status","Sub_status","POD_Date","Delivery_Payment_type","RunDate")
#     silver_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(f"{silver_path}{today}")
#     print(f"Silver written to {silver_path} ✅")
#     print(f"silver layer created for {today}-{silver_df.count()} rows uploaded")
#     #gold_layer
#     gold_fact_path = f"s3a://{bucket_name}/gold/fact/"

#     gold_fact_columns = ['Employee_Name',
#     'Employee_Number',
#     'Waybill_No',
#     'Attempt_Count',
#     'First_attempt',
#     'Consignee_Name',
#     'Pincode',
#     'Amount_payable',
#     'Status',
#     'Sub_status',
#     'POD_Date',
#     'Delivery_Payment_type',
#     'RunDate']
#     #gold table created.......

#     gold_fact_df = silver_df.select(gold_fact_columns)

#     # idempotency check
#     if DeltaTable.isDeltaTable(spark,gold_fact_path):
#         dt = DeltaTable.forPath(spark,gold_fact_path)
#         dt.delete(F.col("RunDate") == today)
#         print(f"deleting records for {today} from gold table ✅")
#     else:
#         print(f"creating gold table for {today} ✅")

#     gold_fact_df.write.format("delta").mode("append").save(gold_fact_path)
#     print(f"gold table created for {today} ✅")
#     # dim_date table creating ......
#     min_date = gold_fact_df.select(F.min("POD_Date")).collect()[0][0]
#     max_date = gold_fact_df.select(F.max("POD_Date")).collect()[0][0]
#     print(f"min date is {min_date} and max date is {max_date}")
#     dim_date_path = f"s3a://{bucket_name}/gold/dim/dim_date/"

#     dim_date_df = spark.createDataFrame([(min_date,max_date)],["start","end"])\
#                         .select(F.explode(F.sequence(F.col("start"),F.col("end"))).alias("date"))\
#                         .withColumn("day",F.dayofmonth(F.col("date")))\
#                         .withColumn("month",F.month(F.col("date")))\
#                         .withColumn("year",F.year(F.col("date")))\
#                         .withColumn("dayofweek",F.dayofweek(F.col("date")))\
#                         .withColumn("dayofyear",F.dayofyear(F.col("date")))\
#                         .withColumn("weekofyear",F.weekofyear(F.col("date")))\
#                         .withColumn("monthname",F.monthname(F.col("date")))\
#                         .withColumn("quarter",F.quarter(F.col("date")))\
#                         .withColumn("is_weekend",F.when(F.col("dayofweek").isin([1,7]),1).otherwise(0))\
#                         .withColumn("is_weekday",F.when(F.col("dayofweek").isin([2,3,4,5,6]),1).otherwise(0))\
#                         .withColumn("month_year",F.date_format(F.col("date"),"MMM-yyyy"))\
#                         .withColumn("quater_year",F.concat(F.lit("Q"),F.quarter(F.col("date")).cast("string"),F.lit("-"),F.year(F.col("date")).cast("string")))
#     dim_date_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(dim_date_path)
#     print(f"dim_date table created for {today} ✅")
#     save_checkpoint(bucket_name,today) #check_point_saved
#     print(f"checkpoint saved for {today} ✅")

                    