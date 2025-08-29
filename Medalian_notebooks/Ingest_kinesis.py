# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# MAGIC %fs ls FileStore/tables

# COMMAND ----------

import boto3
import json

# ---------------------------
# AWS Kinesis Configuration
# ---------------------------
aws_region = "ap-south-1"
stream_name = "data_stream_weather"

# ---------------------------
# Use Databricks Secrets instead of hardcoding keys
# ---------------------------
access_key = dbutils.secrets.get(scope="kinesis-proj", key="access_key").strip()
secret_key = dbutils.secrets.get(scope="kinesis-proj", key="secret_key").strip()

# ---------------------------
# Read JSON file from DBFS using Spark
# ---------------------------
file_path = "dbfs:/FileStore/tables/messy_weather_data.json"

df = spark.read.json(file_path)
data = [row.asDict() for row in df.collect()]

# ---------------------------
# Initialize Kinesis client
# ---------------------------
kinesis_client = boto3.client(
    "kinesis",
    region_name=aws_region,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# ---------------------------
# Push data to Kinesis in batches
# ---------------------------
BATCH_SIZE = 500
batch = []
total_sent = 0

for record in data:
    batch.append({
        'Data': json.dumps(record, default=str),
        'PartitionKey': str(record.get("City", "default"))
    })
    if len(batch) == BATCH_SIZE:
        kinesis_client.put_records(StreamName=stream_name, Records=batch)
        total_sent += len(batch)
        batch = []

# Send remaining records
if batch:
    kinesis_client.put_records(StreamName=stream_name, Records=batch)
    total_sent += len(batch)

print(f" Data uploaded successfully to Kinesis stream: {stream_name}")
print(f"Total records sent: {total_sent}")

