# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Load sample data from github repo

# COMMAND ----------

import os
import zipfile

datasets_dbfs_path = '/dbfs/FileStore/emr'

dbutils.fs.mkdirs(datasets_dbfs_path)
  
with zipfile.ZipFile("sample_data.zip", "r") as zip_ref:
  zip_ref.extractall(datasets_dbfs_path)

# COMMAND ----------

import os
import re

DB_NAME = "emr_sample"

spark.sql(f"create database if not exists {DB_NAME}")
spark.sql(f"use {DB_NAME}")

for file_info in dbutils.fs.ls('/FileStore/emr/sample_data'):
  table_name = re.sub('(.*)\\.csv$', '\\1', file_info.name).lower()
  print(f"creating table '{DB_NAME}.{table_name}' from file {file_info.path}")
  spark.read.options(header=True).csv(file_info.path).write.mode('overwrite').saveAsTable(table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Load big dataset from blob storage

# COMMAND ----------

# DBTITLE 1,Enter your data connection information here
secrets = {'storage_account_name':'PUT_YOUR_BLOB_STORAGE_ACCOUNT_NAME_HERE',
           'container_name':'PUT_YOUR_BLOB_CONTAINER_NAME_HERE',
           'storage_account_key':'PUT_YOUR_BLOB_STORAGE_ACCOUNT_KEY_HERE'}

# COMMAND ----------

# DBTITLE 1,Mount storage container to DBFS
CSV_FOLDER = "/missouri/2021_07_11T17_42_12Z"

DB_NAME = 'missouri'

DATA_SOURCE = "wasbs://{container_name}@{storage_account_name}.blob.core.windows.net".format(**secrets)

EXTRA_CONFIGS = {"fs.azure.account.key.{storage_account_name}.blob.core.windows.net".format(**secrets): secrets['storage_account_key']}

CURRENTLY_MOUNTED = {mount_info.mountPoint for mount_info in dbutils.fs.mounts()}


if f"/mnt/{DB_NAME}" in CURRENTLY_MOUNTED: 
  dbutils.fs.unmount(f"/mnt/{DB_NAME}")

dbutils.fs.mount(
    source = DATA_SOURCE + CSV_FOLDER,
    mount_point = f"/mnt/{DB_NAME}",
    extra_configs = EXTRA_CONFIGS
)

[f.name for f in dbutils.fs.ls(f"/mnt/{DB_NAME}")]

# COMMAND ----------

# DBTITLE 1,Create new database and tables
import re

spark.sql(f"create database if not exists {DB_NAME}")
spark.sql(f"use {DB_NAME}")

for file_info in dbutils.fs.ls(f"/mnt/{DB_NAME}"):
  table_name = re.sub('(.*)\\.csv$', '\\1', file_info.name).lower()
  print(f"creating table '{DB_NAME}.{table_name}' from file {file_info.path}")
  spark.read.options(header=True).csv(file_info.path).write.mode('overwrite').saveAsTable(table_name)
