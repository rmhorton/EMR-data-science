# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Load sample data from github repo

# COMMAND ----------

displayHTML(f'''
<iframe
  src="https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Fraw.githubusercontent.com%2Frmhorton%2FEMR-data-science%2Fmain%2FML_with_simulated_EMR.pptx&slide=8&wdSlideId=260"
  frameborder="0"
  width="80%"
  height="640"
></iframe>
''')

# wdSlideIndex=8

# COMMAND ----------

import os
import zipfile

data_path = '/FileStore/emr_sample'
local_path = '/dbfs' + data_path

dbutils.fs.mkdirs(local_path)
  
with zipfile.ZipFile("sample_data.zip", "r") as zip_ref:
  zip_ref.extractall(local_path)

## If you change your mind:
#    dbutils.fs.rm(data_path, recurse=True)

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /FileStore

# COMMAND ----------

!ls -R /dbfs/FileStore

# COMMAND ----------

import os
import re

DB_NAME = "emr_sample"

spark.sql(f"create database if not exists {DB_NAME}")
spark.sql(f"use {DB_NAME}")

for file_info in dbutils.fs.ls('/FileStore/emr_sample/csv'):
  table_name = re.sub('(.*)\\.csv$', '\\1', file_info.name).lower()
  print(f"creating table '{DB_NAME}.{table_name}' from file {file_info.path}")
  spark.read.options(header=True).csv(file_info.path).write.mode('overwrite').saveAsTable(table_name)


## If you change your mind:
#    spark.sql(f"drop database {DB_NAME} cascade")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Load big dataset from storage container

# COMMAND ----------

# DBTITLE 1,Enter your data connection information here
secrets = {'storage_account_name':'syntheauploadsa',
           'container_name':'syntheadata1',
           'data_path': '/missouri/2021_07_11T17_42_12Z_parquet',
           'sas_token':'PUT_YOUR_SAS_TOKEN_HERE'}

# COMMAND ----------

if secrets['sas_token'] == 'PUT_YOUR_SAS_TOKEN_HERE':
    displayHTML('''<blink><font color="red"><h1>You need to enter your connection info in the 'secrets' dict!</h1></font></blink>''')

# COMMAND ----------

# DBTITLE 1,Mount storage container to DBFS
DATA_SOURCE = "wasbs://{container_name}@{storage_account_name}.blob.core.windows.net".format(**secrets)

DATA_PATH = secrets['data_path']

DB_NAME = 'missouri'

MOUNT_POINT = f"/mnt/{DB_NAME}"

EXTRA_CONFIGS = {"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net".format(**secrets): secrets['sas_token']}

CURRENTLY_MOUNTED = {mount_info.mountPoint for mount_info in dbutils.fs.mounts()}
if MOUNT_POINT in CURRENTLY_MOUNTED: 
  dbutils.fs.unmount(MOUNT_POINT)

dbutils.fs.mount(
    source = DATA_SOURCE + DATA_PATH,
    mount_point = MOUNT_POINT,
    extra_configs = EXTRA_CONFIGS
)

[f.name for f in dbutils.fs.ls(MOUNT_POINT)]

# COMMAND ----------

# DBTITLE 1,Create new database and tables
import re

spark.sql(f"create database if not exists {DB_NAME}")
spark.sql(f"use {DB_NAME}")

for file_info in dbutils.fs.ls(MOUNT_POINT):
  table_name = re.sub('(.*)\\.parquet/$', '\\1', file_info.name).lower()
  print(f"creating table '{DB_NAME}.{table_name}' from file {file_info.path}")
  spark.read.parquet(file_info.path).write.mode('overwrite').saveAsTable(table_name)

## If you change your mind:
#   spark.sql(f"drop database {DB_NAME} cascade")

# COMMAND ----------


