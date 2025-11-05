# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "1bd84002-70ee-4feb-adff-90d4d7e79ba7",
# META       "workspaceId": "748f6814-d554-47e6-9011-2e7b63d72c1d"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "9985e030-821e-a673-4f31-a852861e3467",
# META       "known_warehouses": [
# META         {
# META           "id": "9985e030-821e-a673-4f31-a852861e3467",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pyodbc
import pandas as pd

# Connection string (from Fabric WH)
conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=3klz7u5s3oqebfsrqkpjercbge-azvq6kmwg4uethky6vrp2c76zu.datawarehouse.fabric.microsoft.com,1433;"
    "Database=MDM_Bronze_WH;"
    "Authentication=ActiveDirectoryInteractive;"  
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

# Connect
conn = pyodbc.connect(conn_str)

# Query tables
df = pd.read_sql("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES", conn)

print("Tables in Warehouse:")
print(df.head())

# (Optional) save df to Lakehouse using Spark
spark_df = spark.createDataFrame(df)
spark_df.write.mode("overwrite").saveAsTable("dbo.warehouse_table_list")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
import os

# Base path of your Warehouse tables in OneLake
base_path = "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_WH.warehouse/Tables/dbo"

# List all tables (subdirectories under dbo schema)
table_dirs = [f.name for f in spark._jsparkSession.sessionState().fs().listStatus(base_path)]

# Extract clean table names (remove trailing / if present)
table_names = [name.strip("/") for name in table_dirs]

# Create Spark DataFrame with table names
table_list_df = spark.createDataFrame([Row(Table_Name=name) for name in table_names])

# Save into Lakehouse table (overwrite existing)
table_list_df.write.mode("overwrite").saveAsTable("dbo.table_list_cache")

# Show result
display(table_list_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
