# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "efb5676a-d439-4d96-a9ea-abc3fd926d45",
# META       "default_lakehouse_name": "MDM_Bronze_Layer",
# META       "default_lakehouse_workspace_id": "290f6b06-3796-4928-9d58-f562fd0bfecd",
# META       "known_lakehouses": [
# META         {
# META           "id": "efb5676a-d439-4d96-a9ea-abc3fd926d45"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "1bd84002-70ee-4feb-adff-90d4d7e79ba7",
# META       "workspaceId": "748f6814-d554-47e6-9011-2e7b63d72c1d"
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE MDM_Bronze_Layer.dbo.address_validation_1 USING DELTA AS 
# MAGIC (select * from MDM_Bronze_Layer.dbo.address_validation where 1 = 1)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# 1. Read Delta table
table_path = "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_Layer.Lakehouse/Tables/dbo/address_validation_1"
df = spark.read.format("delta").load(table_path)

# 2. Read Excel file into pandas then Spark
excel_path = "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_Layer.Lakehouse/Files/Customer Name.xlsx"
customer_names_pd = pd.read_excel(excel_path)

# Assume first column of Excel = Customer names
customer_names_spark = spark.createDataFrame(customer_names_pd).withColumnRenamed(
    customer_names_pd.columns[0], "Customer_Name"
)

# 3. Add row numbers to align by row order (since Excel has no cst_key)
df_indexed = df.withColumn("_rn", F.monotonically_increasing_id())
names_indexed = customer_names_spark.withColumn("_rn", F.monotonically_increasing_id())

# 4. Join on row_id, only keep Customer_Name from Excel
df_joined = df_indexed.join(names_indexed.select("_rn", "Customer_Name"), on="_rn", how="left").drop("_rn")

# 5. Reorder columns: SourceSystem, cst_key, Customer_Name, then the rest
cols = df_joined.columns
cst_pos = cols.index("cst_key")

new_order = cols[:cst_pos+1] + ["Customer_Name"] + [c for c in cols if c not in ["Customer_Name"] + cols[:cst_pos+1]]

df_final = df_joined.select(new_order)

# 6. Write back to OneLake (overwrite schema)
df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
