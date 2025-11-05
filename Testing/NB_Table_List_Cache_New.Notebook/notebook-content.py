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
# META       "environmentId": "7b1314d9-16e4-a771-4052-e261ff18e179",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import os

# Set encryption key and initialization vector (IV) as environment variables
# These will be used by the TableManager to encrypt/decrypt sensitive data.
os.environ["TABLE_CLASSIFIER_KEY"] = "V9z@T3#kLm!8Wq2R"
os.environ["TABLE_CLASSIFIER_IV"] = "Xp7$Rz!qM2@eV1#N"

# Import the TableManager class from the library
from table_cache.table_manager_encrypted import TableManager

# Create an instance of TableManager, passing the active SparkSession.
table_manager = TableManager(spark_session=spark)

# Define the list of target schemas from which we want to retrieve table metadata.
schema_list = ["D365", "SAP"]

# Cache the list of tables from the specified schemas into a database table.
# - target_schema: schemas to scan for tables.
# - cache_table: the destination table where the table list will be cached.
# - mode: overwrite ensures that the existing cache_table data is replaced.
table_manager.cache_table_list(
    target_schema=schema_list,
    cache_table="MDM_Schema.package_table_list_cache",
    mode="overwrite"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM MDM_Bronze_Layer.MDM_Schema.package_table_list_cache LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
