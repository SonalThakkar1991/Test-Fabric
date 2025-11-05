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

from pyspark.sql import Row

# Set your target database (Lakehouse schema)
spark.catalog.setCurrentDatabase("D365")  

# Get the list of tables in the current database
tables_df = spark.catalog.listTables()

# Extract only table names into a list
table_names = [table.name for table in tables_df if table.tableType == "MANAGED"]

# Create DataFrame from table_names
table_list_df = spark.createDataFrame([Row(Table_Name=name) for name in table_names])

# Save to a cache table
table_list_df.write.mode("overwrite").saveAsTable("D365.table_list_cache") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
