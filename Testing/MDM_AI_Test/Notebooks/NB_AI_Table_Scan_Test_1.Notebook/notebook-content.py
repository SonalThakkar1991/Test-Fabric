# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9e57a846-976e-4981-9cc0-9cc4380d26eb",
# META       "default_lakehouse_name": "Test_MDM_AI_Demo_Lakehouse",
# META       "default_lakehouse_workspace_id": "290f6b06-3796-4928-9d58-f562fd0bfecd",
# META       "known_lakehouses": [
# META         {
# META           "id": "9e57a846-976e-4981-9cc0-9cc4380d26eb"
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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

# Set your target database (Lakehouse schema)
spark.catalog.setCurrentDatabase("`Test_MDM_AI_Demo_Lakehouse`.`Netforum_Test`")  

# Get the list of tables in the current Lakehouse
tables_df = spark.catalog.listTables() #fetches metadata for all tables accessible in the current Spark session's default database


# Extract only managed table names
table_names = [table.name for table in tables_df if table.tableType == "MANAGED"] #Managed tables are tables whose metadata and data are fully controlled by the Lakehouse — not external or temporary tables.

# Create DataFrame from table_names
table_list_df = spark.createDataFrame([Row(Table_Name=name) for name in table_names]) 

#display(table_list_df)
#It creates a DF of table list which is present in above mentioned schema and write(load) into the table.

table_list_df.write.mode("overwrite").saveAsTable("`Test_MDM_AI_Demo_Lakehouse`.`Netforum_Test`.`table_list_cache`")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Test_MDM_AI_Demo_Lakehouse.Netforum_Test.table_list_cache LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
