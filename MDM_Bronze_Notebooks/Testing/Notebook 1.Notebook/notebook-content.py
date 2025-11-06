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
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE MATERIALIZED LAKE VIEW VW_CUSTomers
# MAGIC As
# MAGIC SELECT * FROM MDM_Bronze_Layer.D365.Customer_Details

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 
Netforum_path = "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_Layer.Lakehouse/Tables/D365/Customer_Details"

#  Use Spark to list all folders (tables) inside Qualtrics schema
Netforum_df = spark.read.format("binaryFile").load(Netforum_path + "/*").select("path")

#  Extract table names (folder names directly under /Qualtrics/)
Netforum_tables = list(set([row.path.split("/")[-2] for row in Netforum_df.collect()]))

#  Store full Delta table paths for next cell
tables = [f"D365/{name}" for name in Netforum_tables]

# Print table list
print("Detected Netforums Delta tables:")
for t in tables:
    print(f"- {t}")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import input_file_name

# Base Lakehouse path (GoogleAnalytics workspace)
base_path = "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_Layer.Lakehouse/Tables/"

#  Variable to track grand total size in bytes
grand_total_bytes = 0

#  Loop through each detected GoogleAnalytics table
for table in tables:
    table_path = f"{base_path}/{table}"

    try:
        #  Read distinct Parquet file paths
        df = spark.read.format("parquet").load(table_path).select(input_file_name()).distinct()
        file_paths = [row[0] for row in df.collect()]

        #  Sum file sizes
        total_size_bytes = 0
        for fp in file_paths:
            try:
                jpath = spark._jvm.org.apache.hadoop.fs.Path(fp)
                fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
                total_size_bytes += fs.getFileStatus(jpath).getLen()
            except Exception as e:
                print(f"Skipped file due to error: {fp}\n{e}")

        #  Add to grand total
        grand_total_bytes += total_size_bytes

        #  Format individual table size
        if total_size_bytes >= 1024 ** 3:
            size = total_size_bytes / (1024 ** 3)
            size_str = f"{size:.2f} GB"
        else:
            size = total_size_bytes / (1024 ** 2)
            size_str = f"{size:.2f} MB"

        # Output table size
        print(f" Table: {table} ➜  Size: {size_str}")

    except Exception as err:
        print(f"Error reading table: {table} ➜ {err}")

#  Format grand total size
print("\n Grand Total Storage Used by All Files:")
if grand_total_bytes >= 1024 ** 4:
    total_tb = grand_total_bytes / (1024 ** 4)
    print(f" Total Size: {total_tb:.2f} TB")
else:
    total_gb = grand_total_bytes / (1024 ** 3)
    print(f" Total Size: {total_gb:.2f} GB")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM MDM_Bronze_Layer.D365.data_quality_passed_tables LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Update MDM_Bronze_Layer.D365.data_quality_passed_tables set Source_System='D365'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE MDM_Bronze_Layer.D365.data_quality_passed_tables
# MAGIC SET Source_System = 'D365';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM MDM_Bronze_Layer.D365.data_quality_passed_tables LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT DISTINCT Table_Name , Column_Name , validation_rule FROM MDM_Bronze_Layer.D365.validation_suggestion LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
