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
# MAGIC select Table_Name,Column_Name,validation_rule,Val_Description,AI_Reasoning
# MAGIC  from MDM_Bronze_Layer.D365.review_line_validation_suggestion_old

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select Table_Name,Column_Name,validation_rule,Val_Description,AI_Reasoning
# MAGIC  from MDM_Bronze_Layer.D365.review_line_validation_suggestion_old_1
# MAGIC where Table_Name = 'mserp_custtablebientity' and Column_Name = 'mserp_accountnum'
# MAGIC group by Table_Name,Column_Name,validation_rule,Val_Description,AI_Reasoning

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT Column_Name, Table_Name , validation_rule,Val_Description,AI_Reasoning
# MAGIC from MDM_Bronze_Layer.D365.review_line_validation_suggestion_old
# MAGIC group by Table_Name,Column_Name,validation_rule,Val_Description,AI_Reasoning

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 
# MAGIC     Table_Name,
# MAGIC     Column_Name,
# MAGIC     Validation_Rule,
# MAGIC     COUNT(DISTINCT AI_Reasoning) AS Unique_Reasoning_Count
# MAGIC FROM 
# MAGIC     MDM_Bronze_Layer.D365.review_line_validation_suggestion_old
# MAGIC GROUP BY 
# MAGIC     Table_Name,
# MAGIC     Column_Name,
# MAGIC     Validation_Rule
# MAGIC HAVING 
# MAGIC     COUNT(DISTINCT AI_Reasoning) > 1


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select Table_Name,Column_Name,validation_rule,Val_Description,AI_Reasoning
# MAGIC  from MDM_Bronze_Layer.D365.review_line_validation_suggestion_old_1
# MAGIC  where  Table_Name = 'mserp_custtablebientity' and Column_Name = 'mserp_accountnum'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Load your DataFrame (replace with actual load path)
df = spark.read.format("delta").load("abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_Layer.Lakehouse/Tables/D365/review_line_validation_suggestion")

# Drop duplicates keeping the first occurrence (based on Table_Name, Column_Name, Validation_Rule)
deduplicated_df = df.dropDuplicates(["Table_Name", "Column_Name", "Validation_Rule"])

# Show result (optional)
deduplicated_df.show()

deduplicated_df.write.format("delta").mode("overwrite").save("abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_Layer.Lakehouse/Tables/D365/review_line_validation_suggestion")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select Table_Name,Column_Name,validation_rule,Description,AI_Reasoning
# MAGIC  from MDM_Bronze_Layer.D365.review_line_validation_suggestion
# MAGIC  where  Table_Name = 'mserp_custtablebientity' and Column_Name = 'mserp_accountnum'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE D365.review_line_validation_suggestion_bkp USING DELTA AS (select * from D365.review_line_validation_suggestion where 1 = 1)
# MAGIC  

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
