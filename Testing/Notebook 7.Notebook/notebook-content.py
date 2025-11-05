# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1e538e02-6831-4e29-924a-6eb38620139c",
# META       "default_lakehouse_name": "Timesheet_LK",
# META       "default_lakehouse_workspace_id": "d43ccbfa-f57d-47db-b1ca-fd0b80700570",
# META       "known_lakehouses": [
# META         {
# META           "id": "1e538e02-6831-4e29-924a-6eb38620139c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * into [Timesheet_LK].[dbo].[Timesheet1] from [Timesheet_LK].[dbo].[Timesheet] where 1 = 2;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE Timesheet_LK.dbo.Timesheet1
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM Timesheet_LK.dbo.Timesheet
# MAGIC WHERE 1 = 2

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
