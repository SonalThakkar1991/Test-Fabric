-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "efb5676a-d439-4d96-a9ea-abc3fd926d45",
-- META       "default_lakehouse_name": "MDM_Bronze_Layer",
-- META       "default_lakehouse_workspace_id": "290f6b06-3796-4928-9d58-f562fd0bfecd",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "efb5676a-d439-4d96-a9ea-abc3fd926d45"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE SCHEMA IF NOT EXISTS  MDM_Bronze_Layer.Test

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE MATERIALIZED LAKE VIEW MDM_Bronze_Layer.Test.addressvw
-- MAGIC (CONSTRAINT check_zip CHECK (Zip IS NOT NULL)  on MISMATCH DROP)
-- MAGIC as
-- MAGIC select * from customer_address_table;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
