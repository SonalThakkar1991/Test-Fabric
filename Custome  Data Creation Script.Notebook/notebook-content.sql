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

-- MAGIC %%sql
-- MAGIC CREATE TABLE  D365.customer_master
-- MAGIC (
-- MAGIC     customer_id STRING,              -- D365 Customer GUID
-- MAGIC     account_number STRING,           -- Customer Account Number
-- MAGIC     customer_group STRING,           -- Customer group in D365
-- MAGIC     customer_name STRING,            -- Customer full name
-- MAGIC     currency_code STRING,            -- Currency used by customer
-- MAGIC     customer_type STRING,            -- e.g., Organization, Individual
-- MAGIC     is_active BOOLEAN,
-- MAGIC     created_date TIMESTAMP,
-- MAGIC     modified_date TIMESTAMP,
-- MAGIC     ingestion_timestamp TIMESTAMP,
-- MAGIC     source_file_path STRING
-- MAGIC )


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE D365.customer_address
-- MAGIC (
-- MAGIC     customer_id STRING,
-- MAGIC     address_id STRING,
-- MAGIC     address_type STRING,             -- Billing / Shipping
-- MAGIC     address_line1 STRING,
-- MAGIC     address_line2 STRING,
-- MAGIC     city STRING,
-- MAGIC     state STRING,
-- MAGIC     country STRING,
-- MAGIC     postal_code STRING,
-- MAGIC     is_primary BOOLEAN,
-- MAGIC     created_date TIMESTAMP,
-- MAGIC     modified_date TIMESTAMP,
-- MAGIC     ingestion_timestamp TIMESTAMP,
-- MAGIC     source_file_path STRING
-- MAGIC )

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE D365.customer_contact
-- MAGIC (
-- MAGIC     customer_id STRING,
-- MAGIC     contact_id STRING,
-- MAGIC     contact_type STRING,             -- Phone, Email, Fax
-- MAGIC     contact_value STRING,
-- MAGIC     is_primary BOOLEAN,
-- MAGIC     created_date TIMESTAMP,
-- MAGIC     modified_date TIMESTAMP,
-- MAGIC     ingestion_timestamp TIMESTAMP,
-- MAGIC     source_file_path STRING
-- MAGIC )

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE D365.customer_metadata
-- MAGIC (
-- MAGIC     customer_id STRING,
-- MAGIC     data_source STRING,              -- e.g., D365 API / Batch Import
-- MAGIC     data_quality STRING,             -- e.g., Good, Review, Invalid
-- MAGIC     created_by STRING,
-- MAGIC     created_date TIMESTAMP,
-- MAGIC     modified_by STRING,
-- MAGIC     modified_date TIMESTAMP,
-- MAGIC     ingestion_timestamp TIMESTAMP,
-- MAGIC     source_file_path STRING
-- MAGIC )

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC INSERT INTO MDM_Bronze_Lakehouse.D365.customer_master
-- MAGIC SELECT
-- MAGIC   CONCAT('C', LPAD(CAST(id AS STRING), 4, '0')) AS customer_id,
-- MAGIC   CONCAT('ACC', LPAD(CAST(id AS STRING), 4, '0')) AS account_number,
-- MAGIC   CASE WHEN id % 2 = 0 THEN 'Retail' ELSE 'Wholesale' END AS customer_group,
-- MAGIC   CONCAT('Customer ', CAST(id AS STRING)) AS customer_name,
-- MAGIC   CASE WHEN id % 3 = 0 THEN 'USD' WHEN id % 3 = 1 THEN 'EUR' ELSE 'GBP' END AS currency_code,
-- MAGIC   CASE WHEN id % 2 = 0 THEN 'Individual' ELSE 'Organization' END AS customer_type,
-- MAGIC   TRUE AS is_active,
-- MAGIC   current_timestamp() AS created_date,
-- MAGIC   current_timestamp() AS modified_date,
-- MAGIC   current_timestamp() AS ingestion_timestamp,
-- MAGIC   CONCAT('/source/path/file', CAST(id AS STRING), '.json') AS source_file_path
-- MAGIC FROM range(1000) AS id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO MDM_Bronze_Lakehouse.D365.customer_contact
SELECT
  CONCAT('C', LPAD(CAST(id AS STRING), 4, '0')) AS customer_id,
  CONCAT('CONT', LPAD(CAST(id AS STRING), 4, '0')) AS contact_id,
  CASE WHEN id % 2 = 0 THEN 'Email' ELSE 'Phone' END AS contact_type,
  CASE WHEN id % 2 = 0 THEN CONCAT('customer', CAST(id AS STRING), '@example.com') ELSE CONCAT('+44 20 7946 ', LPAD(CAST(id AS STRING), 4, '0')) END AS contact_value,
  id % 2 = 0 AS is_primary,
  current_timestamp() AS created_date,
  current_timestamp() AS modified_date,
  current_timestamp() AS ingestion_timestamp,
  CONCAT('/source/path/file', CAST(id AS STRING), '.json') AS source_file_path
FROM range(1000) AS id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO MDM_Bronze_Lakehouse.D365.customer_metadata
SELECT
  CONCAT('C', LPAD(CAST(id AS STRING), 4, '0')) AS customer_id,
  CASE WHEN id % 2 = 0 THEN 'D365 API' ELSE 'Batch Import' END AS data_source,
  CASE WHEN id % 3 = 0 THEN 'Good' ELSE 'Review' END AS data_quality,
  'system' AS created_by,
  current_timestamp() AS created_date,
  'system' AS modified_by,
  current_timestamp() AS modified_date,
  current_timestamp() AS ingestion_timestamp,
  CONCAT('/source/path/file', CAST(id AS STRING), '.json') AS source_file_path
FROM range(1000) AS id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO MDM_Bronze_Lakehouse.D365.customer_address
SELECT
  CONCAT('C', LPAD(CAST(id AS STRING), 4, '0')) AS customer_id,
  CONCAT('ADDR', LPAD(CAST(id AS STRING), 4, '0')) AS address_id,
  CASE WHEN id % 2 = 0 THEN 'Billing' ELSE 'Shipping' END AS address_type,
  CONCAT('Street ', CAST(id AS STRING)) AS address_line1,
  CONCAT('Suite ', CAST(id AS STRING)) AS address_line2,
  CONCAT('City', CAST(id AS STRING % 100 AS STRING)) AS city,
  CASE WHEN id % 2 = 0 THEN 'NY' ELSE 'CA' END AS state,
  CASE WHEN id % 2 = 0 THEN 'USA' ELSE 'UK' END AS country,
  LPAD(CAST(id AS STRING), 5, '0') AS postal_code,
  id % 2 = 0 AS is_primary,
  current_timestamp() AS created_date,
  current_timestamp() AS modified_date,
  current_timestamp() AS ingestion_timestamp,
  CONCAT('/source/path/file', CAST(id AS STRING), '.json') AS source_file_path
FROM range(1000) AS id;



-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC # PySpark code in Notebook
-- MAGIC 
-- MAGIC from pyspark.sql.functions import col, concat, lit, lpad, current_timestamp, expr
-- MAGIC 
-- MAGIC # Generate 1000 rows
-- MAGIC df = spark.range(1000) \
-- MAGIC     .select(
-- MAGIC         concat(lit("C"), lpad(col("id").cast("string"), 4, "0")).alias("customer_id"),
-- MAGIC         concat(lit("ADDR"), lpad(col("id").cast("string"), 4, "0")).alias("address_id"),
-- MAGIC         expr("CASE WHEN id % 2 = 0 THEN 'Billing' ELSE 'Shipping' END").alias("address_type"),
-- MAGIC         concat(lit("Street "), col("id").cast("string")).alias("address_line1"),
-- MAGIC         concat(lit("Suite "), col("id").cast("string")).alias("address_line2"),
-- MAGIC         concat(lit("City"), (col("id") % 100).cast("string")).alias("city"),
-- MAGIC         expr("CASE WHEN id % 2 = 0 THEN 'NY' ELSE 'CA' END").alias("state"),
-- MAGIC         expr("CASE WHEN id % 2 = 0 THEN 'USA' ELSE 'UK' END").alias("country"),
-- MAGIC         lpad(col("id").cast("string"), 5, "0").alias("postal_code"),
-- MAGIC         (col("id") % 2 == 0).alias("is_primary"),
-- MAGIC         current_timestamp().alias("created_date"),
-- MAGIC         current_timestamp().alias("modified_date"),
-- MAGIC         current_timestamp().alias("ingestion_timestamp"),
-- MAGIC         concat(lit("/source/path/file"), col("id").cast("string"), lit(".json")).alias("source_file_path")
-- MAGIC     )
-- MAGIC 
-- MAGIC # Insert into Delta table
-- MAGIC df.write.format("delta").mode("append").saveAsTable("MDM_Bronze_Lakehouse.D365.customer_address")


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- Customer Master Table
CREATE TABLE MDM_Bronze_Layer.SAP.customer_master
(
    customer_id STRING,
    account_number STRING,
    customer_group STRING,
    customer_name STRING,
    currency_code STRING,
    customer_type STRING,
    is_active BOOLEAN,
    created_date TIMESTAMP,
    modified_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file_path STRING
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE MDM_Bronze_Layer.SAP.customer_address
(
    customer_id STRING,
    address_id STRING,
    address_type STRING,
    address_line1 STRING,
    address_line2 STRING,
    city STRING,
    state STRING,
    country STRING,
    postal_code STRING,
    is_primary BOOLEAN,
    created_date TIMESTAMP,
    modified_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file_path STRING
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE MDM_Bronze_Layer.SAP.customer_contact
(
    customer_id STRING,
    contact_id STRING,
    contact_type STRING,
    contact_value STRING,
    is_primary BOOLEAN,
    created_date TIMESTAMP,
    modified_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file_path STRING
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE MDM_Bronze_Layer.SAP.customer_metadata
(
    customer_id STRING,
    data_source STRING,
    data_quality STRING,
    created_by STRING,
    created_date TIMESTAMP,
    modified_by STRING,
    modified_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file_path STRING
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql.functions import col, concat, lit, lpad, current_timestamp, expr
-- MAGIC 
-- MAGIC # Generate 1000 rows for SAP
-- MAGIC df_sap = spark.range(1000) \
-- MAGIC     .select(
-- MAGIC         concat(lit("C"), lpad(col("id").cast("string"), 4, "0")).alias("customer_id"),
-- MAGIC         concat(lit("ACC"), lpad(col("id").cast("string"), 4, "0")).alias("account_number"),
-- MAGIC         expr("CASE WHEN id % 2 = 0 THEN 'Retail' ELSE 'Wholesale' END").alias("customer_group"),
-- MAGIC         concat(lit("SAP Customer "), col("id").cast("string")).alias("customer_name"),
-- MAGIC         expr("CASE WHEN id % 3 = 0 THEN 'USD' WHEN id % 3 = 1 THEN 'EUR' ELSE 'GBP' END").alias("currency_code"),
-- MAGIC         expr("CASE WHEN id % 2 = 0 THEN 'Individual' ELSE 'Organization' END").alias("customer_type"),
-- MAGIC         lit(True).alias("is_active"),
-- MAGIC         current_timestamp().alias("created_date"),
-- MAGIC         current_timestamp().alias("modified_date"),
-- MAGIC         current_timestamp().alias("ingestion_timestamp"),
-- MAGIC         concat(lit("/sap/source/path/file"), col("id").cast("string"), lit(".json")).alias("source_file_path")
-- MAGIC     )
-- MAGIC 
-- MAGIC df_sap.write.format("delta").mode("append").saveAsTable("MDM_Bronze_Layer.SAP.customer_master")
-- MAGIC 
-- MAGIC # Customer Address Table
-- MAGIC df_addr = spark.range(1000) \
-- MAGIC     .select(
-- MAGIC         concat(lit("C"), lpad(col("id").cast("string"), 4, "0")).alias("customer_id"),
-- MAGIC         concat(lit("ADDR"), lpad(col("id").cast("string"), 4, "0")).alias("address_id"),
-- MAGIC         expr("CASE WHEN id % 2 = 0 THEN 'Billing' ELSE 'Shipping' END").alias("address_type"),
-- MAGIC         concat(lit("Street "), col("id").cast("string")).alias("address_line1"),
-- MAGIC         concat(lit("Suite "), col("id").cast("string")).alias("address_line2"),
-- MAGIC         concat(lit("City"), (col("id") % 100).cast("string")).alias("city"),
-- MAGIC         expr("CASE WHEN id % 2 = 0 THEN 'NY' ELSE 'CA' END").alias("state"),
-- MAGIC         expr("CASE WHEN id % 2 = 0 THEN 'USA' ELSE 'UK' END").alias("country"),
-- MAGIC         lpad(col("id").cast("string"), 5, "0").alias("postal_code"),
-- MAGIC         (col("id") % 2 == 0).alias("is_primary"),
-- MAGIC         current_timestamp().alias("created_date"),
-- MAGIC         current_timestamp().alias("modified_date"),
-- MAGIC         current_timestamp().alias("ingestion_timestamp"),
-- MAGIC         concat(lit("/sap/source/path/file"), col("id").cast("string"), lit(".json")).alias("source_file_path")
-- MAGIC     )
-- MAGIC 
-- MAGIC df_addr.write.format("delta").mode("append").saveAsTable("MDM_Bronze_Layer.SAP.customer_address")
-- MAGIC 
-- MAGIC # Customer Contact Table
-- MAGIC df_contact = spark.range(1000) \
-- MAGIC     .select(
-- MAGIC         concat(lit("C"), lpad(col("id").cast("string"), 4, "0")).alias("customer_id"),
-- MAGIC         concat(lit("CONT"), lpad(col("id").cast("string"), 4, "0")).alias("contact_id"),
-- MAGIC         expr("CASE WHEN id % 2 = 0 THEN 'Email' ELSE 'Phone' END").alias("contact_type"),
-- MAGIC         expr("CASE WHEN id % 2 = 0 THEN concat('sap_customer', cast(id as string), '@sap.com') ELSE concat('+44 20 7946 ', lpad(cast(id as string), 4, '0')) END").alias("contact_value"),
-- MAGIC         (col("id") % 2 == 0).alias("is_primary"),
-- MAGIC         current_timestamp().alias("created_date"),
-- MAGIC         current_timestamp().alias("modified_date"),
-- MAGIC         current_timestamp().alias("ingestion_timestamp"),
-- MAGIC         concat(lit("/sap/source/path/file"), col("id").cast("string"), lit(".json")).alias("source_file_path")
-- MAGIC     )
-- MAGIC 
-- MAGIC df_contact.write.format("delta").mode("append").saveAsTable("MDM_Bronze_Layer.SAP.customer_contact")
-- MAGIC 
-- MAGIC # Customer Metadata Table
-- MAGIC df_metadata = spark.range(1000) \
-- MAGIC     .select(
-- MAGIC         concat(lit("C"), lpad(col("id").cast("string"), 4, "0")).alias("customer_id"),
-- MAGIC         expr("CASE WHEN id % 2 = 0 THEN 'SAP API' ELSE 'Batch Import' END").alias("data_source"),
-- MAGIC         expr("CASE WHEN id % 3 = 0 THEN 'Good' ELSE 'Review' END").alias("data_quality"),
-- MAGIC         lit("system").alias("created_by"),
-- MAGIC         current_timestamp().alias("created_date"),
-- MAGIC         lit("system").alias("modified_by"),
-- MAGIC         current_timestamp().alias("modified_date"),
-- MAGIC         current_timestamp().alias("ingestion_timestamp"),
-- MAGIC         concat(lit("/sap/source/path/file"), col("id").cast("string"), lit(".json")).alias("source_file_path")
-- MAGIC     )
-- MAGIC 
-- MAGIC df_metadata.write.format("delta").mode("append").saveAsTable("MDM_Bronze_Layer.SAP.customer_metadata")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS MDM_Bronze_Layer.CRM.customer_master (
    customer_id STRING,
    account_number STRING,
    customer_group STRING,
    customer_name STRING,
    currency_code STRING,
    customer_type STRING,
    is_active BOOLEAN,
    created_date TIMESTAMP,
    modified_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file_path STRING
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO MDM_Bronze_Layer.CRM.customer_master
SELECT
    CONCAT('C', LPAD(CAST(id AS STRING), 4, '0')) AS customer_id,
    CONCAT('CRMACC', LPAD(CAST(id AS STRING), 4, '0')) AS account_number,
    CASE WHEN id % 3 = 0 THEN 'GroupA' WHEN id % 3 = 1 THEN 'GroupB' ELSE 'GroupC' END AS customer_group,
    CONCAT('CRM Customer ', CAST(id AS STRING)) AS customer_name,
    CASE WHEN id % 2 = 0 THEN 'USD' ELSE 'EUR' END AS currency_code,
    CASE WHEN id % 2 = 0 THEN 'Type1' ELSE 'Type2' END AS customer_type,
    CASE WHEN id % 2 = 0 THEN TRUE ELSE FALSE END AS is_active,
    current_timestamp() AS created_date,
    current_timestamp() AS modified_date,
    current_timestamp() AS ingestion_timestamp,
    CONCAT('/source/CRM/path/file', CAST(id AS STRING), '.json') AS source_file_path
FROM range(1000);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS MDM_Bronze_Layer.CRM.customer_address (
    customer_id STRING,
    address_id STRING,
    address_type STRING,
    address_line1 STRING,
    address_line2 STRING,
    city STRING,
    state STRING,
    country STRING,
    postal_code STRING,
    is_primary BOOLEAN,
    created_date TIMESTAMP,
    modified_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file_path STRING
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO MDM_Bronze_Layer.CRM.customer_address
SELECT
    CONCAT('C', LPAD(CAST(id AS STRING), 4, '0')) AS customer_id,
    CONCAT('ADDR', LPAD(CAST(id AS STRING), 4, '0')) AS address_id,
    CASE WHEN id % 2 = 0 THEN 'Billing' ELSE 'Shipping' END AS address_type,
    CONCAT('Street ', CAST(id AS STRING)) AS address_line1,
    CONCAT('Suite ', CAST(id AS STRING)) AS address_line2,
    CONCAT('City', CAST(id % 100 AS STRING)) AS city,
    CASE WHEN id % 2 = 0 THEN 'NY' ELSE 'CA' END AS state,
    CASE WHEN id % 2 = 0 THEN 'USA' ELSE 'UK' END AS country,
    LPAD(CAST(id AS STRING), 5, '0') AS postal_code,
    id % 2 = 0 AS is_primary,
    current_timestamp() AS created_date,
    current_timestamp() AS modified_date,
    current_timestamp() AS ingestion_timestamp,
    CONCAT('/source/CRM/address/file', CAST(id AS STRING), '.json') AS source_file_path
FROM range(1000);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS MDM_Bronze_Layer.CRM.customer_contact (
    customer_id STRING,
    contact_id STRING,
    contact_name STRING,
    email STRING,
    phone STRING,
    created_date TIMESTAMP,
    modified_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file_path STRING
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO MDM_Bronze_Layer.CRM.customer_contact
SELECT
    CONCAT('C', LPAD(CAST(id AS STRING), 4, '0')) AS customer_id,
    CONCAT('CONT', LPAD(CAST(id AS STRING), 4, '0')) AS contact_id,
    CONCAT('Contact ', CAST(id AS STRING)) AS contact_name,
    CONCAT('contact', CAST(id AS STRING), '@crm.com') AS email,
    CONCAT('+1-555-', LPAD(CAST(id AS STRING), 4, '0')) AS phone,
    current_timestamp() AS created_date,
    current_timestamp() AS modified_date,
    current_timestamp() AS ingestion_timestamp,
    CONCAT('/source/CRM/contact/file', CAST(id AS STRING), '.json') AS source_file_path
FROM range(1000);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS MDM_Bronze_Layer.CRM.customer_order (
    order_id STRING,
    customer_id STRING,
    order_date TIMESTAMP,
    order_amount DOUBLE,
    currency_code STRING,
    status STRING,
    created_date TIMESTAMP,
    modified_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file_path STRING
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO MDM_Bronze_Layer.CRM.customer_order
SELECT
    CONCAT('ORD', LPAD(CAST(id AS STRING), 6, '0')) AS order_id,
    CONCAT('C', LPAD(CAST(id AS STRING), 4, '0')) AS customer_id,
    current_timestamp() AS order_date,
    ROUND(rand() * 1000, 2) AS order_amount,
    CASE WHEN id % 2 = 0 THEN 'USD' ELSE 'EUR' END AS currency_code,
    CASE WHEN id % 3 = 0 THEN 'Completed' WHEN id % 3 = 1 THEN 'Pending' ELSE 'Cancelled' END AS status,
    current_timestamp() AS created_date,
    current_timestamp() AS modified_date,
    current_timestamp() AS ingestion_timestamp,
    CONCAT('/source/CRM/order/file', CAST(id AS STRING), '.json') AS source_file_path
FROM range(1000)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS MDM_Bronze_Layer.D365.product_master (
    product_id STRING,
    product_code STRING,
    product_name STRING,
    product_category STRING,
    product_type STRING,
    is_active BOOLEAN,
    created_date TIMESTAMP,
    modified_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file_path STRING
)


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO MDM_Bronze_Layer.D365.product_master
SELECT
    CONCAT('P', LPAD(CAST(id AS STRING), 4, '0')) AS product_id,
    CONCAT('PROD', LPAD(CAST(id AS STRING), 4, '0')) AS product_code,
    CONCAT('Product ', CAST(id AS STRING)) AS product_name,
    CASE WHEN id % 3 = 0 THEN 'CategoryA' WHEN id % 3 = 1 THEN 'CategoryB' ELSE 'CategoryC' END AS product_category,
    CASE WHEN id % 2 = 0 THEN 'Type1' ELSE 'Type2' END AS product_type,
    id % 2 = 0 AS is_active,
    current_timestamp() AS created_date,
    current_timestamp() AS modified_date,
    current_timestamp() AS ingestion_timestamp,
    CONCAT('/source/D365/product/file', CAST(id AS STRING), '.json') AS source_file_path
FROM range(1000);


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS MDM_Bronze_Layer.D365.product_price (
    price_id STRING,
    product_id STRING,
    price DECIMAL(10,2),
    currency STRING,
    effective_date DATE,
    created_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP
)


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO MDM_Bronze_Layer.D365.product_price
SELECT
    CONCAT('PR', LPAD(CAST(id AS STRING), 4, '0')),
    CONCAT('P', LPAD(CAST(id AS STRING), 4, '0')),
    ROUND(rand() * 1000, 2),
    CASE WHEN id % 2 = 0 THEN 'USD' ELSE 'EUR' END,
    current_date(),
    current_timestamp(),
    current_timestamp()
FROM range(1000);


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS MDM_Bronze_Layer.D365.product_inventory (
    inventory_id STRING,
    product_id STRING,
    warehouse_id STRING,
    quantity INT,
    available_quantity INT,
    created_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP
)



-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO MDM_Bronze_Layer.D365.product_inventory
SELECT
    CONCAT('INV', LPAD(CAST(id AS STRING), 4, '0')),
    CONCAT('P', LPAD(CAST(id AS STRING), 4, '0')),
    CONCAT('WH', LPAD(CAST((id % 5) + 1 AS STRING), 3, '0')),
    CAST(rand() * 1000 AS INT),
    CAST(rand() * 800 AS INT),
    current_timestamp(),
    current_timestamp()
FROM range(1000);


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS MDM_Bronze_Layer.D365.product_category (
    category_id STRING,
    category_name STRING,
    description STRING,
    created_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP
)



-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO MDM_Bronze_Layer.D365.product_category
SELECT
    CONCAT('CAT', LPAD(CAST(id AS STRING), 3, '0')),
    CONCAT('Category ', CAST(id AS STRING)),
    CONCAT('Description for category ', CAST(id AS STRING)),
    current_timestamp(),
    current_timestamp()
FROM range(10);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE  TABLE MDM_Bronze_Layer.SAP.product_master
AS
SELECT
    CONCAT('SAP-P', LPAD(CAST(id AS STRING), 4, '0')) AS product_id,
    CONCAT('SAPPROD', LPAD(CAST(id AS STRING), 4, '0')) AS product_code,
    CONCAT('SAP Product ', CAST(id AS STRING)) AS product_name,
    CASE WHEN id % 4 = 0 THEN 'SAP_CategoryA'
         WHEN id % 4 = 1 THEN 'SAP_CategoryB'
         ELSE 'SAP_CategoryC'
    END AS product_category,
    CASE WHEN id % 2 = 0 THEN 'SAP_Type1' ELSE 'SAP_Type2' END AS product_type,
    id % 2 = 0 AS is_active,
    current_timestamp() AS created_date,
    current_timestamp() AS modified_date,
    current_timestamp() AS ingestion_timestamp,
    CONCAT('/source/SAP/product/file', CAST(id AS STRING), '.json') AS source_file_path
FROM range(1000);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE  MDM_Bronze_Layer.CRM.product_master
AS
SELECT
    CONCAT('CRM-P', LPAD(CAST(id AS STRING), 4, '0')) AS product_id,
    CONCAT('CRMPROD', LPAD(CAST(id AS STRING), 4, '0')) AS product_code,
    CONCAT('CRM Product ', CAST(id AS STRING)) AS product_name,
    CASE WHEN id % 5 = 0 THEN 'CRM_CategoryA'
         WHEN id % 5 = 1 THEN 'CRM_CategoryB'
         ELSE 'CRM_CategoryC'
    END AS product_category,
    CASE WHEN id % 2 = 0 THEN 'CRM_Type1' ELSE 'CRM_Type2' END AS product_type,
    id % 2 = 0 AS is_active,
    current_timestamp() AS created_date,
    current_timestamp() AS modified_date,
    current_timestamp() AS ingestion_timestamp,
    CONCAT('/source/CRM/product/file', CAST(id AS STRING), '.json') AS source_file_path
FROM range(1000);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE  TABLE  MDM_Bronze_Layer.SAP.product_pricing

AS
SELECT
    CONCAT('SAP-P', LPAD(CAST(id AS STRING), 4, '0')) AS product_id,
    ROUND(100 + RAND()*900, 2) AS price,
    CASE WHEN id % 2 = 0 THEN 'USD' ELSE 'EUR' END AS currency,
    current_timestamp() AS created_date,
    current_timestamp() AS modified_date,
    current_timestamp() AS ingestion_timestamp
FROM range(1000);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE MDM_Bronze_Layer.SAP.product_inventory
AS
SELECT
    CONCAT('SAP-P', LPAD(CAST(id AS STRING), 4, '0')) AS product_id,
    CAST(id * 10 AS INT) AS stock_quantity,
    CASE WHEN id % 2 = 0 THEN 'Warehouse1' ELSE 'Warehouse2' END AS warehouse_location,
    current_timestamp() AS created_date,
    current_timestamp() AS modified_date,
    current_timestamp() AS ingestion_timestamp
FROM range(1000);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE  MDM_Bronze_Layer.CRM.product_pricing

AS
SELECT
    CONCAT('CRM-P', LPAD(CAST(id AS STRING), 4, '0')) AS product_id,
    ROUND(50 + RAND()*500, 2) AS price,
    CASE WHEN id % 2 = 0 THEN 'USD' ELSE 'GBP' END AS currency,
    current_timestamp() AS created_date,
    current_timestamp() AS modified_date,
    current_timestamp() AS ingestion_timestamp
FROM range(1000);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE  MDM_Bronze_Layer.CRM.product_inventory

AS
SELECT
    CONCAT('CRM-P', LPAD(CAST(id AS STRING), 4, '0')) AS product_id,
    CAST(id * 5 AS INT) AS stock_quantity,
    CASE WHEN id % 2 = 0 THEN 'WarehouseA' ELSE 'WarehouseB' END AS warehouse_location,
    current_timestamp() AS created_date,
    current_timestamp() AS modified_date,
    current_timestamp() AS ingestion_timestamp
FROM range(1000);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM MDM_Bronze_Layer.CRM.customer_address LIMIT 1000

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM MDM_Bronze_Layer.CRM.customer_contact LIMIT 1000

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM MDM_Bronze_Layer.CRM.customer_master LIMIT 1000

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM MDM_Bronze_Layer.CRM.customer_order LIMIT 1000

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
