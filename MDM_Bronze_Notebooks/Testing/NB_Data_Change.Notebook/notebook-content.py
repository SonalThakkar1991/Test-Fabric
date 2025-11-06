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

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite" , "LEGACY")
spark.conf.set("spark.sql.ansi.enabled" , "false")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE D365.mserp_custtablebientity_1 USING DELTA AS 
# MAGIC (select * from D365.mserp_custtablebientity where 1 = 1)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql.functions import rand, when, col

# Step 1: Read table into DataFrame
df = spark.read.format("delta").load(
    "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/"
    "MDM_Bronze_Layer.Lakehouse/Tables/D365/mserp_custtablebientity"
)

# Step 2: Add random column and create new 'mserp_partycountry' column
df_updated = df.withColumn("random_val", rand()) \
               .withColumn("mserp_partycountry", when(col("random_val") <= 0.8, "USA").otherwise(None)) \
               .drop("random_val")  # clean up

# Step 3: Overwrite the table (in-place update)
df_updated.write.mode("overwrite").format("delta").save(
    "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/"
    "MDM_Bronze_Layer.Lakehouse/Tables/D365/mserp_custtablebientity"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql.functions import col, rand, when
import random

# Step 1: Read the existing table
df = spark.read.format("delta").load(
    "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/"
    "MDM_Bronze_Layer.Lakehouse/Tables/D365/mserp_custtablebientity"
)

# Step 2: Define sample US state names
states = [
    "California", "Texas", "Florida", "New York", "Illinois", 
    "Georgia", "Ohio", "North Carolina", "Pennsylvania", "Michigan"
]

# Step 3: Add a random column and update mserp_partystate
# Assign a random state name to 80% rows, null to 20%
df_updated = df.withColumn("random_index", (rand() * len(states)).cast("int")) \
               .withColumn("random_value", when(rand() <= 0.8, col("random_index")).otherwise(None)) \
               .withColumn("mserp_partystate", 
                   when(col("random_value").isNotNull(), 
                        when(col("random_value") < len(states), 
                             lit(states[0])
                        )
                   ).otherwise(None)
               )

# Optional: Replace the above with a proper state list using expr (better below ↓)

from pyspark.sql.functions import expr

# Create a SQL CASE expression to map index → state
case_expr = "CASE"
for i, state in enumerate(states):
    case_expr += f" WHEN random_value = {i} THEN '{state}'"
case_expr += " ELSE NULL END"

# Final update with proper mapping
df_updated = df_updated.withColumn("mserp_partystate", expr(case_expr)).drop("random_index", "random_value")

# Step 4: Overwrite the existing table with updated column
df_updated.write.mode("overwrite").format("delta").save(
    "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/"
    "MDM_Bronze_Layer.Lakehouse/Tables/D365/mserp_custtablebientity"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql.functions import expr

# Step 1: Load the existing Delta table
df = spark.read.format("delta").load(
    "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/"
    "MDM_Bronze_Layer.Lakehouse/Tables/D365/mserp_custtablebientity"
)

# Step 2: Replace `mserp_bankaccount` with a random 12-digit string
df_updated = df.withColumn(
    "mserp_bankaccount",
    expr("LPAD(CAST(CAST(rand() * 1000000000000 AS BIGINT) AS STRING), 12, '0')")
)

# Step 3: Write the updated DataFrame back (overwrites only the column)
df_updated.write.mode("overwrite").format("delta").save(
    "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/"
    "MDM_Bronze_Layer.Lakehouse/Tables/D365/mserp_custtablebientity"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, when, rand, monotonically_increasing_id, expr
from pyspark.sql.types import StringType
import random

# Table path
table_path = "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/" \
             "MDM_Bronze_Layer.Lakehouse/Tables/D365/mserp_dirpartytablebientity"

# Step 1: Read the existing table
df = spark.read.format("delta").load(table_path)

# Step 2: Drop wrong-typed columns (decimal)
df_cleaned = df.drop("mserp_primaryaddresslocation", "mserp_primarycontactemail", "mserp_primarycontactphone")

# Step 3: Create empty columns with StringType
df_fixed = df_cleaned \
    .withColumn("mserp_primaryaddresslocation", lit(None).cast(StringType())) \
    .withColumn("mserp_primarycontactemail", lit(None).cast(StringType())) \
    .withColumn("mserp_primarycontactphone", lit(None).cast(StringType()))

# Step 4: Generate random data lists
street_names = ["Maple", "Oak", "Pine", "Cedar", "Elm", "Washington", "Lakeview", "Hilltop"]
cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Austin", "Seattle", "Denver"]
states = ["NY", "CA", "IL", "TX", "AZ", "WA", "CO", "FL"]
domains = ["example.com", "mail.com", "testmail.org"]

def random_address():
    return f"{random.randint(100, 9999)} {random.choice(street_names)} St, {random.choice(cities)}, {random.choice(states)}"

def random_email():
    first = random.choice(["john", "jane", "mike", "sara", "alex", "chris", "emily", "david"])
    last = random.choice(["smith", "doe", "brown", "johnson", "lee", "clark", "jones"])
    return f"{first}.{last}@{random.choice(domains)}"

def random_phone():
    return f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

# Step 5: Convert to Pandas to generate random data (only for random values)
df_pd = df_fixed.select("Id").toPandas()

random.seed(42)
n = len(df_pd)

# Create 80% populated, 20% null
df_pd["mserp_primaryaddresslocation"] = [random_address() if random.random() < 0.8 else None for _ in range(n)]
df_pd["mserp_primarycontactemail"] = [random_email() if random.random() < 0.8 else None for _ in range(n)]
df_pd["mserp_primarycontactphone"] = [random_phone() if random.random() < 0.8 else None for _ in range(n)]

# Step 6: Convert back to Spark and join on primary key (Id)
df_new_values = spark.createDataFrame(df_pd)

# Join and update only the 3 columns
df_updated = df_fixed.alias("base").join(df_new_values.alias("new"), "Id", "left") \
    .select(
        col("base.*"),
        col("new.mserp_primaryaddresslocation").alias("mserp_primaryaddresslocation"),
        col("new.mserp_primarycontactemail").alias("mserp_primarycontactemail"),
        col("new.mserp_primarycontactphone").alias("mserp_primarycontactphone")
    )

# Step 6 (Updated): Drop old placeholders before adding new ones
df_no_old = df_fixed.drop("mserp_primaryaddresslocation", 
                          "mserp_primarycontactemail", 
                          "mserp_primarycontactphone")

df_updated = df_no_old.alias("base").join(df_new_values.alias("new"), "Id", "left") \
    .withColumn("mserp_primaryaddresslocation", col("new.mserp_primaryaddresslocation")) \
    .withColumn("mserp_primarycontactemail", col("new.mserp_primarycontactemail")) \
    .withColumn("mserp_primarycontactphone", col("new.mserp_primarycontactphone")) \
    .drop("new.mserp_primaryaddresslocation", 
          "new.mserp_primarycontactemail", 
          "new.mserp_primarycontactphone")

# Step 7: Write back to Delta table (overwrite mode)
df_updated.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .save(table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType
import random

# Table path
table_path = "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/" \
             "MDM_Bronze_Layer.Lakehouse/Tables/D365/mserp_custtransbientity"

# Step 1: Read table
df = spark.read.format("delta").load(table_path)

# Step 2: Drop existing 'mserp_paymmode' if exists
df_cleaned = df.drop("mserp_paymmode") if "mserp_paymmode" in df.columns else df

# Step 3: Generate new values in pandas
df_pd = df_cleaned.select("Id").toPandas()
n = len(df_pd)

paym_modes = ["CHECK", "CASH", "ELECTRONICS", "CARD"]

df_pd["mserp_paymmode"] = [random.choice(paym_modes) if random.random() < 0.8 else None for _ in range(n)]

# Step 4: Convert to Spark DataFrame and join
df_new = spark.createDataFrame(df_pd)

# Join and inject new column safely
df_updated = df_cleaned.alias("base").join(df_new.alias("new"), "Id", "left") \
    .withColumn("mserp_paymmode", col("new.mserp_paymmode")) \
    .drop("new.mserp_paymmode")

# Step 5: Write back to Lakehouse (overwrite mode with schema)
df_updated.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .save(table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import random

# Define Lakehouse path
table_path = "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/" \
             "MDM_Bronze_Layer.Lakehouse/Tables/D365/mserp_logisticspostaladdressbientity"

# Step 1: Load existing Delta table
df = spark.read.format("delta").load(table_path)

# Step 2: Drop existing column if exists
df_cleaned = df.drop("mserp_district") if "mserp_district" in df.columns else df

# Step 3: Generate sample US-style districts
districts = [
    "Orange County", "Cook County", "Maricopa County", "Harris County",
    "Clark County", "King County", "San Diego County", "Dallas County",
    "Broward County", "Palm Beach County"
]

# Step 4: Create new district values in pandas
df_pd = df_cleaned.select("Id").toPandas()
n = len(df_pd)
df_pd["mserp_district"] = [
    random.choice(districts) if random.random() < 0.8 else None for _ in range(n)
]

# Step 5: Convert to Spark and join on Id
df_new = spark.createDataFrame(df_pd)

df_updated = df_cleaned.alias("base").join(df_new.alias("new"), "Id", "left") \
    .withColumn("mserp_district", col("new.mserp_district")) \
    .drop("new.mserp_district")

# Step 6: Write updated DataFrame back to Delta
df_updated.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .save(table_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
import random

# Fabric Lakehouse path
table_path = "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/" \
             "MDM_Bronze_Layer.Lakehouse/Tables/D365/mserp_logisticselectronicaddressbientity"

# Read the table
df = spark.read.format("delta").load(table_path)

# Drop column if already present to avoid conflict
df_cleaned = df.drop("mserp_countryregioncode") if "mserp_countryregioncode" in df.columns else df

# Define ISO 3166-1 Alpha-2 codes (USA, UK, etc.)
country_codes = [
    "US", "IN", "GB", "CA", "AU",
    "DE", "FR", "JP", "CN", "BR"
]

# Create Pandas DataFrame with Ids
df_pd = df_cleaned.select("Id").toPandas()
n = len(df_pd)

# Assign new country codes with 80% values and 20% nulls
df_pd["mserp_countryregioncode"] = [
    random.choice(country_codes) if random.random() < 0.8 else None
    for _ in range(n)
]

# Convert back to Spark
df_new = spark.createDataFrame(df_pd)

# Join back to original data on Id and add updated column
df_updated = df_cleaned.alias("base").join(df_new.alias("new"), "Id", "left") \
    .withColumn("mserp_countryregioncode", col("new.mserp_countryregioncode")) \
    .drop("new.mserp_countryregioncode")

# Write the updated table back in-place
df_updated.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .save(table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT validation_rule FROM D365.validation_execution_master

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random
from pyspark.sql.functions import col

# Lakehouse Table Path
table_path = "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/" \
             "MDM_Bronze_Layer.Lakehouse/Tables/D365/mserp_logisticselectronicaddressbientity"

# Step 1: Read existing table
df = spark.read.format("delta").load(table_path)

# Step 2: Drop column if already exists (to avoid merge issues)
df_cleaned = df.drop("mserp_locator") if "mserp_locator" in df.columns else df

# Step 3: Convert Ids to Pandas
df_pd = df_cleaned.select("Id").toPandas()
n = len(df_pd)

# Step 4: Sample valid and invalid data generators
valid_emails = [
    "john.doe@gmail.com", "alice.smith@outlook.com", "bob.jones@yahoo.com",
    "contact@company.com", "support@service.org"
]

valid_phones = [
    "+1-202-555-0143", "2125557890", "+1-415-555-0000",
    "6467894321", "310-555-9988"
]

invalid_values = [
    "abc@xyz", "12345", "@gmail", "email@.com", "999999999999", None
]

# Step 5: Generate 70% valid + 30% invalid
def generate_locator():
    if random.random() < 0.7:
        return random.choice(valid_emails + valid_phones)
    else:
        return random.choice(invalid_values)

df_pd["mserp_locator"] = [generate_locator() for _ in range(n)]

# Step 6: Convert to Spark and join on Id
df_new = spark.createDataFrame(df_pd)

df_updated = df_cleaned.alias("base").join(df_new.alias("new"), "Id", "left") \
    .withColumn("mserp_locator", col("new.mserp_locator")) \
    .drop("new.mserp_locator")

# Step 7: Overwrite back to Fabric Lakehouse
df_updated.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .save(table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT mserp_locator FROM D365.mserp_logisticselectronicaddressbientity

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
