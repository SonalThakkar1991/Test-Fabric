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

# Required libraries
from faker import Faker
import pandas as pd
import random
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Faker for US & UK
fake = Faker(['en_US', 'en_GB'])

# Generate fake data with intentional data quality issues
def generate_customer_data(n=10000):
    rows = []
    for _ in range(n):
        gender = fake.random_element(elements=('Male', 'Female'))
        name = fake.name_male() if gender == 'Male' else fake.name_female()
        email = fake.email()
        phone = fake.phone_number()
        address = fake.street_address()
        city = fake.city()
        state = fake.state()
        zipcode = fake.zipcode()
        bank_acc = fake.bban()
        credit_card = fake.credit_card_number()
        age = fake.random_int(min=18, max=75)
        credit_limit = round(fake.random_number(digits=5), 2)
        cust_id = fake.uuid4()

        # Inject nulls
        if random.random() < 0.05: address = None
        if random.random() < 0.05: city = None
        if random.random() < 0.05: state = None
        if random.random() < 0.05: gender = None

        # Inject bad email
        if random.random() < 0.05:
            email = "invalid-email@com" if random.random() < 0.5 else "noatsign.com"

        # Inject bad bank account
        if random.random() < 0.03:
            bank_acc = ''.join(random.choices('XYZ123', k=10))

        # Inject bad credit card
        if random.random() < 0.03:
            credit_card = 'CC-' + ''.join(random.choices('AB123', k=6))

        rows.append([
            cust_id, name, email, phone, address, city, state, zipcode,
            bank_acc, credit_card, age, gender, credit_limit
        ])

    columns = [
        'Cust_ID', 'Cust_Name', 'Cust_Email', 'Cust_Phone', 'Cust_Address',
        'Cust_City', 'State', 'ZipCode', 'Bank_Acc', 'Credit_Card', 'Age',
        'Gender', 'Credit_Limit'
    ]

    return pd.DataFrame(rows, columns=columns)

# Generate data
pdf = generate_customer_data(10000)

# Define Spark schema
schema = StructType([
    StructField("Cust_ID", StringType(), False),
    StructField("Cust_Name", StringType(), True),
    StructField("Cust_Email", StringType(), True),
    StructField("Cust_Phone", StringType(), True),
    StructField("Cust_Address", StringType(), True),
    StructField("Cust_City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("ZipCode", StringType(), True),
    StructField("Bank_Acc", StringType(), True),
    StructField("Credit_Card", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Credit_Limit", DoubleType(), True)
])

# Convert to Spark DataFrame
df = spark.createDataFrame(pdf, schema=schema)

# Show a preview of the Spark DataFrame
df.show(5, truncate=False)

df.write.mode("overwrite").option('overwriteSchema' , 'true').format("delta").save(
    "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_Layer.Lakehouse/Tables/D365/customer_details"
)

print("✅ Customer data written to Fabric Lakehouse table: D365/Customer_Details (auto-registered)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from faker import Faker
import pandas as pd
import random
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

fake = Faker()

# Sample Envu-style product names
product_names = [
    "K-Othrine WG", "Maxforce Quantum", "Racumin Sure",
    "Suspend Flexx", "Tempo SC Ultra", "Premise 200SC",
    "Aqua-Reslin", "DeltaDust", "Ficam WP", "Temprid FX"
]

# Valid Vendors and Types
valid_vendors = ["Envu", "Bayer", "Syngenta", "BASF", "Sumitomo"]
valid_types = ["Insecticide", "Rodenticide", "Fungicide", "Larvicide"]

# Categories and subcategories
categories = ["Pest Management Chemicals", None]
subcategories = ["Liquid", "Dust", "Gel", "Powder", "Spray", "Granule"]

def generate_product_data(n=10000):
    data = []
    for i in range(n):
        name = random.choice(product_names)
        quantity = random.choice([fake.random_int(1, 500), None]) if random.random() > 0.05 else None
        price = round(random.uniform(25, 1500), 2) if random.random() > 0.05 else None
        description = f"{name} used for {fake.word()} control in industrial or residential areas."
        code = f"PRD-{1001 + i}"
        vendor = random.choice(valid_vendors + [None]) if random.random() > 0.05 else None
        type_ = random.choice(valid_types + [None]) if random.random() > 0.05 else None
        category = random.choice(categories)
        subcategory = random.choice(subcategories)

        data.append([
            name, quantity, price, description, code,
            vendor, type_, category, subcategory
        ])

    return pd.DataFrame(data, columns=[
        "Product_Name", "Quantity", "Price", "Product_Description",
        "Product_Code", "Vendor", "Type", "Category", "SubCategory"
    ])

# Generate 10,000 records
pdf = generate_product_data(10000)

# Define schema for Spark DataFrame
schema = StructType([
    StructField("Product_Name", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Product_Description", StringType(), True),
    StructField("Product_Code", StringType(), True),
    StructField("Vendor", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("SubCategory", StringType(), True),
])

# Convert to Spark DataFrame
df = spark.createDataFrame(pdf, schema=schema)

# Show sample
df.show(5, truncate=False)

# Save to Fabric Lakehouse
df.write.mode("overwrite").format("delta").save(
    "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/"
    "MDM_Bronze_Layer.Lakehouse/Tables/D365/Product_Chemicals"
)

print("✅ Cleaned Envu product dataset with test nulls saved to: D365/Product_Chemicals")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE D365.table_list_cache
# MAGIC (Table_Name STRING) 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
