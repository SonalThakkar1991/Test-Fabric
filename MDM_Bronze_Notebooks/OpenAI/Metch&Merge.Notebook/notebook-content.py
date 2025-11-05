# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "1bd84002-70ee-4feb-adff-90d4d7e79ba7",
# META       "workspaceId": "748f6814-d554-47e6-9011-2e7b63d72c1d"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# %pip install rapidfuzz
import re
from rapidfuzz import process, fuzz
from pyspark.sql.types import IntegerType, StringType, StructField, BooleanType
from pyspark.sql.functions import lit,col,when,count, sum

def normalize_name(name):
    name = name.strip()
    if ',' in name:
        parts = [p.strip() for p in name.split(',', 1)]
        if len(parts) == 2:
            name = f"{parts[1]} {parts[0]}"
    name = re.sub(r'\s+', ' ', name).title()
    parts = name.split()
    if len(parts) == 3 and len(parts[1]) == 1:
        parts.pop(1)
    return ' '.join(parts)

def match_all_employees(
    employee_col="Employee_Name",
    employee_table="SAP_Employee_Data",
    email_col="Employee_Email",
    title_col="Position_Title",
    sales_col="SALES_REP",
    rep_id_col="Sales_Rep_id",
    sales_table="CRM_Sales_Rep_Data",
    match_threshold=50
):
    # Load data
    employee_df = spark.read.format("delta").load("abfss://748f6814-d554-47e6-9011-2e7b63d72c1d@onelake.dfs.fabric.microsoft.com/61eadb89-0a8d-44c6-8892-0e8e0ef49a85/Tables/Bronze/SAP_Employee_Data")
    sales_df = spark.read.format("delta").load("abfss://748f6814-d554-47e6-9011-2e7b63d72c1d@onelake.dfs.fabric.microsoft.com/61eadb89-0a8d-44c6-8892-0e8e0ef49a85/Tables/Bronze/CRM_Sales_Rep_Data")

    # Normalize employee names and store corresponding row details
    employee_info = {
        normalize_name(row[employee_col]): {
            "raw_name": row[employee_col],
            "email": row[email_col],
            "title": row[title_col]
        }
        for row in employee_df.select(employee_col, email_col, title_col).distinct().collect()
    }

    results = []

    for row in sales_df.collect():
        sales_name_raw = row[sales_col]
        sales_name_norm = normalize_name(sales_name_raw)
        sales_rep_id = row[rep_id_col]

        best_match, best_score, _ = process.extractOne(
            query=sales_name_norm,
            choices=list(employee_info.keys()),
            scorer=fuzz.token_sort_ratio
        )

        if best_score == 100:
            original = row.asDict()
            original["match_score"] = int(best_score)
            original["Employee_Email"] = None
            original["Position_Title"] = None
            results.append(original)

            matched_row = {col: None for col in sales_df.columns}
            matched_row[rep_id_col] = sales_rep_id
            matched_row[sales_col] = employee_info[best_match]["raw_name"]
            matched_row["match_score"] = int(best_score)
            matched_row["Employee_Email"] = employee_info[best_match]["email"].split('@')[0] + "@dynatech.com"
            matched_row["Position_Title"] = employee_info[best_match]["title"]
            results.append(matched_row)
        else:
            top_matches = process.extract(
                query=sales_name_norm,
                choices=list(employee_info.keys()),
                scorer=fuzz.token_sort_ratio,
                limit=3
            )
            top_matches = [m for m in top_matches if m[1] >= match_threshold]

            if top_matches:
                original = row.asDict()
                original["match_score"] = int(top_matches[0][1])
                original["Employee_Email"] = None
                original["Position_Title"] = None
                results.append(original)

                for match_name, score, _ in top_matches:
                    matched_row = {col: None for col in sales_df.columns}
                    matched_row[rep_id_col] = sales_rep_id
                    matched_row[sales_col] = employee_info[match_name]["raw_name"]
                    matched_row["match_score"] = int(score)
                    # matched_row["Employee_Email"] = employee_info[match_name]["email"]
                    matched_row["Employee_Email"] = employee_info[match_name]["email"].split('@')[0] + "@dynatech.com"
                    matched_row["Position_Title"] = employee_info[match_name]["title"]
                    results.append(matched_row)

    # Extend schema to include new columns
    final_schema = sales_df.schema\
        .add("match_score", IntegerType())\
        .add(StructField("Employee_Email", StringType(), True))\
        .add(StructField("Position_Title", StringType(), True))

    if results:
        rows = [tuple(r.get(c.name, None) for c in final_schema.fields) for r in results]
        final_df = spark.createDataFrame(rows, schema=final_schema)
        df=final_df
        sales_ids_with_2_occurrences = (
            df.filter(col("match_score") == 100)
            .groupBy("Sales_Rep_id")
            .agg(
                count("*").alias("count_100")
            )
            .filter(col("count_100") == 2)
            .select("Sales_Rep_id")
        )
        sales_ids_list = [row["Sales_Rep_id"] for row in sales_ids_with_2_occurrences.collect()]
        if "Selected" not in df.columns:
            df = df.withColumn("Selected", lit(False).cast(BooleanType()))
        updated_df = df.withColumn(
            "Selected",
            when(col("Sales_Rep_id").isin(sales_ids_list), True).otherwise(col("Selected"))
        )
        # print(updated_df.show())
        updated_df.write.format("delta").mode("overwrite").format("delta").save("abfss://748f6814-d554-47e6-9011-2e7b63d72c1d@onelake.dfs.fabric.microsoft.com/61eadb89-0a8d-44c6-8892-0e8e0ef49a85/Tables/Bronze/Employee_Merge_Suggestions")
    else:
        return spark.createDataFrame([], final_schema)

# Run
df = match_all_employees()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee_df = spark.read.format("delta").load("abfss://748f6814-d554-47e6-9011-2e7b63d72c1d@onelake.dfs.fabric.microsoft.com/61eadb89-0a8d-44c6-8892-0e8e0ef49a85/Tables/Bronze/SAP_Employee_Data")
sales_df = spark.read.format("delta").load("abfss://748f6814-d554-47e6-9011-2e7b63d72c1d@onelake.dfs.fabric.microsoft.com/61eadb89-0a8d-44c6-8892-0e8e0ef49a85/Tables/Bronze/CRM_Sales_Rep_Data")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee_df.count(), sales_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, first, struct, expr

# Start Spark session
spark = SparkSession.builder.appName("MergeRows").getOrCreate()

# Sample schema based on your image
columns = ["Index", "SalesID", "Name", "ShortName", "Level", "Selected"]

# Sample input data (replace with actual DataFrame read from CSV/Excel)
df = spark.read.format("delta").load("abfss://748f6814-d554-47e6-9011-2e7b63d72c1d@onelake.dfs.fabric.microsoft.com/61eadb89-0a8d-44c6-8892-0e8e0ef49a85/Tables/dbo/employee_name_suggestions_final")
    

# Filter only rows with Selected = true
selected_df = df.filter(col("Selected") == True)

# Group by SalesID and collect matches
grouped = selected_df.groupBy("Sales_Rep_id").agg(
    collect_list(struct("Sales_Rep_id", "TERRITORY", "SALES_REP", "TERRITORY_REP_FK", "TERR_CWID")).alias("rows")
)

# Filter only those groups with 2 entries (meaning duplicates with true)
filtered = grouped.filter(expr("size(rows) = 2"))

# Create new DataFrame with required transformation
merged_df = filtered.selectExpr(
    "rows[0].Sales_Rep_id as Sales_Rep_id",
    "rows[0].TERRITORY as TERRITORY",
    "rows[1].SALES_REP as SALES_REP",
    "rows[0].TERRITORY_REP_FK as TERRITORY_REP_FK",
    "rows[0].TERR_CWID as TERR_CWID"
)

# Show result
merged_df.show(truncate=False)

# merged_df.write.format("delta").mode("overwrite").format("delta").save("abfss://748f6814-d554-47e6-9011-2e7b63d72c1d@onelake.dfs.fabric.microsoft.com/61eadb89-0a8d-44c6-8892-0e8e0ef49a85/Tables/Bronze/CleanCRMData")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
