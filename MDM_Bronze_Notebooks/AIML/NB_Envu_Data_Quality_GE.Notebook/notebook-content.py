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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Python Code for Data Quality

import great_expectations as ge
import pandas as pd
from datetime import datetime
import pytz
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from uuid import uuid4  
from pyspark.sql.functions import upper, col

# Load only selected rows for validation
expectations_table_path = "abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_Layer.Lakehouse/Tables/D365/validation_execution_master"


expectations_df = (
    spark.read.format("delta")
    .load(expectations_table_path)
    .filter(
        (col("Selected") == True) | (upper(col("Selected")) == "TRUE")
    )
    .toPandas()
)


# List of tables to process based on validation table
selected_tables = expectations_df['Table_Name'].unique().tolist()

# GE context setup
context = ge.get_context()
suite_name = "my_expectation_suite"
data_source_name = "pandas"
data_source = context.data_sources.add_pandas(data_source_name)
suite = ge.ExpectationSuite(name=suite_name)
suite = context.suites.add(suite)

# ✅ Cache to avoid redefining assets
data_assets_cache = {}

# Helper function to build audit row
def build_audit_row(table, column, rule, index_key, index_value, failed_value, source_system, ai_reasoning, ai_columnname, val_description):
    now = datetime.utcnow()
    fixed_date = datetime(2025, 7, 14, now.hour, now.minute, now.second, now.microsecond)
    loadtime_utc_str = fixed_date.strftime("%Y-%m-%d %H:%M:%S.%f")

    return {
        'Source_System' : str(source_system),
        'Table_Name': str(table),
        'Column_Name': str(column),
        'AI_ColumnName': str(ai_columnname),
        'Index_Key': str(index_key),
        'validation_rules': str(rule),
        'Val_Description': str(val_description),
        'Index_Value': str(index_value),
        'Failed_Value': str(failed_value),
        'LoadTime_UTC': loadtime_utc_str,   
        'LoadTime_PST': str(datetime.now().astimezone(pytz.timezone('US/Pacific'))),
        'ai_reasoning': str(ai_reasoning),
        'SampleData' : str(SampleData)
    }

def record_passed_table_info(table_name, row_count, validation_rules_df):
    load_time_pst = datetime.now().astimezone(pytz.timezone('US/Pacific')).date()

    validation_rules_df = validation_rules_df.copy()
    validation_rules_df["Table_Name"] = table_name
    validation_rules_df["LoadDate_PST"] = load_time_pst
    validation_rules_df["Count"] = row_count

    pdf = validation_rules_df[[
        "Table_Name", "LoadDate_PST", "Count",
        "validation_rule", "AI_ColumnName", "AI_Reasoning"
    ]]
    
    sdf = spark.createDataFrame(pdf)

    sdf = sdf.select(
        col("Table_Name").cast("string"),
        col("LoadDate_PST").cast("timestamp"),      
        col("Count").cast("long"),
        col("validation_rule").cast("string"),
        col("AI_ColumnName").cast("string"),
        col("AI_Reasoning").cast("string")
    )

    passed_tables_path = 'abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_Layer.Lakehouse/Tables/D365/data_quality_passed_tables'
    sdf.write.mode("append").option("overwriteSchema", "true").save(passed_tables_path)

    print(f"{table_name} has passed all data quality checks.")

# Loop through selected tables
for Stage_Table_Var in selected_tables:
    try:
        print(f"📘 Processing table: {Stage_Table_Var}")

        # Load data from source
        clean_df = spark.read.format("delta").load(
            f"abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_Layer.Lakehouse/Tables/D365/{Stage_Table_Var}"
        )

        # Drop fully null columns
        non_null_cols = [c for c in clean_df.columns if clean_df.filter(f"{c} IS NOT NULL").limit(1).count() > 0]
        clean_df = clean_df.select(*non_null_cols)

        # Look for a column that includes 'delete_flag'
        delete_flag_column = None
        for c in clean_df.columns:
            if 'delete_flag' in c.lower():
                delete_flag_column = c
                print(f"✅ Found delete flag column: {delete_flag_column}")
                break

        # Filter if delete_flag column is found
        if delete_flag_column:
            clean_df = clean_df.filter(col(delete_flag_column) == 0)
            print(f"✅ Applied filter {delete_flag_column} = 0")
        else:
            print(f"⚠️ No delete_flag column found in {Stage_Table_Var}. Proceeding with full dataset.")

        clean_df = clean_df.toPandas()

        expectation_for_table = expectations_df[expectations_df['Table_Name'] == Stage_Table_Var]
        ai_reasoning_map = {}


        # Add expectations to the suite
        for _, row in expectation_for_table.iterrows():
            column_name = row['Column_Name'].strip()
            ai_columnname = row['AI_ColumnName'].strip()
            validation_rule = row['validation_rule'].strip()
            val_description = row['Val_Description'].strip()
            index_key = row['Index_Key'].strip()
            # source_system = row['source_system'].strip()
            source_system = "Netforum"
            condition = row['Condition'].strip() if pd.notna(row['Condition']) else None
            min_range = row['Minimum_Range']
            max_range = row['Maximum_Range']
            ai_reasoning_value = row['AI_Reasoning'].strip() if pd.notna(row['AI_Reasoning']) else ""
            ai_reasoning_map[(validation_rule, column_name)] = ai_reasoning_value
            SampleData = row['SampleData'].strip() if pd.notna(condition) and pd.notna(row['SampleData']) else ""

            # from pyspark.sql.functions import when

            # def clean_null_like_values(df, column):
            #     return df.withColumn(
            #         column,
            #         when(col(column).isin("None", "null", "NULL", ""), None).otherwise(col(column))
            #     )


            if validation_rule == 'expect_column_values_to_not_be_null':
                # df = clean_null_like_values(df, column_name)
                expectation = ge.expectations.ExpectColumnValuesToNotBeNull(column=column_name)

            elif validation_rule == 'expect_column_values_to_match_regex':
                if condition:
                    expectation = ge.expectations.ExpectColumnValuesToMatchRegex(column=column_name, regex=condition)

            elif validation_rule == 'expect_column_values_to_be_unique':
                expectation = ge.expectations.ExpectColumnValuesToBeUnique(column=column_name)

            elif validation_rule == 'expect_column_values_to_be_between':
                expectation = ge.expectations.ExpectColumnValuesToBeBetween(
                    column=column_name,
                    min_value=min_range,
                    max_value=max_range
                )

            elif validation_rule == 'expect_column_values_to_be_in_set':
                if condition:
                    allowed_values = [val.strip() for val in condition.split(',')]
                    expectation = ge.expectations.ExpectColumnValuesToBeInSet(
                        column=column_name,
                        value_set=allowed_values
                    )
                else:
                    continue  

            else:
                print(f"⚠️ Unknown validation rule: {validation_rule}")
                continue


            suite.add_expectation(expectation)

        # Skip if no expectations defined
        if not suite.expectations:
            print(f"⚠️ No expectations for {Stage_Table_Var}, skipping...")
            clean_df = clean_df.toPandas()

        # ✅ Create or reuse the data asset for this table
        if Stage_Table_Var not in data_assets_cache:
            data_asset_name = f"{Stage_Table_Var}_asset"
            data_asset = data_source.add_dataframe_asset(data_asset_name)
            data_assets_cache[Stage_Table_Var] = data_asset
        else:
            data_asset = data_assets_cache[Stage_Table_Var]

        # ✅ Clear batch definitions and create a new one
        data_asset.batch_definitions.clear()
        unique_id = uuid4().hex[:8]
        batch_definition_name = f"batch_def_{Stage_Table_Var}_{unique_id}"
        batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

        # Run validation
        expectation_suite = context.suites.get(name=suite_name)
        validation_run_id = f"validation_run_{Stage_Table_Var}_{uuid4().hex[:8]}"
        validation_definition = ge.ValidationDefinition(
            data=batch_definition,
            suite=expectation_suite,
            name=validation_run_id  
        )

        batch_parameters = {"dataframe": clean_df}
        validation_result = validation_definition.run(batch_parameters=batch_parameters, result_format={"result_format": "COMPLETE"})

        # Collect failed rows
        results = []
        unexpected_indices = set()
        for result in validation_result.to_json_dict()["results"]:
            try:
                exp_type = result["expectation_config"]["type"]
                col_name = result["expectation_config"]["kwargs"]["column"]
                ai_reasoning = ai_reasoning_map.get((exp_type, col_name), "")
                index_key = expectation_for_table.iloc[0]['Index_Key'].strip()
                source_system = "D365"
                sample_data_value = expectation_for_table.iloc[0]['SampleData'].strip()

                for idx in result["result"].get("unexpected_index_list", []):
                    try:
                        index_key_value = clean_df.iloc[idx][index_key]
                        results.append(build_audit_row(
                            Stage_Table_Var, col_name, exp_type,
                            index_key_value,
                            clean_df.iloc[idx][index_key],
                            clean_df.iloc[idx][col_name],
                            source_system,
                            ai_reasoning,
                            ai_columnname,
                            val_description,
                            sample_data_value
                        ))
                        unexpected_indices.add(idx)

                        if has_condition and 'SampleData' in row and pd.notna(row['SampleData']):
                                sample_data_value = row['SampleData'].strip()
                        else:
                            sample_data_value = None  # <- Write as null if no sample data or no condition
                    except Exception as e:
                        print(f"⚠️ Error parsing validation result: {e}")
            except Exception as e:
                print(f"⚠️ Error parsing validation result: {e}")

        # Drop failed rows
        clean_df = clean_df.drop(list(unexpected_indices)).reset_index(drop=True)

        # Write audit table if needed
        if results:
            audit_df = pd.DataFrame(results).astype(object).where(pd.notnull(pd.DataFrame(results)), None)
            audit_df = audit_df.applymap(lambda x: x.item() if hasattr(x, 'item') else x)

            audit_df = audit_df.astype(str)
            audit_spark_df = spark.createDataFrame(audit_df)
            audit_spark_df = audit_spark_df.select([col(c).cast(StringType()).alias(c) for c in audit_spark_df.columns])
            audit_spark_df.write.option("mergeSchema", "true").mode("append").saveAsTable("D365.data_quality_audit_table")
            print(f"✅ Bad rows written to audit table for {Stage_Table_Var}")

        # Write clean_df to Silver Stage
        if clean_df.empty:
            print(f"⚠️ Clean DataFrame for {Stage_Table_Var} is empty after validation.")
            continue

        base_table_name = Stage_Table_Var
        silver_table_name = f"SilverStage_{base_table_name}"
        silver_table_path = f"abfss://Dynatech_MDM_Silver@onelake.dfs.fabric.microsoft.com/MDM_SilverStage_Layer.Lakehouse/Tables/D365/{silver_table_name}"
        # spark.read.format("delta").load(silver_table_path)

        clean_df = clean_df.astype(str)
        clean_spark_df = spark.createDataFrame(clean_df)
        clean_spark_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_table_path)

        print(f"✅ Cleaned data written to Silver Layer at {silver_table_path}")

        # ✅ Only record table if NO bad rows were found
        if not results:
            passed_validation_info = expectation_for_table[[
                "validation_rule", "AI_ColumnName", "AI_Reasoning"
            ]].drop_duplicates()

            record_passed_table_info(Stage_Table_Var, clean_df.shape[0], passed_validation_info)
            print(f"✅ Table {Stage_Table_Var} passed all DQ checks and logged to passed table")
        else:
            print(f"⚠️ Table {Stage_Table_Var} had failed rows, not logging to passed table")

    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import great_expectations as ge
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, upper
from datetime import datetime
import pytz
from uuid import uuid4
from pyspark.sql.functions import col, to_timestamp, year, when, lit
from pyspark.sql.types import TimestampType

spark = SparkSession.builder.getOrCreate()

# Reusable function to sanitize timestamp columns
# def sanitize_timestamps(df):
#     for col_name in df.columns:
#         if pd.api.types.is_datetime64_any_dtype(df[col_name]):
#             df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
#             df.loc[(df[col_name].dt.year < 1900) | (df[col_name].dt.year > 9999), col_name] = pd.NaT
#     return df

def sanitize_timestamps(df):
    for col_name, dtype in df.dtypes:
        if dtype == 'timestamp':
            # Coerce invalid timestamps to null
            df = df.withColumn(
                col_name,
                to_timestamp(col(col_name), 'yyyy-MM-dd HH:mm:ss')
            )
            # Set year out of bounds (<1900 or >9999) to null
            df = df.withColumn(
                col_name,
                when((year(col(col_name)) < 1900) | (year(col(col_name)) > 9999), lit(None).cast(TimestampType()))
                .otherwise(col(col_name))
            )
    return df

# Load validation execution master
expectations_df = (
    spark.read.format("delta")
    .load("abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_Layer.Lakehouse/Tables/D365/validation_execution_master")
    .filter((col("Selected") == True) | (upper(col("Selected")) == "TRUE"))
    .toPandas()
)

selected_tables = expectations_df['Table_Name'].unique().tolist()

context = ge.get_context()
data_source_name = "pandas"
data_source = context.data_sources.add_pandas(data_source_name)
data_assets_cache = {}

for Stage_Table_Var in selected_tables:
    try:
        print(f"Processing table: {Stage_Table_Var}")
        suite_name = f"{Stage_Table_Var}"
        suite = ge.ExpectationSuite(name=suite_name)
        suite = context.suites.add(suite)

        clean_df = spark.read.format("delta").load(
            f"abfss://Dynatech_MDM_Bronze@onelake.dfs.fabric.microsoft.com/MDM_Bronze_Layer.Lakehouse/Tables/D365/{Stage_Table_Var}"
        )

        non_null_cols = [c for c in clean_df.columns if clean_df.filter(f"{c} IS NOT NULL").limit(1).count() > 0]
        clean_df = clean_df.select(*non_null_cols)

        delete_flag_column = None
        for c in clean_df.columns:
            if 'delete_flag' in c.lower():
                delete_flag_column = c
                break

        if delete_flag_column:
            clean_df = clean_df.filter(col(delete_flag_column) == 0)

        clean_df = sanitize_timestamps(clean_df)
        clean_df = clean_df.toPandas()

        expectation_for_table = expectations_df[expectations_df['Table_Name'] == Stage_Table_Var]

        for _, row in expectation_for_table.iterrows():
            column_name = row['Column_Name'].strip()
            validation_rule = row['validation_rule'].strip()
            condition = row['Condition'].strip() if pd.notna(row['Condition']) else None
            min_range = row['Minimum_Range']
            max_range = row['Maximum_Range']
            index_key = row['Index_Key'].strip()
            ai_reasoning = row['AI_Reasoning'].strip() if pd.notna(row['AI_Reasoning']) else ""
            SampleData = row['SampleData'].strip() if pd.notna(condition) and pd.notna(row['SampleData']) else ""
            val_description = row['Val_Description'].strip()
            ai_columnname = row['AI_ColumnName'].strip()


            if validation_rule == 'expect_column_values_to_not_be_null':
                expectation = ge.expectations.ExpectColumnValuesToNotBeNull(column=column_name)

            elif validation_rule == 'expect_column_values_to_match_regex' and condition:
                expectation = ge.expectations.ExpectColumnValuesToMatchRegex(column=column_name, regex=condition)

            elif validation_rule == 'expect_column_values_to_not_match_regex' and condition:
                expectation = ge.expectations.ExpectColumnValuesToNotMatchRegex(column=column_name, regex=condition)

            elif validation_rule == 'expect_column_values_to_be_unique':
                expectation = ge.expectations.ExpectColumnValuesToBeUnique(column=column_name)

            elif validation_rule == 'expect_column_values_to_be_between':
                expectation = ge.expectations.ExpectColumnValuesToBeBetween(
                    column=column_name,
                    min_value=min_range,
                    max_value=max_range
                )

            elif validation_rule == 'expect_column_values_to_be_in_set' and condition:
                allowed_values = [val.strip() for val in condition.split(',')]
                expectation = ge.expectations.ExpectColumnValuesToBeInSet(
                    column=column_name,
                    value_set=allowed_values
                )

            elif validation_rule == 'expect_column_values_to_be_null_or_unique':
                expectation = ge.expectations.ExpectColumnValuesToBeNullOrUnique(column=column_name)

            elif validation_rule == 'expect_column_value_lengths_to_be_between':
                expectation = ge.expectations.ExpectColumnValueLengthsToBeBetween(
                    column=column_name,
                    min_value=min_range,
                    max_value=max_range
                )

            elif validation_rule == 'expect_column_distinct_values_to_be_in_set' and condition:
                allowed_values = [val.strip() for val in condition.split(',')]
                expectation = ge.expectations.ExpectColumnDistinctValuesToBeInSet(
                    column=column_name,
                    value_set=allowed_values
                )

            elif validation_rule == 'expect_column_values_to_be_null_or_not_null':
                not_null_count = clean_df[column_name].notnull().sum()
                null_count = clean_df[column_name].isnull().sum()
                if not_null_count > 0 and null_count > 0:
                    expectation = ge.expectations.ExpectColumnValuesToNotBeNull(column=column_name)

            else:
                continue

            suite.add_expectation(expectation)

        if not suite.expectations:
            print(f"No expectations for {Stage_Table_Var}, skipping...")
            continue

        if Stage_Table_Var not in data_assets_cache:
            data_asset_name = f"{Stage_Table_Var}_asset"
            data_asset = data_source.add_dataframe_asset(data_asset_name)
            data_assets_cache[Stage_Table_Var] = data_asset
        else:
            data_asset = data_assets_cache[Stage_Table_Var]

        data_asset.batch_definitions.clear()
        batch_definition = data_asset.add_batch_definition_whole_dataframe(f"batch_def_{uuid4().hex[:8]}")

        expectation_suite = context.suites.get(name=suite_name)
        validation_run_id = f"validation_run_{uuid4().hex[:8]}"
        validation_definition = ge.ValidationDefinition(
            data=batch_definition,
            suite=expectation_suite,
            name=validation_run_id
        )

        batch_parameters = {"dataframe": clean_df}
        validation_result = validation_definition.run(batch_parameters=batch_parameters, result_format={"result_format": "COMPLETE"})

        results = []
        for result in validation_result.to_json_dict()["results"]:
            exp_type = result["expectation_config"]["type"]
            col_name = result["expectation_config"]["kwargs"]["column"]
            for idx in result["result"].get("unexpected_index_list", []):
                index_key_value = clean_df.iloc[idx][index_key]  # Value from Index_Key column
                failed_value = clean_df.iloc[idx][col_name]
                results.append({
                    "Source_System": "D365",
                    "Table_Name": Stage_Table_Var,
                    "Column_Name": col_name,
                    'Index_Key': str(index_key_value),
                    "Failed_Value": str(failed_value),
                    "validation_rules": exp_type,
                    'Val_Description': str(val_description),
                    'AI_ColumnName': str(ai_columnname),
                    'ai_reasoning': str(ai_reasoning),
                    'SampleData' : str(SampleData),
                    "LoadTime_UTC": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"),
                    "LoadTime_PST": datetime.now().astimezone(pytz.timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S.%f")
                })

        if results:
            audit_df = pd.DataFrame(results).astype(str)

            schema = StructType([StructField(c, StringType(), True) for c in audit_df.columns])
            audit_spark_df = spark.createDataFrame(audit_df, schema=schema)

            audit_spark_df.write.option("mergeSchema", "true").mode("append").saveAsTable("D365.data_quality_audit_table_V1")

            print(f"Written audit data for {Stage_Table_Var}")

        # Write Clean Data to Silver Layer as String
        silver_table_name = f"SilverStage_{Stage_Table_Var}" if not Stage_Table_Var.lower().startswith("bronze_") else f"SilverStage_{Stage_Table_Var[7:]}"
        silver_table_path = f"abfss://Dynatech_MDM_Silver@onelake.dfs.fabric.microsoft.com/MDM_SilverStage_Layer.Lakehouse/Tables/D365/{silver_table_name}"

        clean_df = clean_df.astype(str)
        clean_spark_df = spark.createDataFrame(clean_df)
        clean_spark_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_table_path)

        print(f"Clean data written to Silver Layer: {silver_table_path}")

        print(f"Validated table {Stage_Table_Var}")

    except Exception as ex:
        print(f"Error processing {Stage_Table_Var}: {ex}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
