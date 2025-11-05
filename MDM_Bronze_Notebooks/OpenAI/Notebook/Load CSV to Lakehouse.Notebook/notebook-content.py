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
# META       "environmentId": "7a1d0185-84b6-4ef8-a6ed-64f2f5adb5c6",
# META       "workspaceId": "748f6814-d554-47e6-9011-2e7b63d72c1d"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
import os
import re
from collections import deque

spark = SparkSession.builder.getOrCreate()

# --- Configure: folders to scan (you can add/remove)
input_folders = [
    "Files/CRM/",
    "Files/FNO/",
    "Files/NETSUITE/"
  # included in case of typo/alternate path
]

# Optional: enable recursion into subfolders
RECURSE = True

# Helper: safely create schema if missing
def ensure_schema(schema_name: str):
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print(f"Schema '{schema_name}' ensured.")
        return True
    except Exception as e:
        print(f"❌ Could not create schema '{schema_name}': {e}")
        return False

# Helper: sanitize table name
def sanitize_table_name(name: str, schema_name: str) -> str:
    # drop schema prefix if user included it in filename like "FNO.Custtable"
    if name.upper().startswith(f"{schema_name}."):
        name = name[len(schema_name)+1:]
    # replace invalid chars with _
    name = re.sub(r'[^0-9a-zA-Z_]', '_', name)
    name = name.strip('_')
    if not name:
        name = "table_" + str(abs(hash(name)) % (10**6))
    return name.lower()

# Helper: list CSV files (optionally recursive)
def list_csv_files(start_path: str, recurse: bool = True):
    csv_paths = []
    try:
        queue = deque([start_path])
        while queue:
            path = queue.popleft()
            try:
                entries = mssparkutils.fs.ls(path)
            except Exception as ex:
                # cannot list path (maybe doesn't exist) -> skip
                # print a small warning and continue
                print(f"Warning: could not list '{path}': {ex}")
                continue

            for e in entries:
                # detect directories: many Fabric entries show paths ending with '/'
                if hasattr(e, 'path') and e.path.endswith('/'):
                    if recurse:
                        queue.append(e.path)
                    continue

                # some environments: name may end with '/'
                if hasattr(e, 'name') and e.name.endswith('/'):
                    if recurse:
                        queue.append(e.path)
                    continue

                # treat as file
                name = e.name if hasattr(e, 'name') else os.path.basename(e.path)
                if name.lower().endswith(".csv"):
                    csv_paths.append(e.path)
        return csv_paths
    except Exception as e:
        print(f"Error listing CSVs under {start_path}: {e}")
        return []

# --- Main loop: process each configured folder
all_found = 0
for folder in input_folders:
    # infer schema name from last path part (e.g., "Files/FNO" -> "FNO")
    schema_name = os.path.basename(folder.rstrip("/"))
    if not schema_name:
        print(f"Skipping invalid folder path '{folder}'")
        continue
    schema_name = schema_name.upper()

    print(f"\n--- Processing folder '{folder}' -> schema '{schema_name}' ---")
    # ensure schema exists
    ok = ensure_schema(schema_name)
    if not ok:
        print(f"Skipping folder '{folder}' because schema '{schema_name}' could not be ensured.")
        continue

    # list CSV files (recursive)
    csv_files = list_csv_files(folder, recurse=RECURSE)
    print(f"Found {len(csv_files)} CSV files in '{folder}' (including subfolders).")
    if len(csv_files) == 0:
        continue

    for file_path in csv_files:
        try:
            raw_filename = os.path.basename(file_path)
            base_name = os.path.splitext(raw_filename)[0]
            safe_name = sanitize_table_name(base_name, schema_name)
            table_full = f"{schema_name}.{safe_name}"

            print(f"\n📦 Loading file: {file_path}")
            print(f"   raw base name: '{base_name}' -> sanitized table: '{table_full}'")

            # read CSV (tweak options if you need different delimiter / quote / encoding)
            df = (spark.read
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv(file_path))

            # write to Delta table inside the schema
            df.write.mode("overwrite").format("delta").saveAsTable(table_full)

            # report row count (may be expensive for very large files; optional)
            try:
                rc = df.count()
                print(f"✅ Created table: {table_full} (rows: {rc})")
            except Exception:
                print(f"✅ Created table: {table_full} (row count skipped)")

            all_found += 1

        except Exception as ex:
            print(f"❌ Failed to load file '{file_path}': {ex}")

print(f"\n🎉 Done. Processed {all_found} file(s).")
print("You can inspect with: SHOW TABLES IN <SCHEMA>; e.g., SHOW TABLES IN FNO;")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
