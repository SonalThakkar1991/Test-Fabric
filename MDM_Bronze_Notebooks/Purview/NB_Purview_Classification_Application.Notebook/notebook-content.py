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

import requests
import pandas as pd
import msal
import json
import time

# Step 1: Load credentials from CSV file
cred_df = spark.read.table("Purview_Credentials").toPandas()
cred_dict = dict(zip(cred_df["KEY"], cred_df["VALUE"]))

# Assign variables
TENANT_ID = cred_dict["TENANT_ID"]
CLIENT_ID = cred_dict["CLIENT_ID"]
CLIENT_SECRET = cred_dict["CLIENT_SECRET"]
PURVIEW_NAME = cred_dict["PURVIEW_NAME"]


# Step 2: Configure Authentication
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://purview.azure.net/.default"]

def get_access_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" in result:
        return result["access_token"]
    else:
        print("Token acquisition failed:")
        print(json.dumps(result, indent=2))
        raise Exception("Access token not returned")

def search_table_by_name(access_token, table_name):
    url = f"https://{PURVIEW_NAME}.purview.azure.com/datamap/api/search/query?api-version=2023-09-01"
    headers = {"Authorization": f"Bearer {access_token}"}
    body = {
        "keywords": table_name,
        "limit": 25,
        "entityTypes": ["fabric_lakehouse_table"]
    }
    response = requests.post(url, headers=headers, json=body)
    if response.ok:
        data = response.json()
        for item in data.get("value", []):
            if item["name"].strip().lower() == table_name.lower():
                print(f"Found table: {table_name} | QualifiedName: {item['qualifiedName']}")
                return item["qualifiedName"], item["id"]
        print(f"No exact match found for table: '{table_name}'")
    else:
        print(f"Search failed ({response.status_code}): {response.text}")
    return None, None

def get_column_qualified_name_map(access_token, table_guid):
    url = f"https://{PURVIEW_NAME}.purview.azure.com/datamap/api/atlas/v2/entity/guid/{table_guid}?api-version=2023-09-01"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    column_map = {}
    if response.ok:
        entity = response.json()
        referred = entity.get("referredEntities", {})
        for guid, ref in referred.items():
            if ref.get("typeName") == "fabric_lakehouse_table_column":
                col_name = ref.get("displayText", "").strip().lower()
                column_map[col_name] = guid
        print(f"Found {len(column_map)} columns.")
    else:
        print(f"Failed to get column details: {response.status_code} - {response.text}")
    return column_map

def get_existing_classifications(access_token, entity_guid):
    url = f"https://{PURVIEW_NAME}.purview.azure.com/datamap/api/atlas/v2/entity/guid/{entity_guid}/classifications?api-version=2023-09-01"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    classifications = []
    if response.ok:
        try:
            data = response.json()
            if isinstance(data, list):
                classifications = [c.get("typeName") for c in data if isinstance(c, dict)]
            elif isinstance(data, dict) and "list" in data:
                classifications = [c.get("typeName") for c in data["list"] if isinstance(c, dict)]
        except Exception as e:
            print(f"Error parsing classifications: {e}")
    else:
        print(f"Error fetching classifications: {response.status_code} - {response.text}")
    return classifications

def apply_classification(access_token, column_guid, desired_classification, table_name, col_name):
    current_classifications = get_existing_classifications(access_token, column_guid)
    if desired_classification in current_classifications:
        print(f"Classification '{desired_classification}' already exists. Skipping...")
        return False
    url = f"https://{PURVIEW_NAME}.purview.azure.com/datamap/api/atlas/v2/entity/guid/{column_guid}/classifications?api-version=2023-09-01"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = [{"typeName": desired_classification}]
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 204:
        print(f"Classification '{desired_classification}' applied on Column '{table_name}'.'{col_name}'.")
        return True
    else:
        print(f"Failed to apply classification: Classification Not present")
        return False

# Main Execution
def main():
    access_token = get_access_token()

    # Read Excel
    df = spark.read.table("MDM_Bronze_Layer.dbo.Master_Column_classifications").toPandas()
    df.columns = df.columns.str.strip()

    required_cols = ['Table_Name', 'column_name', 'classification']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Input must contain columns: {required_cols}")

    grouped = df.groupby('Table_Name')
    for table_name, group in grouped:
        print(f"\nProcessing Table = '{table_name}'")
        qualified_name, table_guid = search_table_by_name(access_token, table_name)
        if not qualified_name or not table_guid:
            print(f"Skipping table: {table_name}")
            continue

        column_map = get_column_qualified_name_map(access_token, table_guid)
        if not column_map:
            print("No columns found.")
            continue

        for _, row in group.iterrows():
            col_name = str(row['column_name']).strip().lower()
            desired_classification = str(row['classification']).strip()
            col_guid = column_map.get(col_name)
            if not col_guid:
                print(f"Column '{col_name}' not found in table '{table_name}'")
                continue

            print(f"Processing column '{col_name}' for classification '{desired_classification}'")
            apply_classification(access_token, col_guid, desired_classification, table_name, col_name)
            time.sleep(1)  # avoid throttling

# Starting Point
if __name__ == '__main__':
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
