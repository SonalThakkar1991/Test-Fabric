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

import pandas as pd
import requests
import msal
import json

# --- Step 1: Load Purview credentials from Fabric Lakehouse table ---
cred_spark_df = spark.read.table("Purview_Credentials")  # Table with columns: KEY, VALUE
cred_pd_df = cred_spark_df.toPandas()
cred_dict = dict(zip(cred_pd_df["KEY"], cred_pd_df["VALUE"]))

client_id = cred_dict["CLIENT_ID"]
client_secret = cred_dict["CLIENT_SECRET"]
tenant_id = cred_dict["TENANT_ID"]
authority = f"https://login.microsoftonline.com/{tenant_id}"

# --- Step 2: Purview endpoints ---
purview_endpoint = f"https://{tenant_id}-api.purview-service.microsoft.com"
cde_create_url = f"{purview_endpoint}/datagovernance/catalog/criticalDataElements"
cde_search_url = f"{purview_endpoint}/datagovernance/catalog/criticalDataElements/query"
business_domains_url = f"{purview_endpoint}/datagovernance/catalog/businessdomains"

# --- Step 3: Authenticate using MSAL ---
app = msal.ConfidentialClientApplication(
    client_id,
    authority=authority,
    client_credential=client_secret
)

scope = ["https://purview.azure.net/.default"]
token_result = app.acquire_token_for_client(scopes=scope)

if "access_token" not in token_result:
    raise Exception("Fail Message: Authentication failed")

access_token = token_result["access_token"]
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# --- Step 4: Get domain mapping from Purview ---
resp = requests.get(business_domains_url, headers=headers)
if resp.status_code != 200:
    raise Exception(f"Fail Message: Failed to get domains: {resp.status_code} — {resp.text}")

domains = resp.json().get("value", [])
domain_map = {d["name"].strip().lower(): d["id"] for d in domains}

# --- Step 5: Load AAD owner mapping from Fabric table ---
owner_df = spark.read.table("AAD_User_Details").toPandas()  # Table with Users_Email, ID
email_to_owner_id = dict(zip(owner_df["Users_Email"], owner_df["ID"]))

# --- Step 6: Read control table from Fabric Lakehouse ---
df_spark = spark.read.table("dbo.CDE_Data")  # Table must exist in Lakehouse
df = df_spark.toPandas()

# --- Step 7: Process each CDE ---
for idx, row in df.iterrows():
    domain_name = str(row['Governance_Domain_Name']).strip().lower()
    domain_id = domain_map.get(domain_name)

    if not domain_id:
        print(f"Skipping CDE: Row {idx+2}: Domain '{domain_name}' not found. Skipping.")
        continue

    name = str(row['CDE_Name']).strip()
    description = f"<div>{row['CDE_Description']}</div>"
    data_type = row['CDE_Expected_data_type']
    status = row['Status']
    owner_email = row['CDE_Owner']
    owner_id = email_to_owner_id.get(owner_email)

    if not owner_id:
        print(f"Skipping CDE: Row {idx+2}: Owner email '{owner_email}' not found. Skipping.")
        continue

    # --- Step 7.1: Search for existing CDE ---
    search_payload = {
        "filter": {
            "and": [
                { "attributeName": "name", "operator": "eq", "value": name }
            ]
        }
    }
    search_resp = requests.post(cde_search_url, headers=headers, json=search_payload)

    if search_resp.status_code != 200:
        print(f"Fail Message: Row {idx+2}: Failed CDE search for '{name}' — {search_resp.status_code} — {search_resp.text}")
        continue

    existing = search_resp.json().get("value", [])
    already_exists = any(
        cde.get("name", "").strip().lower() == name.lower() and
        cde.get("domain", "").strip().lower() == domain_id.lower()
        for cde in existing
    )

    if already_exists:
        print(f"Row {idx+2}: CDE '{name}' already exists in domain '{domain_name}'. Skipping.")
        continue

    # --- Step 7.2: Create the new CDE ---
    create_payload = {
        "name": name,
        "description": description,
        "dataType": data_type,
        "status": status,
        "ignoreDuplicateWarning": True,
        "contacts": {
            "owner": [{ "id": owner_id }]
        },
        "domain": domain_id
    }

    create_resp = requests.post(cde_create_url, headers=headers, json=create_payload)

    if create_resp.status_code in [200, 201]:
        print(f"Row {idx+2}: Created CDE '{name}' in domain '{domain_name}'")
    else:
        print(f"Row {idx+2}: Failed to create CDE '{name}' — {create_resp.status_code} — {create_resp.text}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
