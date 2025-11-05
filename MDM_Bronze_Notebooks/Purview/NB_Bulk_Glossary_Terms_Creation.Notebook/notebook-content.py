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
cred_df = spark.read.table("Purview_Credentials").toPandas()
cred_dict = dict(zip(cred_df["KEY"], cred_df["VALUE"]))

client_id = cred_dict["CLIENT_ID"]
client_secret = cred_dict["CLIENT_SECRET"]
tenant_id = cred_dict["TENANT_ID"]
authority = f"https://login.microsoftonline.com/{tenant_id}"

# --- Step 2: Purview endpoints ---
purview_endpoint = f"https://{tenant_id}-api.purview-service.microsoft.com"
terms_url = f"{purview_endpoint}/datagovernance/catalog/terms"
terms_query_url = f"{purview_endpoint}/datagovernance/catalog/terms/query"
business_domains_url = f"{purview_endpoint}/datagovernance/catalog/businessdomains"

# --- Step 3: Authenticate using MSAL ---
app = msal.ConfidentialClientApplication(
    client_id,
    authority=authority,
    client_credential=client_secret
)

token_result = app.acquire_token_for_client(scopes=["https://purview.azure.net/.default"])
if "access_token" not in token_result:
    raise Exception("Authentication failed")

access_token = token_result["access_token"]
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# --- Step 4: Get domain mapping from Purview ---
resp = requests.get(business_domains_url, headers=headers)
if resp.status_code != 200:
    raise Exception(f"Failed to retrieve domains: {resp.status_code} — {resp.text}")

domains = resp.json().get("value", [])
domain_map = {d["name"].strip().lower(): d["id"] for d in domains}

# --- Step 5: Load AAD owner mapping from Fabric table ---
owner_df = spark.read.table("AAD_User_Details").toPandas()
email_to_owner_id = dict(zip(owner_df["Users_Email"], owner_df["ID"]))

# --- Step 6: Load Glossary data from Fabric table ---
glossary_df = spark.read.table("Glossary_Data").toPandas()

# --- Step 7: Process each glossary term ---
for idx, row in glossary_df.iterrows():
    domain_name = str(row["Governance_Domain_Name"]).strip().lower()
    term_name = str(row["Term_Name"]).strip()
    term_desc = str(row["Term_Description"]).strip()
    owner_email = str(row["Term_Owner"]).strip()
    status = str(row["Status"]).strip()

    domain_id = domain_map.get(domain_name)
    if not domain_id:
        print(f"Row {idx+2}: Domain '{domain_name}' not found. Skipping.")
        continue

    owner_id = email_to_owner_id.get(owner_email)
    if not owner_id:
        print(f"Row {idx+2}: Owner email '{owner_email}' not found. Skipping.")
        continue

    # --- Step 7.1: Check if term already exists in domain ---
    search_payload = {
        "domainIds": [domain_id],
        "nameKeyword": term_name,
        "skip": 0,
        "top": 25,
        "orderBy": [{"field": "name", "direction": "asc"}]
    }

    search_resp = requests.post(terms_query_url, headers=headers, json=search_payload)
    if search_resp.status_code != 200:
        print(f"Row {idx+2}: Failed to query terms — {search_resp.status_code}")
        continue

    existing_terms = search_resp.json().get("value", [])
    exists = any(
        term.get("name", "").strip().lower() == term_name.lower()
        for term in existing_terms
    )

    if exists:
        print(f"Row {idx+2}: Term '{term_name}' already exists in domain '{domain_name}'. Skipping.")
        continue

    # --- Step 7.2: Create the glossary term ---
    create_payload = {
        "name": term_name,
        "description": f"<div>{term_desc}</div>",
        "domain": domain_id,
        "status": status,
        "contacts": {
            "owner": [{"id": owner_id}]
        }
    }

    create_resp = requests.post(terms_url, headers=headers, json=create_payload)
    if create_resp.status_code in [200, 201]:
        print(f"Row {idx+2}: Created term '{term_name}' in domain '{domain_name}'")
    else:
        print(f"Row {idx+2}: Failed to create term '{term_name}' — {create_resp.status_code} — {create_resp.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
