# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "439bdb2b-2975-9723-4db7-4d263c28a419",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

%pip install dotenv

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from azure.identity import DefaultAzureCredential
# from azure.keyvault.secrets import SecretClient

# key_vault_name = "dtmdmvault"
# secret_name = "MDMSecretKey"

# KVUri = f"https://{key_vault_name}.vault.azure.net"

# credential = DefaultAzureCredential()
# client = SecretClient(vault_url=KVUri, credential=credential)


# print(client)
from dotenv import load_dotenv
import os

# Load the .env file from the environment resource
dotenv_path = "./env/env"
load_dotenv(dotenv_path)  # This automatically loads .env if it's in the working directory

key = os.getenv("key")

# Access an environment variable
# api_key = os.getenv("key")  # Returns None if the variable is not set

# secret_value = client.get_secret(secret_name).value
print(key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Not supported

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dir(client.get_secret(secret_name))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
