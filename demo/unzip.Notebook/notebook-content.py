# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9e618e32-dbc2-48d7-9bd2-4beec24d9266",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "57d84005-cae9-4623-8753-45f36fb2c4cb",
# META       "known_lakehouses": [
# META         {
# META           "id": "9e618e32-dbc2-48d7-9bd2-4beec24d9266"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import zipfile

zip_path = "/lakehouse/default/Files/bronze/orders.zip"
extract_path = "/lakehouse/default/Files/bronze/"

with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall(extract_path)

# METADATA ********************
#adding comments

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
