# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "13e742fe-590f-42d3-b62a-ccb7dd31b7e9",
# META       "default_lakehouse_name": "landing_zone",
# META       "default_lakehouse_workspace_id": "daba8301-ad97-44a1-a555-56cbe5bcd423",
# META       "known_lakehouses": [
# META         {
# META           "id": "13e742fe-590f-42d3-b62a-ccb7dd31b7e9"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "42a74380-f32a-9edd-4dae-147387133737",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # dlt & SQLMesh Runner

# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

env = "dev"

CODE_PATH = "/lakehouse/default/Files/code"
KEYVAULT = "https://mattiasthalen-fabric.vault.azure.net/"

CREDENTIALS__AZURE_TENANT_ID = None
CREDENTIALS__AZURE_CLIENT_ID = None
CREDENTIALS__AZURE_CLIENT_SECRET = None
CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME = "onelake"
CREDENTIALS__AZURE_ACCOUNT_HOST = "onelake.blob.fabric.microsoft.com"

FABRIC__WAREHOUSE_ENDPOINT = None
FABRIC__SQL_DB_ENDPOINT = None
FABRIC__SQL_DB_NAME = None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Load Libraries

# CELL ********************

!pip install "sqlmesh[fabric] @ git+https://github.com/mattiasthalen/sqlmesh.git@add-fabric-engine-adapter"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import notebookutils
import os
import subprocess

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Setup Environment

# CELL ********************

env_vars = {
    "CREDENTIALS__AZURE_CLIENT_ID": CREDENTIALS__AZURE_CLIENT_ID,
    "CREDENTIALS__AZURE_CLIENT_SECRET": CREDENTIALS__AZURE_CLIENT_SECRET,
    "FABRIC__WAREHOUSE_ENDPOINT": FABRIC__WAREHOUSE_ENDPOINT,
    "FABRIC__SQL_DB_ENDPOINT": FABRIC__SQL_DB_ENDPOINT,
    "FABRIC__SQL_DB_NAME": FABRIC__SQL_DB_NAME,
    "CODE_PATH": CODE_PATH,
}

for key, value in env_vars.items():

    if not value:
        secret = key.replace("_", "-")
        
        try:
            value = notebookutils.credentials.getSecret(KEYVAULT, secret)
        except:
            value = None

    if value:
        os.environ[key] = value

    print(f"{key} = {value}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Run dlt & SQLMesh

# CELL ********************

code_path = os.getenv("CODE_PATH")
project_path = os.path.join(code_path, "sqlmesh")

commands = [
    ["sqlmesh", "migrate"],
    ["sqlmesh", "plan", env, "--auto-apply"],
    ["sqlmesh", "run", env]
]

for cmd in commands:
    print(f"Executing: {' '.join(cmd)} in {project_path}")
    subprocess.run(cmd, check=True, cwd=project_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
