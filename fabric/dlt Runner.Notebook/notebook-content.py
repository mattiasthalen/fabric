# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     },
# META     "environment": {
# META       "environmentId": "42a74380-f32a-9edd-4dae-147387133737",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # dlt Runner

# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

dlt_pipelines = ["dlt/northwind/northwind.py"]
env = "dev"

CODE_PATH = "/tmp/code"
KEYVAULT = "https://mattiasthalen-fabric.vault.azure.net/"

CREDENTIALS__AZURE_TENANT_ID = None
CREDENTIALS__AZURE_CLIENT_ID = None
CREDENTIALS__AZURE_CLIENT_SECRET = None
CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME = "onelake"
CREDENTIALS__AZURE_ACCOUNT_HOST = "onelake.blob.fabric.microsoft.com"

DESTINATION__BUCKET_URL = "/lakehouse/default/Tables"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Load Libraries

# CELL ********************

!pip install "dlt[deltalake,filesystem,parquet]>=1.12.1"
!pip install "enlighten>=1.14.1"

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

# ## Helper Functions

# CELL ********************

def run_subprocess(commands, cwd=None):
    if not isinstance(commands[0], (list, tuple)):
        commands = [commands]

    for command in commands:
        print(f"Executing subprocess: {command}")
        result = subprocess.run(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE, 
            text=True,
            cwd=cwd
        )

        if result.returncode != 0:
            print(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Setup Environment

# CELL ********************

env_vars = {
    "CODE_PATH": CODE_PATH,
    "CREDENTIALS__AZURE_TENANT_ID": CREDENTIALS__AZURE_TENANT_ID,
    "CREDENTIALS__AZURE_CLIENT_ID": CREDENTIALS__AZURE_CLIENT_ID,
    "CREDENTIALS__AZURE_CLIENT_SECRET": CREDENTIALS__AZURE_CLIENT_SECRET,
    "CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME": CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME,
    "CREDENTIALS__AZURE_ACCOUNT_HOST": CREDENTIALS__AZURE_ACCOUNT_HOST,
    "DESTINATION__BUCKET_URL": DESTINATION__BUCKET_URL,
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

# ## Run dlt

# CELL ********************

notebookutils.notebook.run("Retrieve Codebase")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

code_path = os.getenv("CODE_PATH")

if not isinstance(dlt_pipelines, list):
    dlt_pipelines = [dlt_pipelines]

commands = []
for pipeline in dlt_pipelines:
    commands.append(["python", pipeline, env])

run_subprocess(commands, code_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
