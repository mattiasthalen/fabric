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

# # dlt & SQLMesh Runner

# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

dlt_pipelines = ["dlt/northwind/northwind.py"]
env = "dev"
command = "elt"

CREDENTIALS__AZURE_TENANT_ID = None
CREDENTIALS__AZURE_CLIENT_ID = None
CREDENTIALS__AZURE_CLIENT_SECRET = None
CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME = "onelake"
CREDENTIALS__AZURE_ACCOUNT_HOST = "onelake.blob.fabric.microsoft.com"
FABRIC__WAREHOUSE_ENDPOINT = None
FABRIC__SQL_DB_ENDPOINT = None
FABRIC__SQL_DB_NAME = None
DESTINATION__BUCKET_URL = "/lakehouse/default/Tables"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Load Requirements

# CELL ********************

import notebookutils
import os
import shutil
import subprocess

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Helper Functions

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

# CELL ********************

def install_packages(packages):
    commands = []

    for package in packages:
        commands.append(["pip", "install", package])

    run_subprocess(commands)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def list_directory_contents(path):
    """
    Lists the contents of a directory, separating directories from files.
    
    Args:
        path (str): Path to the directory to list
        
    Returns:
        dict: A dictionary with 'directories' and 'files' keys
    """
    all_entries = os.listdir(path)
    
    directories = []
    files = []
    
    for entry in all_entries:
        full_path = os.path.join(path, entry)
        if os.path.isdir(full_path):
            directories.append(entry)
        else:
            files.append(entry)

    return [*sorted(directories), *sorted(files)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Setup Environment Variables

# CELL ********************

env_vars = {
    "CREDENTIALS__AZURE_TENANT_ID": CREDENTIALS__AZURE_TENANT_ID,
    "CREDENTIALS__AZURE_CLIENT_ID": CREDENTIALS__AZURE_CLIENT_ID,
    "CREDENTIALS__AZURE_CLIENT_SECRET": CREDENTIALS__AZURE_CLIENT_SECRET,
    "CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME": CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME,
    "CREDENTIALS__AZURE_ACCOUNT_HOST": CREDENTIALS__AZURE_ACCOUNT_HOST,
    "FABRIC__WAREHOUSE_ENDPOINT": FABRIC__WAREHOUSE_ENDPOINT,
    "FABRIC__SQL_DB_ENDPOINT": FABRIC__SQL_DB_ENDPOINT,
    "FABRIC__SQL_DB_NAME": FABRIC__SQL_DB_NAME,
    "DESTINATION__BUCKET_URL": DESTINATION__BUCKET_URL,
}

keyvault = "https://mattiasthalen-fabric.vault.azure.net/"

for key, value in env_vars.items():

    if not value:
        secret = key.replace("_", "-")
        value = notebookutils.credentials.getSecret(keyvault, secret)

    os.environ[key] = value
    print(f"{key} = {os.getenv(key)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Clone Repo

# CELL ********************

code_path = "/tmp/code"
repo_url = "https://github.com/mattiasthalen/fabric"

# Trunctate the repo dir
if os.path.isdir(code_path):
    shutil.rmtree(code_path)

# Create a shallow clone
run_subprocess(["git", "clone", "--depth", "1", repo_url, code_path])

list_directory_contents(code_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Run dlt & SQLMesh

# CELL ********************

def extract_and_load(pipelines, cwd, line_width = 80):
    print("EXTRACTING & LOADING".center(line_width, "="))

    install_packages([
        "dlt[deltalake,filesystem,parquet]>=1.12.1",
        "enlighten>=1.14.1"
    ])

    commands = []

    for pipeline in pipelines:
        commands.append(["python", pipeline, env])

    run_subprocess(commands, cwd)   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def transform(cwd, line_width = 80):
    print("TRANSFORMING".center(line_width, "="))

    install_packages([
        "sqlmesh[fabric] @ git+https://github.com/mattiasthalen/sqlmesh.git@add-fabric-engine-adapter"
    ])

    project_path = os.path.join(cwd, "sqlmesh")
    commands = [
        ["sqlmesh", "migrate"],
        ["sqlmesh", "plan", env, "--auto-apply"],
        ["sqlmesh", "run", env]
    ]

    run_subprocess(commands, cwd=project_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if command.startswith("el"):
    extract_and_load(dlt_pipelines, code_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if command.endswith("t"):
    transform(code_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
