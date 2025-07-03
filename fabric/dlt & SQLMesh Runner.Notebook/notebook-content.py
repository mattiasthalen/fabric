# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # dlt & SQLMesh Runner

# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

dlt_pipelines = ["dlt/northwind/northwind.py"]
env = "prod"
command = "elt"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Environment Variables

# CELL ********************

import notebookutils
import os

os.environ["CREDENTIALS__AZURE_TENANT_ID"] = notebookutils.variableLibrary.get("$(/**/Secrets/CREDENTIALS__AZURE_TENANT_ID)")
os.environ["CREDENTIALS__AZURE_CLIENT_ID"] = notebookutils.variableLibrary.get("$(/**/Secrets/CREDENTIALS__AZURE_CLIENT_ID)")
os.environ["CREDENTIALS__AZURE_CLIENT_SECRET"] = notebookutils.variableLibrary.get("$(/**/Secrets/CREDENTIALS__AZURE_CLIENT_SECRET)")
os.environ["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = notebookutils.variableLibrary.get("$(/**/Secrets/CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME)")
os.environ["CREDENTIALS__AZURE_ACCOUNT_HOST"] = notebookutils.variableLibrary.get("$(/**/Secrets/CREDENTIALS__AZURE_ACCOUNT_HOST)")

os.environ["FABRIC__WAREHOUSE_ENDPOINT"] = notebookutils.variableLibrary.get("$(/**/Secrets/FABRIC__WAREHOUSE_ENDPOINT)")
os.environ["FABRIC__SQL_DB_ENDPOINT"] = notebookutils.variableLibrary.get("$(/**/Secrets/FABRIC__SQL_DB_ENDPOINT)")
os.environ["FABRIC__SQL_DB_NAME"] = notebookutils.variableLibrary.get("$(/**/Secrets/FABRIC__SQL_DB_NAME)")

os.environ["DESTINATION__BUCKET_URL"] = notebookutils.variableLibrary.get("$(/**/Secrets/DESTINATION__BUCKET_URL)")

os.getenv("DESTINATION__BUCKET_URL")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Clone Repo

# CELL ********************

import shutil
import os
import subprocess

repo_dir = "/tmp/Code"
repo_url = "https://github.com/mattiasthalen/fabric"

if os.path.isdir(repo_dir):
    shutil.rmtree(repo_dir)

os.makedirs(repo_dir)
os.chdir(repo_dir)
!git clone {repo_url} {repo_dir}

os.listdir(repo_dir)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Install Requirements

# CELL ********************

if command.startswith("el"):
    !pip install "dlt[deltalake,filesystem,parquet]>=1.12.1"
    !pip install "enlighten>=1.14.1"

if command.endswith("t"):
    !pip install "sqlmesh[mssql-odbc] @ git+https://github.com/fresioAS/sqlmesh.git@add_fabric_warehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Run dlt & SQLMesh

# CELL ********************

if command.startswith("el"):
    for pipeline in dlt_pipelines:
        !python {repo_dir}/{pipeline} {env}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if command.endswith("t"):
    !sqlmesh -p {repo_dir}/sqlmesh run {env}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
