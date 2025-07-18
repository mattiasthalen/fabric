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

# # Retrieve Codebase

# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

CODE_PATH = "/lakehouse/default/Files/code"
KEYVAULT = "https://mattiasthalen-fabric.vault.azure.net/"
GIT__URL = "https://dev.azure.com/mattiasthalen/fabric/_git/adss.git"
GIT__USER = None
GIT__TOKEN = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Load Libraries

# CELL ********************

import io
import notebookutils
import os
import requests
import shutil
import subprocess
import zipfile

from urllib.parse import urlparse, urlunparse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Helper Functions

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

# ## Setup Environment

# CELL ********************

env_vars = {
    "CODE_PATH": CODE_PATH,
    "GIT__URL": GIT__URL,
    "GIT__USER": GIT__USER,
    "GIT__TOKEN": GIT__TOKEN,
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

# ## Retrieve Codebase

# CELL ********************

code_path = os.getenv("CODE_PATH")
repo_url=os.getenv("GIT__URL")
username=os.getenv("GIT__USER")
token=os.getenv("GIT__TOKEN")

parsed_url = urlparse(repo_url)
netloc = f"{username}:{token}@{parsed_url.netloc}"
url_with_auth = urlunparse(parsed_url._replace(netloc=netloc))

if os.path.isdir(code_path) and os.path.isdir(os.path.join(code_path, ".git")):
    # Directory exists and looks like a git repo: pull latest changes
    cmd = ["git", "pull"]
    print(f"Pulling latest changes in {code_path}...")
    subprocess.run(cmd, check=True, cwd=code_path)

else:
    # Directory doesn't exist or is not a git repo: do a shallow clone
    cmd = [
        "git", "clone",
        "--branch", "main",
        "--depth", "1",
        url_with_auth,
        code_path
    ]
    print(f"Cloning into {code_path}...")
    subprocess.run(cmd, check=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

list_directory_contents(code_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
