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

GIT__URL = "https://dev.azure.com/mattiasthalen/fabric/_git/adss.git"
GIT__USER = None
GIT__TOKEN = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Load Requirements

# CELL ********************

import io
import notebookutils
import os
import requests
import shutil
import subprocess
import zipfile

from urllib.parse import urlparse

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
    "GIT__URL": GIT__URL,
    "GIT__USER": GIT__USER,
    "GIT__TOKEN": GIT__TOKEN
}

keyvault = "https://mattiasthalen-fabric.vault.azure.net/"

for key, value in env_vars.items():

    if not value:
        secret = key.replace("_", "-")
        
        try:
            value = notebookutils.credentials.getSecret(keyvault, secret)
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

# ## Clone Repo

# CELL ********************

def get_devops_zip_url(repo_url, branch="main"):
    """
    Constructs the Azure DevOps API zip download URL from a standard repository URL.

    Args:
        repo_url (str): The Azure DevOps repository URL (e.g., "https://dev.azure.com/org/project/_git/repo").
        branch (str, optional): The branch to download. Defaults to "main".

    Returns:
        str: The API endpoint to download the specified branch as a zip.

    Raises:
        ValueError: If the repo_url format is invalid.
    """
    parsed = urlparse(repo_url)
    path_parts = parsed.path.strip("/").split("/")

    if len(path_parts) < 4:
        raise ValueError("Invalid Azure DevOps repo URL format")

    org, project, _, repo = path_parts

    return (
        f"https://dev.azure.com/{org}/{project}/_apis/git/repositories/"
        f"{repo}/items?scopePath=/&$format=zip&download=true&versionDescriptor.version={branch}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def get_github_zip_url(repo_url, branch="main"):
    """
    Constructs the GitHub API zip download URL from a standard repository URL.

    Args:
        repo_url (str): The GitHub repository URL (e.g., "https://github.com/owner/repo").
        branch (str, optional): The branch to download. Defaults to "main".

    Returns:
        str: The API endpoint to download the specified branch as a zip.

    Raises:
        ValueError: If the repo_url format is invalid.
    """
    parsed = urlparse(repo_url)
    path_parts = parsed.path.strip("/").split("/")

    if len(path_parts) < 2:
        raise ValueError("Invalid GitHub repo URL format")

    owner, repo = path_parts[:2]
    repo = repo.replace(".git", "")

    return f"https://api.github.com/repos/{owner}/{repo}/zipball/{branch}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def get_zip_url(repo_url, branch="main"):
    """
    Determines the platform from the repo URL and dispatches to the appropriate zip download URL constructor.

    Args:
        repo_url (str): The repository URL (GitHub or Azure DevOps).
        branch (str, optional): The branch to download. Defaults to "main".

    Returns:
        str: The constructed zip download URL.

    Raises:
        ValueError: If the platform in repo_url is unknown.
    """
    parsed = urlparse(repo_url)

    if "dev.azure.com" in parsed.netloc:
        return get_devops_zip_url(repo_url, branch)

    elif "github.com" in parsed.netloc:
        return get_github_zip_url(repo_url, branch)

    else:
        raise ValueError("Unknown platform in repo_url.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def fetch_zip_bytes(zip_url, username, token):
    """
    Downloads the zip archive from the specified URL using basic authentication.

    Args:
        zip_url (str): The zip download URL.
        username (str): Username or login (GitHub or Azure DevOps).
        token (str): Personal Access Token (PAT) for authentication.

    Returns:
        bytes: The zip file contents as bytes.

    Raises:
        requests.HTTPError: If the HTTP request fails.
    """
    resp = requests.get(zip_url, auth=(username, token))
    resp.raise_for_status()

    return resp.content

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def extract_zip_bytes(zip_bytes, code_path):
    """
    Extracts the contents of a zip archive (provided as bytes) to a directory.

    Args:
        zip_bytes (bytes): The bytes of the zip archive.
        code_path (str): The path to extract files into.

    Returns:
        list of str: List of top-level entries (files/directories) in code_path after extraction.

    Side effects:
        - Overwrites/deletes existing code_path directory if it exists.
    """
    if os.path.isdir(code_path):
        shutil.rmtree(code_path)

    os.makedirs(code_path, exist_ok=True)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        zf.extractall(code_path)

    return os.listdir(code_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def flatten_single_root_dir(code_path):
    """
    If code_path contains a single directory, moves all its contents up one level and removes the directory.

    Args:
        code_path (str): The path to check and flatten if necessary.

    Returns:
        list of str: All file paths (recursively) in code_path after flattening.
    """
    entries = os.listdir(code_path)

    if len(entries) == 1:
        root_dir = os.path.join(code_path, entries[0])
        if os.path.isdir(root_dir):
            for item in os.listdir(root_dir):
                shutil.move(os.path.join(root_dir, item), code_path)
            os.rmdir(root_dir)

    # Return all files recursively for convenience
    return [
        os.path.join(root, name)
        for root, dirs, files in os.walk(code_path)
        for name in files
    ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def extract_zip_bytes_to_dir(zip_bytes, code_path):
    """
    Pipeline: Extracts zip bytes to directory and flattens single-root-directory structure if present.

    Args:
        zip_bytes (bytes): The bytes of the zip archive.
        code_path (str): The path to extract files into.

    Returns:
        list of str: All file paths (recursively) in code_path after extraction and flattening.
    """
    extract_zip_bytes(zip_bytes, code_path)
    return flatten_single_root_dir(code_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def download_zip_and_extract(repo_url, username, token, code_path, branch="main"):
    """
    Downloads a branch as a zip archive from a GitHub or Azure DevOps repo, extracts it, and flattens the directory if needed.

    Args:
        repo_url (str): The repository URL (GitHub or Azure DevOps).
        username (str): Username or login (GitHub or Azure DevOps).
        token (str): Personal Access Token (PAT) for authentication.
        code_path (str): The directory to extract the archive to.
        branch (str, optional): The branch to download. Defaults to "main".

    Returns:
        list of str: All file paths (recursively) in code_path after extraction and flattening.
    """
    zip_url = get_zip_url(repo_url, branch)
    zip_bytes = fetch_zip_bytes(zip_url, username, token)
    return extract_zip_bytes_to_dir(zip_bytes, code_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

code_path = "/tmp/code"
files = download_zip_and_extract(
    repo_url=os.getenv("GIT__URL"),
    username=os.getenv("GIT__USER"),
    token=os.getenv("GIT__TOKEN"),
    code_path=code_path
)

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
