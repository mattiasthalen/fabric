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

# # Retrieve Codebase

# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

CODE_PATH = "/tmp/code"
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

from urllib.parse import urlparse

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
