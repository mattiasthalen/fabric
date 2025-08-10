import getpass
import os
import subprocess
from dotenv import load_dotenv

from sqlmesh.core.config import (
    Config,
    ModelDefaultsConfig,
    GatewayConfig,
    FabricConnectionConfig,
    MSSQLConnectionConfig,
    NameInferenceConfig,
    CategorizerConfig,
    PlanConfig,
    AutoCategorizationMode
)
from sqlmesh.core.user import User, UserRole
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig, MergeMethod

load_dotenv(override=True)

azure__tenant_id = os.getenv("CREDENTIALS__AZURE_TENANT_ID", "Missing")
azure__client_id = os.getenv("CREDENTIALS__AZURE_CLIENT_ID", "Missing")
azure__client_secret = os.getenv("CREDENTIALS__AZURE_CLIENT_SECRET", "Missing")

fabric__workspace_id = os.getenv("FABRIC__WORKSPACE_ID", "Missing")
fabric__warehouse_endpoint = os.getenv("FABRIC__WAREHOUSE_ENDPOINT", "Missing")
fabric__database_endpoint = os.getenv("FABRIC__DATABASE_ENDPOINT", "Missing")
fabric__database_name = os.getenv("FABRIC__DATABASE", "Missing")

def get_current_branch():
    try:
        branch_name = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip().decode('utf-8')
        return branch_name
        
    except Exception as e:
        print(f"Error getting current branch: {e}")
        return None

branch = get_current_branch()
default_environment = f"dev__{branch}".replace('-', '_') if branch else "dev"

print(f"Environment is set to: {default_environment}.")

config = Config(
    project="analytical-data-storage-system",
    default_target_environment=default_environment,
    gateways={
        "fabric": GatewayConfig(
            connection=FabricConnectionConfig(
                host=fabric__warehouse_endpoint,
                user=azure__client_id,
                password=azure__client_secret,
                database="das",
                timeout=120,
                login_timeout=120,
                driver="pyodbc",
                driver_name="ODBC Driver 18 for SQL Server",
                tenant_id=azure__tenant_id,
                workspace_id=fabric__workspace_id,
                odbc_properties={
                    "Authentication": "ActiveDirectoryServicePrincipal"
                }
            ),
            state_connection=MSSQLConnectionConfig(
                host=fabric__database_endpoint,
                user=azure__client_id,
                password=azure__client_secret,
                database=fabric__database_name,
                timeout=120,
                login_timeout=120,
                driver="pyodbc",
                driver_name="ODBC Driver 18 for SQL Server",
                odbc_properties={
                    "Authentication": "ActiveDirectoryServicePrincipal"
                }
                
            )
        )
    },
    default_gateway="fabric",
    model_defaults=ModelDefaultsConfig(
        dialect="duckdb,normalization_strategy=case_sensitive",
        start="2025-05-09",
        cron="*/5 * * * *"
    ),
    model_naming=NameInferenceConfig(
        infer_names=True
    ),
    plan=PlanConfig(
        auto_categorize_changes=CategorizerConfig(
            external=AutoCategorizationMode.FULL,
            python=AutoCategorizationMode.FULL,
            sql=AutoCategorizationMode.FULL,
            seed=AutoCategorizationMode.FULL
        )
    ),
    variables = {
        "project_path": os.path.abspath(".").lstrip('/'),
        "min_date": "1970-01-01",
        "max_date": "9999-12-31",
        "min_ts": "1970-01-01 00:00:00",
        "max_ts": "9999-12-31 23:59:59"
    }
)