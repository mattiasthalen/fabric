import getpass
import os
import subprocess

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

azure_client_id = os.getenv("AZURE_CLIENT_ID")
azure_client_secret = os.getenv("AZURE_CLIENT_SECRET")

fabric__warehouse_endpoint = os.getenv("FABRIC__WAREHOUSE_ENDPOINT")
fabric__sql_db_endpoint = os.getenv("FABRIC__SQL_DB_ENDPOINT")
fabric__sql_db_name = os.getenv("FABRIC__SQL_DB_NAME")

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
    project="adventure-works",
    default_target_environment=default_environment,
    gateways={
        "fabric": GatewayConfig(
            connection=FabricConnectionConfig(
                host=fabric__warehouse_endpoint,
                user=azure_client_id,
                password=azure_client_secret,
                database="das",
                timeout=120,
                login_timeout=120,
                driver="pyodbc",
                driver_name="ODBC Driver 18 for SQL Server",
                odbc_properties={
                    "Authentication": "ActiveDirectoryServicePrincipal"
                }
            ),
            state_connection=MSSQLConnectionConfig(
                host=fabric__sql_db_endpoint,
                user=azure_client_id,
                password=azure_client_secret,
                database=fabric__sql_db_name,
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