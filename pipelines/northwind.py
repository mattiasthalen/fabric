import dlt
import os
import subprocess
import sys
import typing as t
import urllib

from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.rest_api import rest_api_resources

@dlt.source(name="northwind")
def northwind_source() -> t.Any:
    source_config: RESTAPIConfig = {
        "client": {
            "base_url": "https://demodata.grapecity.com/northwind/api/v1/",
        },
        "resource_defaults": {
            "write_disposition": "append",
            "max_table_nesting": 0
        },
        "resources": [
            {
                "name": "get_northwindapiv_1_categories",
                "table_name": "raw__northwind__categories",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Categories",
                    "paginator": "single_page",
                },
            },
            
            {
                "name": "get_northwindapiv_1_categoriesid_details",
                "table_name": "raw__northwind__category_details",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Categories/{id}/Details",
                    "params": {
                        "id": {
                            "type": "resolve",
                            "resource": "get_northwindapiv_1_categories",
                            "field": "categoryId",
                        },
                    },
                    "paginator": "single_page",
                },
            },
            
            {
                "name": "get_northwindapiv_1_customers",
                "table_name": "raw__northwind__customers",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Customers",
                    "paginator": "single_page",
                },
            },
            
            {
                "name": "get_northwindapiv_1_employees",
                "table_name": "raw__northwind__employees",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Employees",
                    "paginator": "single_page",
                },
            },

            {
                "name": "get_northwindapiv_1_employeesid_territories",
                "table_name": "raw__northwind__employee_territories",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Employees/{id}/Territories",
                    "params": {
                        "id": {
                            "type": "resolve",
                            "resource": "get_northwindapiv_1_employees",
                            "field": "employeeId",
                        },
                    },
                    "paginator": "single_page",
                },
                "include_from_parent": ["employeeId"]
            },
            
            {
                "name": "get_northwindapiv_1_ordersid_order_details",
                "table_name": "raw__northwind__order_details",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Orders/{id}/OrderDetails",
                    "params": {
                        "id": {
                            "type": "resolve",
                            "resource": "get_northwindapiv_1_orders",
                            "field": "orderId",
                        },
                    },
                    "paginator": "single_page",
                },
            },
            {
                "name": "get_northwindapiv_1_orders",
                "table_name": "raw__northwind__orders",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Orders",
                    "paginator": "single_page",
                },
            },
            
            {
                "name": "get_northwindapiv_1_products",
                "table_name": "raw__northwind__products",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Products",
                    "paginator": "single_page",
                },
            },
            
            {
                "name": "get_northwindapiv_1_regions",
                "table_name": "raw__northwind__regions",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Regions",
                    "paginator": "single_page",
                },
            },
           
            {
                "name": "get_northwindapiv_1_shippers",
                "table_name": "raw__northwind__shippers",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Shippers",
                    "paginator": "single_page",
                },
            },
           
            {
                "name": "get_northwindapiv_1_suppliers",
                "table_name": "raw__northwind__suppliers",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Suppliers",
                    "paginator": "single_page",
                },
            },
            
            {
                "name": "get_northwindapiv_1_territories",
                "table_name": "raw__northwind__territories",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Territories",
                    "paginator": "single_page",
                },
            }
            
        ],
    }

    yield from rest_api_resources(source_config)

def load_northwind(env) -> None:
    dev_mode = env != "prod"
    print(f"Running in {'dev' if dev_mode else 'prod'} mode")

    dataset_name = "northwind"

    if dev_mode:
        branch_name = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip().decode('utf-8')
        dataset_name = f"dev__{dataset_name}__{branch_name.replace('-', '_')}"

    workspace_id = os.getenv("FABRIC__WORKSPACE_ID")
    lakehouse_id = os.getenv("FABRIC__LAKEHOUSE_ID")

    bucket_url = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables"

    pipeline = dlt.pipeline(
        pipeline_name="northwind",
        destination=dlt.destinations.filesystem(bucket_url=bucket_url),
        dataset_name=dataset_name,
        progress="enlighten",
        export_schema_path="./pipelines/schemas/export",
        import_schema_path="./pipelines/schemas/import",
        dev_mode=dev_mode
    )

    source = northwind_source()
    
    load_info = pipeline.run(source, table_format="delta")
    print(load_info)

if __name__ == "__main__":
    env = sys.argv[1] if len(sys.argv) > 1 else "dev"
    
    os.environ["CREDENTIALS__AZURE_TENANT_ID"] = os.getenv("AZURE_TENANT_ID")
    os.environ["CREDENTIALS__AZURE_CLIENT_ID"] = os.getenv("AZURE_CLIENT_ID")
    os.environ["CREDENTIALS__AZURE_CLIENT_SECRET"] = os.getenv("AZURE_CLIENT_SECRET")
    os.environ["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = "onelake"
    os.environ["CREDENTIALS__AZURE_ACCOUNT_HOST"] = "onelake.blob.fabric.microsoft.com"

    load_northwind(env=env)