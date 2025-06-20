import dlt
import os
import sys
import typing as t
import urllib

from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.rest_api import rest_api_resources

@dlt.source(name="northwind")
def northwind_source() -> t.Any:
    source_config: RESTAPIConfig = {
        "client": {
            "base_url": "https://demodata.grapecity.com",
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "max_table_nesting": 0
        },
        "resources": [
            {
                "name": "get_northwindapiv_1_categories",
                "table_name": "raw__northwind__categories",
                "primary_key": "categoryId",
                "endpoint": {
                    "data_selector": "$",
                    "path": "/northwind/api/v1/Categories",
                    "paginator": "auto",
                },
            },
            
            {
                "name": "get_northwindapiv_1_categoriesid_details",
                "table_name": "raw__northwind__category_details",
                "endpoint": {
                    "data_selector": "$",
                    "path": "/northwind/api/v1/Categories/{id}/Details",
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
                "primary_key": "customerId",
                "endpoint": {
                    "data_selector": "$",
                    "path": "/northwind/api/v1/Customers",
                    "paginator": "auto",
                },
            },
            
            {
                "name": "get_northwindapiv_1_employees",
                "table_name": "raw__northwind__employees",
                "primary_key": "employeeId",
                "endpoint": {
                    "data_selector": "$",
                    "path": "/northwind/api/v1/Employees",
                    "paginator": "auto",
                },
            },

            {
                "name": "get_northwindapiv_1_employeesid_territories",
                "table_name": "raw__northwind__employee_territories",
                "primary_key": "territoryId",
                "endpoint": {
                    "data_selector": "$",
                    "path": "/northwind/api/v1/Employees/{id}/Territories",
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
                    "path": "/northwind/api/v1/Orders/{id}/OrderDetails",
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
                "primary_key": "orderId",
                "endpoint": {
                    "data_selector": "$",
                    "path": "/northwind/api/v1/Orders",
                    "paginator": "auto",
                },
            },
            
            {
                "name": "get_northwindapiv_1_products",
                "table_name": "raw__northwind__products",
                "primary_key": "productId",
                "endpoint": {
                    "data_selector": "$",
                    "path": "/northwind/api/v1/Products",
                    "paginator": "auto",
                },
            },
            
            {
                "name": "get_northwindapiv_1_regions",
                "table_name": "raw__northwind__regions",
                "primary_key": "regionId",
                "endpoint": {
                    "data_selector": "$",
                    "path": "/northwind/api/v1/Regions",
                    "paginator": "auto",
                },
            },
           
            {
                "name": "get_northwindapiv_1_shippers",
                "table_name": "raw__northwind__shippers",
                "primary_key": "shipperId",
                "endpoint": {
                    "data_selector": "$",
                    "path": "/northwind/api/v1/Shippers",
                    "paginator": "auto",
                },
            },
           
            {
                "name": "get_northwindapiv_1_suppliers",
                "table_name": "raw__northwind__suppliers",
                "primary_key": "supplierId",
                "endpoint": {
                    "data_selector": "$",
                    "path": "/northwind/api/v1/Suppliers",
                    "paginator": "auto",
                },
            },
            
            {
                "name": "get_northwindapiv_1_territories",
                "table_name": "raw__northwind__territories",
                "primary_key": "territoryId",
                "endpoint": {
                    "data_selector": "$",
                    "path": "/northwind/api/v1/Territories",
                    "paginator": "auto",
                },
            }
            
        ],
    }

    yield from rest_api_resources(source_config)

def load_northwind(env) -> None:
    dev_mode = env != "prod"
    print(f"Running in {'dev' if dev_mode else 'prod'} mode")

    workspace_id = os.getenv("FABRIC__WORKSPACE_ID")
    lakehouse_id = os.getenv("FABRIC__LAKEHOUSE_ID")

    bucket_url = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files"

    pipeline = dlt.pipeline(
        pipeline_name="northwind",
        destination=dlt.destinations.filesystem(bucket_url=bucket_url),
        dataset_name="northwind",
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