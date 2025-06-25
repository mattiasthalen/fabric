-- Dummy model to render the external_models.yaml
MODEL (
    name das.dummy.dummy__@source,
    kind VIEW,
    enabled FALSE,
    blueprints (
        (schema_name := northwind, source := northwind__categories),
        (schema_name := northwind, source := northwind__category_details),
        (schema_name := northwind, source := northwind__customers),
        (schema_name := northwind, source := northwind__employees),
        (schema_name := northwind, source := northwind__employee_territories),
        (schema_name := northwind, source := northwind__order_details),
        (schema_name := northwind, source := northwind__orders),
        (schema_name := northwind, source := northwind__products),
        (schema_name := northwind, source := northwind__regions),
        (schema_name := northwind, source := northwind__shippers),
        (schema_name := northwind, source := northwind__suppliers),
        (schema_name := northwind, source := northwind__territories),
    )
);

SELECT
    *
FROM landing_zone.@schema_name.raw__@source