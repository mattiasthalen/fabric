-- Dummy model to render the external_models.yaml
MODEL (
    kind VIEW,
    enabled FALSE
);

WITH 
A AS (SELECT * FROM landing_zone.northwind.raw__northwind__categories),
A AS (SELECT * FROM landing_zone.northwind.raw__northwind__category_details),
A AS (SELECT * FROM landing_zone.northwind.raw__northwind__customers),
A AS (SELECT * FROM landing_zone.northwind.raw__northwind__employees),
A AS (SELECT * FROM landing_zone.northwind.raw__northwind__employee_territories),
A AS (SELECT * FROM landing_zone.northwind.raw__northwind__order_details),
A AS (SELECT * FROM landing_zone.northwind.raw__northwind__orders),
A AS (SELECT * FROM landing_zone.northwind.raw__northwind__products),
A AS (SELECT * FROM landing_zone.northwind.raw__northwind__regions),
A AS (SELECT * FROM landing_zone.northwind.raw__northwind__shippers),
A AS (SELECT * FROM landing_zone.northwind.raw__northwind__suppliers),
A AS (SELECT * FROM landing_zone.northwind.raw__northwind__territories)
SELECT 1 AS dummy;