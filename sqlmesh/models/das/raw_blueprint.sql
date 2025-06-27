MODEL (
  name das.raw.raw__@source,
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _record__hash
  ),
  blueprints (
    (
      source := northwind__categories
    ),
    (
      source := northwind__category_details
    ),
    (
      source := northwind__customers
    ),
    (
      source := northwind__employees
    ),
    (
      source := northwind__employee_territories
    ),
    (
      source := northwind__order_details
    ),
    (
      source := northwind__orders
    ),
    (
      source := northwind__products
    ),
    (
      source := northwind__regions
    ),
    (
      source := northwind__shippers
    ),
    (
      source := northwind__suppliers
    ),
    (
      source := northwind__territories
    )
  )
);

SELECT
  *,
  @GENERATE_SURROGATE_KEY(
    @STAR__LIST(
      table_name := landing_zone.northwind.raw__@source,
      exclude := [_dlt_load_id, _dlt_id]
    ),
    hash_function := 'SHA256'
  ) AS _record__hash,
  @TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _record__loaded_at
FROM landing_zone.northwind.raw__@source