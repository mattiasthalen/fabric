MODEL (
  name das.raw.raw__@source,
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _record__hash
  ),
  blueprints (
    (schema := northwind, source := northwind__categories),
    (schema := northwind, source := northwind__category_details),
    (schema := northwind, source := northwind__customers),
    (schema := northwind, source := northwind__employees),
    (schema := northwind, source := northwind__employee_territories),
    (schema := northwind, source := northwind__order_details),
    (schema := northwind, source := northwind__orders),
    (schema := northwind, source := northwind__products),
    (schema := northwind, source := northwind__regions),
    (schema := northwind, source := northwind__shippers),
    (schema := northwind, source := northwind__suppliers),
    (schema := northwind, source := northwind__territories)
  )
);

SELECT
  @STAR(relation := landing_zone.@schema.raw__@source),
  @GENERATE_SURROGATE_KEY(
    @STAR__LIST(
      table_name := landing_zone.@schema.raw__@source,
      exclude := [_dlt_load_id, _dlt_id]
    ),
    hash_function := 'SHA256'
  ) AS _record__hash,
  @TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _record__loaded_at
FROM landing_zone.@schema.raw__@source