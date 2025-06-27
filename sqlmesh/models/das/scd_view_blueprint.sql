MODEL (
  name das.scd.scd_view__@source,
  enabled TRUE,
  kind VIEW,
  blueprints (
    (source := northwind__categories, @unique_key := category_id),
    (source := northwind__category_details, @unique_key := category_id),
    (source := northwind__customers, @unique_key := customer_id),
    (source := northwind__employees, @unique_key := employee_id),
    (
      source := northwind__employee_territories,
      @unique_key := _get_northwindapiv_1_employees_employee_id::TEXT||'|'||territory_id::TEXT
    ),
    (source := northwind__order_details, @unique_key := order_id::TEXT||'|'||product_id::TEXT),
    (source := northwind__orders, @unique_key := order_id),
    (source := northwind__products, @unique_key := product_id),
    (source := northwind__regions, @unique_key := region_id),
    (source := northwind__shippers, @unique_key := shipper_id),
    (source := northwind__suppliers, @unique_key := supplier_id),
    (source := northwind__territories, @unique_key := territory_id)
  )
);

SELECT
  @STAR(
    relation := das.scd.scd__@source,
    exclude := [_record__valid_from, _record__valid_to]
  ),
  GREATEST(_record__valid_to, _record__loaded_at) AS _record__updated_at,
  _record__valid_from,
  COALESCE(_record__valid_to, @max_ts::TIMESTAMP) AS _record__valid_to,
  CASE WHEN _record__valid_to IS NULL THEN 1 ELSE 0 END AS _record__is_current,
  ROW_NUMBER() OVER (PARTITION BY @unique_key ORDER BY _record__valid_from ASC) AS _record__version
FROM das.scd.scd__@source