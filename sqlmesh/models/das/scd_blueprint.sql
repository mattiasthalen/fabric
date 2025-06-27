MODEL (
  name das.scd.scd__@source,
  enabled TRUE,
  kind SCD_TYPE_2_BY_TIME (
    unique_key @unique_key,
    updated_at_name _record__loaded_at,
    valid_from_name _record__valid_from,
    valid_to_name _record__valid_to
  ),
  blueprints (
    (source := northwind__categories, @unique_key := category_id),
    (source := northwind__category_details, @unique_key := category_id),
    (source := northwind__customers, @unique_key := customer_id),
    (source := northwind__employees, @unique_key := employee_id),
    (
      source := northwind__employee_territories,
      @unique_key := (_get_northwindapiv_1_employees_employee_id, territory_id)
    ),
    (source := northwind__order_details, @unique_key := (order_id, product_id)),
    (source := northwind__orders, @unique_key := order_id),
    (source := northwind__products, @unique_key := product_id),
    (source := northwind__regions, @unique_key := region_id),
    (source := northwind__shippers, @unique_key := shipper_id),
    (source := northwind__suppliers, @unique_key := supplier_id),
    (source := northwind__territories, @unique_key := territory_id)
  )
);

SELECT
  @STAR(relation := das.raw.raw__@source)
FROM das.raw.raw__@source