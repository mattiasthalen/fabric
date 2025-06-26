MODEL (
  enabled TRUE,
  kind VIEW
);

WITH cte__record_validity AS (
  SELECT
    @STAR(
      relation := das.scd.scd__northwind__orders,
      exclude := [_record__loaded_at, _record__valid_from, _record__valid_to]
    ),
    _record__loaded_at,
    GREATEST(_record__valid_to, _record__loaded_at) AS _record__updated_at,
    _record__valid_from,
    COALESCE(_record__valid_to, @max_ts::TIMESTAMP) AS _record__valid_to,
    CASE WHEN _record__valid_to IS NULL THEN 1 ELSE 0 END AS _record__is_current,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _record__valid_from ASC) AS _record__version
  FROM das.scd.scd__northwind__orders
), cte__hooks AS (
  SELECT
    CONCAT('northwind.order.id|', order_id::TEXT) AS _hook__order__id,
    CONCAT('northwind.customer.id|', customer_id::TEXT) AS _hook__customer__id,
    CONCAT('northwind.employee.id|', employee_id::TEXT) AS _hook__employee__id,
    *
  FROM cte__record_validity
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__order__id) AS _pit_hook__order__id,
    *
  FROM cte__hooks
)
SELECT
  _pit_hook__order__id,
  _hook__order__id,
  _hook__customer__id,
  _hook__employee__id,
  @STAR__LIST(
    table_name := das.scd.scd__northwind__orders,
    exclude := [_record__loaded_at, _record__valid_from, _record__valid_to],
  ),
  _record__loaded_at,
  _record__updated_at,
  _record__valid_from,
  _record__valid_to,
  _record__is_current,
  _record__version
FROM cte__pit_hooks