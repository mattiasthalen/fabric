MODEL (
  enabled FALSE,
  kind VIEW
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.order.id|', order_id::TEXT) AS _hook__order__id,
    CONCAT('northwind.customer.id|', customer_id::TEXT) AS _hook__customer__id,
    CONCAT('northwind.employee.id|', employee_id::TEXT) AS _hook__employee__id,
    *
  FROM das.scd.scd_view__northwind__orders
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__order__id) AS _pit_hook__order__id,
    *
  FROM cte__hooks
)
SELECT
  *
FROM cte__pit_hooks