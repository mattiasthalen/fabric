MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.order.id|', order_id::TEXT) AS _hook__order__id,
    CONCAT('northwind.customer.id|', customer_id::TEXT) AS _hook__customer__id,
    CONCAT('northwind.employee.id|', employee_id::TEXT) AS _hook__employee__id,
    *
  FROM das.scd.scd__northwind__orders
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
  order_id,
  customer_id,
  employee_id,
  order_date,
  required_date,
  shipped_date,
  ship_via,
  freight,
  ship_name,
  ship_address,
  ship_city,
  ship_postal_code,
  ship_country,
  ship_region,
  _dlt_load_id,
  _dlt_id,
  _record__loaded_at,
  record_updated_at,
  record_version,
  _record__valid_from,
  _record__valid_to,
  is_current_record
FROM cte__pit_hooks
WHERE
  1 = 1 AND record_updated_at BETWEEN @start_ts AND @end_ts