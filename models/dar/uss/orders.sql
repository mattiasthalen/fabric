MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (order__record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

SELECT
  _pit_hook__order__id,
  order_id AS order__order_id,
  customer_id AS order__customer_id,
  employee_id AS order__employee_id,
  order_date AS order__order_date,
  required_date AS order__required_date,
  shipped_date AS order__shipped_date,
  ship_via AS order__ship_via,
  freight AS order__freight,
  ship_name AS order__ship_name,
  ship_address AS order__ship_address,
  ship_city AS order__ship_city,
  ship_postal_code AS order__ship_postal_code,
  ship_country AS order__ship_country,
  ship_region AS order__ship_region,
  _dlt_load_id AS order__dlt_load_id,
  _dlt_id AS order__dlt_id,
  _record__loaded_at AS order___record__loaded_at,
  record_updated_at AS order__record_updated_at,
  record_version AS order__record_version,
  _record__valid_from AS order___record__valid_from,
  _record__valid_to AS order___record__valid_to,
  is_current_record AS order__is_current_record
FROM dab.hook.frame__northwind__orders
WHERE
  1 = 1 AND order__record_updated_at BETWEEN @start_ts AND @end_ts