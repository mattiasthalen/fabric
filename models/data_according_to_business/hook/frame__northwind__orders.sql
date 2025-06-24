MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__source AS (
  SELECT
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
    @to_timestamp(_dlt_load_id::DOUBLE) AS record_loaded_at
  FROM data_according_to_system.northwind.raw__northwind__orders
), cte__record_windows AS (
  @record_windows(cte__source, order_id, record_loaded_at, @min_ts, @max_ts)
)
SELECT
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
  record_loaded_at,
  record_updated_at,
  record_version,
  record_valid_from,
  record_valid_to,
  is_current_record
FROM cte__record_windows
WHERE
  1 = 1 AND record_updated_at BETWEEN @start_ts AND @end_ts