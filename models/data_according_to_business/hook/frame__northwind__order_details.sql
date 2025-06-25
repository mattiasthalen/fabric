MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__source AS (
  SELECT
    order_id::TEXT || '|' || product_id::TEXT AS order_id__product_id,
    order_id,
    product_id,
    unit_price,
    quantity,
    discount,
    discount__v_double,
    _dlt_load_id,
    _dlt_id,
    @to_timestamp(_dlt_load_id::DOUBLE) AS record_loaded_at
  FROM data_according_to_system.northwind.raw__northwind__order_details
), cte__record_windows AS (
  @record_windows(cte__source, order_id__product_id, record_loaded_at, @min_ts, @max_ts)
), cte__hooks AS (
  SELECT
    CONCAT('northwind.order.id|', order_id::TEXT) AS _hook__order__id,
    CONCAT('northwind.product.id|', order_id::TEXT) AS _hook__product__id,
    *
  FROM cte__record_windows
), cte__composite_hooks AS (
  SELECT
    CONCAT(_hook__order__id, '~', _hook__product__id) AS _hook__order__product,
    *
  FROM cte__hooks
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', record_valid_from::TEXT, '~', _hook__order__product) AS _pit_hook__order__product,
    *
  FROM cte__composite_hooks
)
SELECT
  _pit_hook__order__product,
  _hook__order__product,
  _hook__order__id,
  _hook__product__id,
  order_id__product_id,
  order_id,
  product_id,
  unit_price,
  quantity,
  discount,
  discount__v_double,
  _dlt_load_id,
  _dlt_id,
  record_loaded_at::TIMESTAMP,
  record_updated_at::TIMESTAMP,
  record_version::INT,
  record_valid_from::TIMESTAMP,
  record_valid_to::TIMESTAMP,
  is_current_record::INT
FROM cte__pit_hooks
WHERE
  1 = 1 AND record_updated_at BETWEEN @start_ts AND @end_ts