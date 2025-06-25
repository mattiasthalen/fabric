MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.order.id|', order_id::TEXT) AS _hook__order__id,
    CONCAT('northwind.product.id|', product_id::TEXT) AS _hook__product__id,
    *
  FROM das.cdc.cdc__northwind__order_details
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
  record_loaded_at,
  record_updated_at,
  record_version,
  record_valid_from,
  record_valid_to,
  is_current_record
FROM cte__pit_hooks
WHERE
  1 = 1 AND record_updated_at BETWEEN @start_ts AND @end_ts