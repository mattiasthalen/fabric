MODEL (
  kind VIEW
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
  FROM das.northwind.raw__northwind__order_details
), cte__record_windows AS (
  @record_windows(cte__source, order_id__product_id, record_loaded_at, @min_ts, @max_ts)
)
SELECT
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
FROM cte__record_windows