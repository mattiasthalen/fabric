MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_loaded_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__source AS (
  SELECT
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
)
SELECT
  *
FROM cte__source
WHERE
  record_loaded_at BETWEEN @start_ts AND @end_ts