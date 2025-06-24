MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__source AS (
  SELECT
    category_id,
    category_name,
    description,
    picture,
    product_names,
    _dlt_load_id,
    _dlt_id,
    @to_timestamp(_dlt_load_id::DOUBLE) AS record_loaded_at
  FROM data_according_to_system.northwind.raw__northwind__category_details
), cte__record_windows AS (
  @record_windows(cte__source, category_id, record_loaded_at, @min_ts, @max_ts)
), cte__hooks AS (
  SELECT
    CONCAT('northwind.category_detail.id|', category_id::TEXT) AS _hook__category_detail__id,
    *
  FROM cte__record_windows
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', record_valid_from::TEXT, '~', _hook__category_detail__id) AS _pit_hook__category_detail__id,
    *
  FROM cte__hooks
)
SELECT
  _pit_hook__category_detail__id,
  _hook__category_detail__id,
  category_id,
  category_name,
  description,
  picture,
  product_names,
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