MODEL (
  kind VIEW
);

WITH cte__source AS (
  SELECT
    category_id,
    category_name,
    description,
    _dlt_load_id,
    _dlt_id,
    @to_timestamp(_dlt_load_id::DOUBLE) AS record_loaded_at
  FROM data_according_to_system.northwind.raw__northwind__categories
), cte__record_windows AS (
  @record_windows(cte__source, category_id, record_loaded_at, @min_ts, @max_ts)
)
SELECT
  category_id,
  category_name,
  description,
  _dlt_load_id,
  _dlt_id,
  record_loaded_at,
  record_updated_at,
  record_version,
  record_valid_from,
  record_valid_to,
  is_current_record
FROM cte__record_windows