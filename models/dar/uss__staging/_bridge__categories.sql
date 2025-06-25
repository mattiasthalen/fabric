MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

WITH cte__bridge AS (
  SELECT
    'categories' AS peripheral,
    _pit_hook__category__id,
    record_updated_at,
    record_valid_from,
    record_valid_to,
    is_current_record
  FROM dab.hook.frame__northwind__categories
)
SELECT
  peripheral,
  _pit_hook__category__id,
  record_updated_at,
  record_valid_from,
  record_valid_to,
  is_current_record
FROM cte__bridge
WHERE
  1 = 1 AND record_updated_at BETWEEN @start_ts AND @end_ts