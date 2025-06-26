MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

WITH cte__bridge AS (
  SELECT
    'category_details' AS peripheral,
    _pit_hook__category__id AS _pit_hook__category_detail__id,
    record_updated_at,
    _record__valid_from,
    _record__valid_to,
    is_current_record
  FROM dab.hook.frame__northwind__category_details
)

SELECT
  peripheral,
  _pit_hook__category_detail__id,
  record_updated_at,
  _record__valid_from,
  _record__valid_to,
  is_current_record
FROM cte__bridge
WHERE
  1 = 1 AND record_updated_at BETWEEN @start_ts AND @end_ts