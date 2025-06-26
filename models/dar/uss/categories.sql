MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (category__record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

SELECT
  _pit_hook__category__id,
  category_id AS category__category_id,
  category_name AS category__category_name,
  description AS category__description,
  _dlt_load_id AS category__dlt_load_id,
  _dlt_id AS category__dlt_id,
  _record__loaded_at AS category___record__loaded_at,
  record_updated_at AS category__record_updated_at,
  record_version AS category__record_version,
  _record__valid_from AS category___record__valid_from,
  _record__valid_to AS category___record__valid_to,
  is_current_record AS category__is_current_record
FROM dab.hook.frame__northwind__categories
WHERE
  1 = 1 AND category__record_updated_at BETWEEN @start_ts AND @end_ts