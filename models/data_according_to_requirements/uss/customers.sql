MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

SELECT
  *
FROM data_according_to_business.hook.frame__northwind__customers
WHERE
  record_updated_at BETWEEN @start_ts AND @end_ts