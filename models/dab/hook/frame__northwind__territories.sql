MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.territory.id|', territory_id::TEXT) AS _hook__territory__id,
    CONCAT('northwind.region.id|', region_id::TEXT) AS _hook__region__id,
    *
  FROM das.scd.scd__northwind__territories
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__territory__id) AS _pit_hook__territory__id,
    *
  FROM cte__hooks
)
SELECT
  _pit_hook__territory__id,
  _hook__territory__id,
  _hook__region__id,
  territory_id,
  territory_description,
  region_id,
  _dlt_load_id,
  _dlt_id,
  _record__loaded_at,
  record_updated_at,
  record_version,
  _record__valid_from,
  _record__valid_to,
  is_current_record
FROM cte__pit_hooks
WHERE
  1 = 1 AND record_updated_at BETWEEN @start_ts AND @end_ts