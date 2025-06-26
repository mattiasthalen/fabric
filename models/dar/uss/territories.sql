MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (territory__record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

SELECT
  _pit_hook__territory__id,
  territory_id AS territory__territory_id,
  territory_description AS territory__territory_description,
  region_id AS territory__region_id,
  _dlt_load_id AS territory__dlt_load_id,
  _dlt_id AS territory__dlt_id,
  _record__loaded_at AS territory___record__loaded_at,
  record_updated_at AS territory__record_updated_at,
  record_version AS territory__record_version,
  _record__valid_from AS territory___record__valid_from,
  _record__valid_to AS territory___record__valid_to,
  is_current_record AS territory__is_current_record
FROM dab.hook.frame__northwind__territories
WHERE
  1 = 1 AND territory__record_updated_at BETWEEN @start_ts AND @end_ts