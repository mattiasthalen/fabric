MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (region__record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

SELECT
  _pit_hook__region__id,
  region_id AS region__region_id,
  region_description AS region__region_description,
  _dlt_load_id AS region__dlt_load_id,
  _dlt_id AS region__dlt_id,
  _record__loaded_at AS region___record__loaded_at,
  record_updated_at AS region__record_updated_at,
  record_version AS region__record_version,
  _record__valid_from AS region___record__valid_from,
  _record__valid_to AS region___record__valid_to,
  is_current_record AS region__is_current_record
FROM dab.hook.frame__northwind__regions
WHERE
  1 = 1 AND region__record_updated_at BETWEEN @start_ts AND @end_ts