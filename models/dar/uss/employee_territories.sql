MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (employee_territory__record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

SELECT
  _pit_hook__employee__territory,
  employee_id__territory_id AS employee_territory__employee_id__territory_id,
  employee_id AS employee_territory__employee_id,
  territory_id AS employee_territory__territory_id,
  territory_description AS employee_territory__territory_description,
  region_id AS employee_territory__region_id,
  _dlt_load_id AS employee_territory__dlt_load_id,
  _dlt_id AS employee_territory__dlt_id,
  _record__loaded_at AS employee_territory___record__loaded_at,
  record_updated_at AS employee_territory__record_updated_at,
  record_version AS employee_territory__record_version,
  _record__valid_from AS employee_territory___record__valid_from,
  _record__valid_to AS employee_territory___record__valid_to,
  is_current_record AS employee_territory__is_current_record
FROM dab.hook.frame__northwind__employee_territories
WHERE
  1 = 1 AND employee_territory__record_updated_at BETWEEN @start_ts AND @end_ts