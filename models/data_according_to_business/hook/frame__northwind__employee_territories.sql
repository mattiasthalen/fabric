MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.employee.id|', employee_id::TEXT) AS _hook__employee__id,
    CONCAT('northwind.territory.id|', territory_id::TEXT) AS _hook__territory__id,
    *
  FROM data_according_to_system.cdc.cdc__northwind__employee_territories
), cte__composite_hooks AS (
  SELECT
    CONCAT(_hook__employee__id, '~', _hook__territory__id) AS _hook__employee__territory,
    *
  FROM cte__hooks
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', record_valid_from::TEXT, '~', _hook__employee__territory) AS _pit_hook__employee__territory,
    *
  FROM cte__composite_hooks
)
SELECT
  _pit_hook__employee__territory,
  _hook__employee__territory,
  _hook__employee__id,
  _hook__territory__id,
  employee_id__territory_id,
  employee_id,
  territory_id,
  territory_description,
  region_id,
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