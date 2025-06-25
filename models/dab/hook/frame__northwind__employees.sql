MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.employee.id|', employee_id::TEXT) AS _hook__employee__id,
    CONCAT('northwind.region.id|', region::TEXT) AS _hook__region__id,
    *
  FROM das.cdc.cdc__northwind__employees
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', record_valid_from::TEXT, '~', _hook__employee__id) AS _pit_hook__employee__id,
    *
  FROM cte__hooks
)
SELECT
  _pit_hook__employee__id,
  _hook__employee__id,
  _hook__region__id,
  employee_id,
  last_name,
  first_name,
  title,
  title_of_courtesy,
  birth_date,
  hire_date,
  address,
  city,
  region,
  postal_code,
  country,
  home_phone,
  extension,
  photo,
  notes,
  reports_to,
  photo_path,
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