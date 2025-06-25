MODEL (
  kind VIEW
);

WITH cte__source AS (
  SELECT
    _get_northwindapiv_1_employees_employee_id::TEXT || '|' || territory_id::TEXT AS employee_id__territory_id,
    _get_northwindapiv_1_employees_employee_id AS employee_id,
    territory_id,
    territory_description,
    region_id,
    _dlt_load_id,
    _dlt_id,
    @to_timestamp(_dlt_load_id::DOUBLE) AS record_loaded_at
  FROM data_according_to_system.northwind.raw__northwind__employee_territories
), cte__record_windows AS (
  @record_windows(cte__source, employee_id__territory_id, record_loaded_at, @min_ts, @max_ts)
)
SELECT
  employee_id__territory_id,
  employee_id,
  territory_id,
  territory_description,
  region_id,
  _dlt_load_id,
  _dlt_id,
  record_loaded_at::TIMESTAMP,
  record_updated_at::TIMESTAMP,
  record_version::INT,
  record_valid_from::TIMESTAMP,
  record_valid_to::TIMESTAMP,
  is_current_record::INT
FROM cte__record_windows
