MODEL (
  kind VIEW
);

WITH cte__source AS (
  SELECT
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
    @to_timestamp(_dlt_load_id::DOUBLE) AS record_loaded_at
  FROM data_according_to_system.northwind.raw__northwind__employees
), cte__record_windows AS (
  @record_windows(cte__source, employee_id, record_loaded_at, @min_ts, @max_ts)
)
SELECT
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
  record_loaded_at::TIMESTAMP,
  record_updated_at::TIMESTAMP,
  record_version::INT,
  record_valid_from::TIMESTAMP,
  record_valid_to::TIMESTAMP,
  is_current_record::INT
FROM cte__record_windows
