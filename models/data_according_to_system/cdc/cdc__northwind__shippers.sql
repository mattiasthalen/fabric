MODEL (
  kind VIEW
);

WITH cte__source AS (
  SELECT
    shipper_id,
    company_name,
    phone,
    _dlt_load_id,
    _dlt_id,
    @to_timestamp(_dlt_load_id::DOUBLE) AS record_loaded_at
  FROM data_according_to_system.northwind.raw__northwind__shippers
), cte__record_windows AS (
  @record_windows(cte__source, shipper_id, record_loaded_at, @min_ts, @max_ts)
)
SELECT
  shipper_id,
  company_name,
  phone,
  _dlt_load_id,
  _dlt_id,
  record_loaded_at::TIMESTAMP,
  record_updated_at::TIMESTAMP,
  record_version::INT,
  record_valid_from::TIMESTAMP,
  record_valid_to::TIMESTAMP,
  is_current_record::INT
FROM cte__record_windows