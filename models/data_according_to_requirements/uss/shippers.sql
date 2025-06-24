MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (shipper__record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

SELECT
  shipper_id AS shipper__shipper_id,
  company_name AS shipper__company_name,
  phone AS shipper__phone,
  _dlt_load_id AS shipper__dlt_load_id,
  _dlt_id AS shipper__dlt_id,
  record_loaded_at AS shipper__record_loaded_at,
  record_updated_at AS shipper__record_updated_at,
  record_version AS shipper__record_version,
  record_valid_from AS shipper__record_valid_from,
  record_valid_to AS shipper__record_valid_to,
  is_current_record AS shipper__is_current_record
FROM data_according_to_business.hook.frame__northwind__shippers
WHERE
  1 = 1 AND shipper__record_updated_at BETWEEN @start_ts AND @end_ts