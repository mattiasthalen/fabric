MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (shipper__record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

SELECT
  _pit_hook__shipper__id,
  shipper_id AS shipper__shipper_id,
  company_name AS shipper__company_name,
  phone AS shipper__phone,
  _dlt_load_id AS shipper__dlt_load_id,
  _dlt_id AS shipper__dlt_id,
  _record__loaded_at AS shipper___record__loaded_at,
  record_updated_at AS shipper__record_updated_at,
  record_version AS shipper__record_version,
  _record__valid_from AS shipper___record__valid_from,
  _record__valid_to AS shipper___record__valid_to,
  is_current_record AS shipper__is_current_record
FROM dab.hook.frame__northwind__shippers
WHERE
  1 = 1 AND shipper__record_updated_at BETWEEN @start_ts AND @end_ts