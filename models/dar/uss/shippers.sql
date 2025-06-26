MODEL (
  enabled TRUE,
  kind VIEW
);

SELECT
  _pit_hook__shipper__id,
  shipper_id AS shipper__shipper_id,
  company_name AS shipper__company_name,
  phone AS shipper__phone,
  _dlt_load_id AS shipper__dlt_load_id,
  _dlt_id AS shipper__dlt_id,
  _record__loaded_at AS shipper__record__loaded_at,
  _record__updated_at AS shipper__record__updated_at,
  _record__version AS shipper__record__version,
  _record__valid_from AS shipper__record__valid_from,
  _record__valid_to AS shipper__record__valid_to,
  _record__is_current AS shipper__record__is_current
FROM dab.hook.frame__northwind__shippers