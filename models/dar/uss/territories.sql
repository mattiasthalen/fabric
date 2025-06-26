MODEL (
  enabled TRUE,
  kind VIEW
);

SELECT
  _pit_hook__territory__id,
  territory_id AS territory__territory_id,
  territory_description AS territory__territory_description,
  region_id AS territory__region_id,
  _dlt_load_id AS territory__dlt_load_id,
  _dlt_id AS territory__dlt_id,
  _record__loaded_at AS territory__record__loaded_at,
  _record__updated_at AS territory__record__updated_at,
  _record__version AS territory__record__version,
  _record__valid_from AS territory__record__valid_from,
  _record__valid_to AS territory__record__valid_to,
  _record__is_current AS territory__record__is_current
FROM dab.hook.frame__northwind__territories