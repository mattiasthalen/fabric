MODEL (
  enabled TRUE,
  kind VIEW
);

SELECT
  _pit_hook__region__id,
  region_id AS region__region_id,
  region_description AS region__region_description,
  _dlt_load_id AS region__dlt_load_id,
  _dlt_id AS region__dlt_id,
  _record__loaded_at AS region__record__loaded_at,
  _record__updated_at AS region__record__updated_at,
  _record__version AS region__record__version,
  _record__valid_from AS region__record__valid_from,
  _record__valid_to AS region__record__valid_to,
  _record__is_current AS region__record__is_current
FROM dab.hook.frame__northwind__regions