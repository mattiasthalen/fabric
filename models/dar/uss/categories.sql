MODEL (
  enabled TRUE,
  kind VIEW
);

SELECT
  _pit_hook__category__id,
  category_id AS category__category_id,
  category_name AS category__category_name,
  description AS category__description,
  _dlt_load_id AS category__dlt_load_id,
  _dlt_id AS category__dlt_id,
  _record__loaded_at AS category__record__loaded_at,
  _record__updated_at AS category__record__updated_at,
  _record__version AS category__record__version,
  _record__valid_from AS category__record__valid_from,
  _record__valid_to AS category__record__valid_to,
  _record__is_current AS category__record__is_current
FROM dab.hook.frame__northwind__categories