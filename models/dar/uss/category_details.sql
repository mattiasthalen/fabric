MODEL (
  enabled TRUE,
  kind VIEW
);

SELECT
  _pit_hook__category__id AS _pit_hook__category_detail__id,
  category_id AS category_detail__category_id,
  category_name AS category_detail__category_name,
  description AS category_detail__description,
  picture AS category_detail__picture,
  product_names AS category_detail__product_names,
  _dlt_load_id AS category_detail__dlt_load_id,
  _dlt_id AS category_detail__dlt_id,
  _record__loaded_at AS category_detail__record__loaded_at,
  _record__updated_at AS category_detail__record__updated_at,
  _record__version AS category_detail__record__version,
  _record__valid_from AS category_detail__record__valid_from,
  _record__valid_to AS category_detail__record__valid_to,
  _record__is_current AS category_detail__record__is_current
FROM dab.hook.frame__northwind__category_details