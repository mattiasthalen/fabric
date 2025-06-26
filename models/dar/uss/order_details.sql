MODEL (
  enabled TRUE,
  kind VIEW
);

SELECT
  _pit_hook__order__product,
  order_id AS order_detail__order_id,
  product_id AS order_detail__product_id,
  unit_price AS order_detail__unit_price,
  quantity AS order_detail__quantity,
  discount AS order_detail__discount,
  discount__v_double AS order_detail__discount__v_double,
  _dlt_load_id AS order_detail__dlt_load_id,
  _dlt_id AS order_detail__dlt_id,
  _record__loaded_at AS order_detail__record__loaded_at,
  _record__updated_at AS order_detail__record__updated_at,
  _record__version AS order_detail__record__version,
  _record__valid_from AS order_detail__record__valid_from,
  _record__valid_to AS order_detail__record__valid_to,
  _record__is_current AS order_detail__record__is_current
FROM dab.hook.frame__northwind__order_details