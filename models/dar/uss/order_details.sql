MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (order_detail__record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

SELECT
  _pit_hook__order__product,
  order_id__product_id AS order_detail__order_id__product_id,
  order_id AS order_detail__order_id,
  product_id AS order_detail__product_id,
  unit_price AS order_detail__unit_price,
  quantity AS order_detail__quantity,
  discount AS order_detail__discount,
  discount__v_double AS order_detail__discount__v_double,
  _dlt_load_id AS order_detail__dlt_load_id,
  _dlt_id AS order_detail__dlt_id,
  _record__loaded_at AS order_detail___record__loaded_at,
  record_updated_at AS order_detail__record_updated_at,
  record_version AS order_detail__record_version,
  _record__valid_from AS order_detail___record__valid_from,
  _record__valid_to AS order_detail___record__valid_to,
  is_current_record AS order_detail__is_current_record
FROM dab.hook.frame__northwind__order_details
WHERE
  1 = 1 AND order_detail__record_updated_at BETWEEN @start_ts AND @end_ts