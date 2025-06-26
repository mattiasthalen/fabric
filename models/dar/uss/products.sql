MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (product__record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

SELECT
  _pit_hook__product__id,
  product_id AS product__product_id,
  product_name AS product__product_name,
  supplier_id AS product__supplier_id,
  category_id AS product__category_id,
  quantity_per_unit AS product__quantity_per_unit,
  unit_price AS product__unit_price,
  units_in_stock AS product__units_in_stock,
  units_on_order AS product__units_on_order,
  reorder_level AS product__reorder_level,
  discontinued AS product__discontinued,
  _dlt_load_id AS product__dlt_load_id,
  _dlt_id AS product__dlt_id,
  _record__loaded_at AS product___record__loaded_at,
  record_updated_at AS product__record_updated_at,
  record_version AS product__record_version,
  _record__valid_from AS product___record__valid_from,
  _record__valid_to AS product___record__valid_to,
  is_current_record AS product__is_current_record
FROM dab.hook.frame__northwind__products
WHERE
  1 = 1 AND product__record_updated_at BETWEEN @start_ts AND @end_ts