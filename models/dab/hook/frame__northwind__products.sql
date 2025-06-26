MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.product.id|', product_id::TEXT) AS _hook__product__id,
    CONCAT('northwind.supplier.id|', supplier_id::TEXT) AS _hook__supplier__id,
    CONCAT('northwind.category.id|', category_id::TEXT) AS _hook__category__id,
    CONCAT('northwind.category_detail.id|', category_id::TEXT) AS _hook__category_detail__id,
    *
  FROM das.scd.scd__northwind__products
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__product__id) AS _pit_hook__product__id,
    *
  FROM cte__hooks
)
SELECT
  _pit_hook__product__id,
  _hook__product__id,
  _hook__supplier__id,
  _hook__category__id,
  _hook__category_detail__id,
  product_id,
  product_name,
  supplier_id,
  category_id,
  quantity_per_unit,
  unit_price,
  units_in_stock,
  units_on_order,
  reorder_level,
  discontinued,
  _dlt_load_id,
  _dlt_id,
  _record__loaded_at,
  record_updated_at,
  record_version,
  _record__valid_from,
  _record__valid_to,
  is_current_record
FROM cte__pit_hooks
WHERE
  1 = 1 AND record_updated_at BETWEEN @start_ts AND @end_ts