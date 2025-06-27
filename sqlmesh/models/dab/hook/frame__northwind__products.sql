MODEL (
  enabled TRUE,
  kind VIEW
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.product.id|', product_id::TEXT) AS _hook__product__id,
    CONCAT('northwind.supplier.id|', supplier_id::TEXT) AS _hook__supplier__id,
    CONCAT('northwind.category.id|', category_id::TEXT) AS _hook__category__id,
    CONCAT('northwind.category_detail.id|', category_id::TEXT) AS _hook__category_detail__id,
    *
  FROM das.scd.scd_view__northwind__products
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__product__id) AS _pit_hook__product__id,
    *
  FROM cte__hooks
)
SELECT
  *
FROM cte__pit_hooks