MODEL (
  enabled TRUE,
  kind VIEW
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.order.id|', order_id::TEXT) AS _hook__order__id,
    CONCAT('northwind.product.id|', product_id::TEXT) AS _hook__product__id,
    *
  FROM das.scd.scd_view__northwind__order_details
), cte__composite_hooks AS (
  SELECT
    CONCAT(_hook__order__id, '~', _hook__product__id) AS _hook__order__product,
    *
  FROM cte__hooks
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__order__product) AS _pit_hook__order__product,
    *
  FROM cte__composite_hooks
)
SELECT
  *
FROM cte__pit_hooks