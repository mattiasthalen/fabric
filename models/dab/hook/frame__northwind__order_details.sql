MODEL (
  enabled TRUE,
  kind VIEW
);

WITH cte__record_validity AS (
  SELECT
    @STAR(
      relation := das.scd.scd__northwind__order_details,
      exclude := [_record__loaded_at, _record__valid_from, _record__valid_to]
    ),
    _record__loaded_at,
    GREATEST(_record__valid_to, _record__loaded_at) AS _record__updated_at,
    _record__valid_from,
    COALESCE(_record__valid_to, @max_ts::TIMESTAMP) AS _record__valid_to,
    CASE WHEN _record__valid_to IS NULL THEN 1 ELSE 0 END AS _record__is_current,
    ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY _record__valid_from ASC) AS _record__version
  FROM das.scd.scd__northwind__order_details
), cte__hooks AS (
  SELECT
    CONCAT('northwind.order.id|', order_id::TEXT) AS _hook__order__id,
    CONCAT('northwind.product.id|', product_id::TEXT) AS _hook__product__id,
    *
  FROM cte__record_validity
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
  _pit_hook__order__product,
  _hook__order__product,
  _hook__order__id,
  _hook__product__id,
  @STAR__LIST(
    table_name := das.scd.scd__northwind__order_details,
    exclude := [_record__loaded_at, _record__valid_from, _record__valid_to],
  ),
  _record__loaded_at,
  _record__updated_at,
  _record__valid_from,
  _record__valid_to,
  _record__is_current,
  _record__version
FROM cte__pit_hooks