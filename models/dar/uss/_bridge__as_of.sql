MODEL (
  enabled TRUE,
  kind VIEW
);

WITH cte__union AS (
  SELECT
    peripheral,
    NULL AS _pit_hook__product__id,
    _pit_hook__category__id,
    NULL AS _pit_hook__category_detail__id,
    _record__updated_at,
    _record__valid_from,
    _record__valid_to,
    _record__is_current
  FROM dar.uss__staging._bridge__categories
  UNION ALL
  SELECT
    peripheral,
    NULL AS _pit_hook__product__id,
    NULL AS _pit_hook__category__id,
    _pit_hook__category_detail__id,
    _record__updated_at,
    _record__valid_from,
    _record__valid_to,
    _record__is_current
  FROM dar.uss__staging._bridge__category_details
  UNION ALL
  SELECT
    peripheral,
    _pit_hook__product__id,
    _pit_hook__category__id,
    _pit_hook__category_detail__id,
    _record__updated_at,
    _record__valid_from,
    _record__valid_to,
    _record__is_current
  FROM dar.uss__staging._bridge__products
)
SELECT
  peripheral,
  _pit_hook__category__id,
  _pit_hook__category_detail__id,
  _pit_hook__product__id,
  _record__updated_at,
  _record__valid_from,
  _record__valid_to,
  _record__is_current
FROM cte__union