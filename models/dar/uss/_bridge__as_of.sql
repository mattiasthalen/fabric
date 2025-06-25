MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

WITH cte__union AS (
  SELECT
    peripheral,
    NULL AS _pit_hook__product__id,
    _pit_hook__category__id,
    NULL AS _pit_hook__category_detail__id,
    record_updated_at,
    record_valid_from,
    record_valid_to,
    is_current_record
  FROM dar.uss__staging._bridge__categories
  UNION ALL
  SELECT
    peripheral,
    NULL AS _pit_hook__product__id,
    NULL AS _pit_hook__category__id,
    _pit_hook__category_detail__id,
    record_updated_at,
    record_valid_from,
    record_valid_to,
    is_current_record
  FROM dar.uss__staging._bridge__category_details
  UNION ALL
  SELECT
    peripheral,
    _pit_hook__product__id,
    _pit_hook__category__id,
    _pit_hook__category_detail__id,
    record_updated_at,
    record_valid_from,
    record_valid_to,
    is_current_record
  FROM dar.uss__staging._bridge__products
)

SELECT
  peripheral,
  _pit_hook__category__id,
  _pit_hook__category_detail__id,
  _pit_hook__product__id,
  record_updated_at,
  record_valid_from,
  record_valid_to,
  is_current_record
FROM cte__union