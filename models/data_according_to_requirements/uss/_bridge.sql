MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

WITH cte__northwind__categories AS (
  SELECT
    'northwind__categories' AS peripheral,
    _pit_hook__category__id,
    record_updated_at,
    record_valid_from,
    record_valid_to,
    is_current_record
  FROM data_according_to_business.hook.frame__northwind__categories
), cte__northwind__category_details AS (
  SELECT
    'northwind__category_details' AS peripheral,
    _pit_hook__category_detail__id,
    record_updated_at,
    record_valid_from,
    record_valid_to,
    is_current_record
  FROM data_according_to_business.hook.frame__northwind__category_details
), cte__northwind__products AS (
  SELECT
    'northwind__products' AS peripheral,
    frame__northwind__products._pit_hook__product__id,
    frame__northwind__categories._pit_hook__category__id,
    frame__northwind__category_details._pit_hook__category_detail__id,
    GREATEST(
      frame__northwind__products.record_updated_at,
      frame__northwind__categories.record_updated_at,
      frame__northwind__category_details.record_updated_at
    ) AS record_updated_at,
    GREATEST(
      frame__northwind__products.record_valid_from,
      frame__northwind__categories.record_valid_from,
      frame__northwind__category_details.record_valid_from
    ) AS record_valid_from,
    LEAST(
      frame__northwind__products.record_valid_to,
      frame__northwind__categories.record_valid_to,
      frame__northwind__category_details.record_valid_to
    ) AS record_valid_to,
    LEAST(
      frame__northwind__products.is_current_record,
      frame__northwind__categories.is_current_record,
      frame__northwind__category_details.is_current_record
    ) AS is_current_record
  FROM data_according_to_business.hook.frame__northwind__products
  LEFT JOIN data_according_to_business.hook.frame__northwind__categories
    ON frame__northwind__products._hook__product__id = frame__northwind__categories._hook__category__id
  LEFT JOIN data_according_to_business.hook.frame__northwind__category_details
    ON frame__northwind__products._hook__category_detail__id = frame__northwind__category_details._hook__category_detail__id
  WHERE
    1 = 1
    AND frame__northwind__products.record_valid_from >= frame__northwind__categories.record_valid_to
    AND frame__northwind__products.record_valid_to <= frame__northwind__categories.record_valid_from
    AND frame__northwind__products.record_valid_from >= frame__northwind__category_details.record_valid_to
    AND frame__northwind__products.record_valid_to <= frame__northwind__category_details.record_valid_from
), cte__union AS (
  SELECT
    peripheral,
    _pit_hook__category__id,
    NULL AS _pit_hook__category_detail__id,
    NULL AS _pit_hook__product__id,
    record_updated_at,
    record_valid_from,
    record_valid_to,
    is_current_record
  FROM cte__northwind__categories
  UNION ALL
  SELECT
    peripheral,
    NULL AS _pit_hook__category__id,
    _pit_hook__category_detail__id,
    NULL AS _pit_hook__product__id,
    record_updated_at,
    record_valid_from,
    record_valid_to,
    is_current_record
  FROM cte__northwind__category_details
  UNION ALL
  SELECT
    peripheral,
    _pit_hook__category__id,
    _pit_hook__category_detail__id,
    _pit_hook__product__id,
    record_updated_at,
    record_valid_from,
    record_valid_to,
    is_current_record
  FROM cte__northwind__products
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