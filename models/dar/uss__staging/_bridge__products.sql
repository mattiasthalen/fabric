MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

WITH cte__bridge AS (
  SELECT
    'products' AS peripheral,
    frame__northwind__products._pit_hook__product__id,
    frame__northwind__categories._pit_hook__category__id,
    frame__northwind__category_details._pit_hook__category__id AS _pit_hook__category_detail__id,
    GREATEST(
      frame__northwind__products.record_updated_at,
      frame__northwind__categories.record_updated_at,
      frame__northwind__category_details.record_updated_at
    ) AS record_updated_at,
    GREATEST(
      frame__northwind__products._record__valid_from,
      frame__northwind__categories._record__valid_from,
      frame__northwind__category_details._record__valid_from
    ) AS _record__valid_from,
    LEAST(
      frame__northwind__products._record__valid_to,
      frame__northwind__categories._record__valid_to,
      frame__northwind__category_details._record__valid_to
    ) AS _record__valid_to,
    LEAST(
      frame__northwind__products.is_current_record,
      frame__northwind__categories.is_current_record,
      frame__northwind__category_details.is_current_record
    ) AS is_current_record
  FROM dab.hook.frame__northwind__products
  LEFT JOIN dab.hook.frame__northwind__categories
    ON frame__northwind__products._hook__category__id = frame__northwind__categories._hook__category__id
  LEFT JOIN dab.hook.frame__northwind__category_details
    ON frame__northwind__products._hook__category__id = frame__northwind__category_details._hook__category__id
  WHERE
    1 = 1
    AND frame__northwind__products._record__valid_from >= frame__northwind__categories._record__valid_to
    AND frame__northwind__products._record__valid_to <= frame__northwind__categories._record__valid_from
    AND frame__northwind__products._record__valid_from >= frame__northwind__category_details._record__valid_to
    AND frame__northwind__products._record__valid_to <= frame__northwind__category_details._record__valid_from
)
SELECT
  peripheral,
  _pit_hook__product__id,
  _pit_hook__category__id,
  _pit_hook__category_detail__id,
  record_updated_at,
  _record__valid_from,
  _record__valid_to,
  is_current_record
FROM cte__bridge
WHERE
  1 = 1 AND record_updated_at BETWEEN @start_ts AND @end_ts