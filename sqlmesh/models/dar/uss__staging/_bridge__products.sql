MODEL (
  enabled FALSE,
  kind VIEW
);

WITH cte__bridge AS (
  SELECT
    'products' AS peripheral,
    frame__northwind__products._pit_hook__product__id,
    frame__northwind__categories._pit_hook__category__id,
    frame__northwind__category_details._pit_hook__category__id AS _pit_hook__category_detail__id,
    GREATEST(
      frame__northwind__products._record__updated_at,
      frame__northwind__categories._record__updated_at,
      frame__northwind__category_details._record__updated_at
    ) AS _record__updated_at,
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
      frame__northwind__products._record__is_current,
      frame__northwind__categories._record__is_current,
      frame__northwind__category_details._record__is_current
    ) AS _record__is_current
  FROM dab.hook.frame__northwind__products
  LEFT JOIN dab.hook.frame__northwind__categories
    ON frame__northwind__products._hook__category__id = frame__northwind__categories._hook__category__id
    AND frame__northwind__products._record__valid_from < frame__northwind__categories._record__valid_to
    AND frame__northwind__products._record__valid_to > frame__northwind__categories._record__valid_from
  LEFT JOIN dab.hook.frame__northwind__category_details
    ON frame__northwind__products._hook__category__id = frame__northwind__category_details._hook__category__id
    AND frame__northwind__products._record__valid_from < frame__northwind__category_details._record__valid_to
    AND frame__northwind__products._record__valid_to > frame__northwind__category_details._record__valid_from
)
SELECT
  peripheral,
  _pit_hook__product__id,
  _pit_hook__category__id,
  _pit_hook__category_detail__id,
  _record__updated_at,
  _record__valid_from,
  _record__valid_to,
  _record__is_current
FROM cte__bridge