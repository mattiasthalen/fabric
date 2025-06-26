MODEL (
  enabled TRUE,
  kind VIEW
);

WITH cte__bridge AS (
  SELECT
    'category_details' AS peripheral,
    _pit_hook__category_detail__id,
    _record__updated_at,
    _record__valid_from,
    _record__valid_to,
    _record__is_current
  FROM dab.hook.frame__northwind__category_details
)
SELECT
  peripheral,
  _pit_hook__category_detail__id,
  _record__updated_at,
  _record__valid_from,
  _record__valid_to,
  _record__is_current
FROM cte__bridge