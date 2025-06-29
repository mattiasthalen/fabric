MODEL (
  enabled FALSE,
  kind VIEW
);

WITH cte__bridge AS (
  SELECT
    'categories' AS peripheral,
    _pit_hook__category__id,
    _record__updated_at,
    _record__valid_from,
    _record__valid_to,
    _record__is_current
  FROM dab.hook.frame__northwind__categories
)
SELECT
  peripheral,
  _pit_hook__category__id,
  _record__updated_at,
  _record__valid_from,
  _record__valid_to,
  _record__is_current
FROM cte__bridge