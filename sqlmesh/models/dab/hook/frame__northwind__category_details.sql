MODEL (
  enabled TRUE,
  kind VIEW
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.category.id|', category_id::TEXT) AS _hook__category__id,
    *
  FROM das.scd.scd__northwind__category_details
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__category__id) AS _pit_hook__category__id,
    *
  FROM cte__hooks
)
SELECT
  *
FROM cte__pit_hooks