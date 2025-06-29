MODEL (
  enabled FALSE,
  kind VIEW
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.territory.id|', territory_id::TEXT) AS _hook__territory__id,
    CONCAT('northwind.region.id|', region_id::TEXT) AS _hook__region__id,
    *
  FROM das.scd.scd_view__northwind__territories
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__territory__id) AS _pit_hook__territory__id,
    *
  FROM cte__hooks
)
SELECT
  *
FROM cte__pit_hooks