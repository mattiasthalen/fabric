MODEL (
  enabled FALSE,
  kind VIEW
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.supplier.id|', supplier_id::TEXT) AS _hook__supplier__id,
    CONCAT('northwind.region.id|', region::TEXT) AS _hook__region__id,
    *
  FROM das.scd.scd_view__northwind__suppliers
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__supplier__id) AS _pit_hook__supplier__id,
    *
  FROM cte__hooks
)
SELECT
  *
FROM cte__pit_hooks