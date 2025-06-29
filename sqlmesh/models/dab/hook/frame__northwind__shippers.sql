MODEL (
  enabled FALSE,
  kind VIEW
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.shipper.id|', shipper_id::TEXT) AS _hook__shipper__id,
    *
  FROM das.scd.scd__northwind__shippers
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__shipper__id) AS _pit_hook__shipper__id,
    *
  FROM cte__hooks
)
SELECT
  *
FROM cte__pit_hooks