MODEL (
  enabled FALSE,
  kind VIEW
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.employee.id|', employee_id::TEXT) AS _hook__employee__id,
    CONCAT('northwind.region.id|', region::TEXT) AS _hook__region__id,
    *
  FROM das.scd.scd__northwind__employees
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__employee__id) AS _pit_hook__employee__id,
    *
  FROM cte__hooks
)
SELECT
  *
FROM cte__pit_hooks