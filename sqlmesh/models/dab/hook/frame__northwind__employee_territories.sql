MODEL (
  enabled FALSE,
  kind VIEW
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.employee.id|', _get_northwindapiv_1_employees_employee_id::TEXT) AS _hook__employee__id,
    CONCAT('northwind.territory.id|', territory_id::TEXT) AS _hook__territory__id,
    *
  FROM das.scd.scd__northwind__employee_territories
), cte__composite_hooks AS (
  SELECT
    CONCAT(_hook__employee__id, '~', _hook__territory__id) AS _hook__employee__territory,
    *
  FROM cte__hooks
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__employee__territory) AS _pit_hook__employee__territory,
    *
  FROM cte__composite_hooks
)
SELECT
  *
FROM cte__pit_hooks