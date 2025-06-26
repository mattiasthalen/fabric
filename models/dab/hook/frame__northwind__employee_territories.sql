MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_record__updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__record_validity AS (
  SELECT
    @STAR(
      relation := das.scd.scd__northwind__employee_territories,
      exclude := [_record__loaded_at, _record__valid_from, _record__valid_to]
    ),
    _record__loaded_at,
    GREATEST(_record__valid_to, _record__loaded_at) AS _record__updated_at,
    _record__valid_from,
    COALESCE(_record__valid_to, @max_ts::TIMESTAMP) AS _record__valid_to,
    CASE WHEN _record__valid_to IS NULL THEN 1 ELSE 0 END AS _record__is_current,
    ROW_NUMBER() OVER (PARTITION BY _get_northwindapiv_1_employees_employee_id, territory_id ORDER BY _record__valid_from ASC) AS _record__version
  FROM das.scd.scd__northwind__employee_territories
), cte__hooks AS (
  SELECT
    CONCAT('northwind.employee.id|', _get_northwindapiv_1_employees_employee_id::TEXT) AS _hook__employee__id,
    CONCAT('northwind.territory.id|', territory_id::TEXT) AS _hook__territory__id,
    *
  FROM cte__record_validity
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
  _pit_hook__employee__territory,
  _hook__employee__territory,
  _hook__employee__id,
  _hook__territory__id,
  @STAR__LIST(
    table_name := das.scd.scd__northwind__employee_territories,
    exclude := [_record__loaded_at, _record__valid_from, _record__valid_to],
  ),
  _record__loaded_at,
  _record__updated_at,
  _record__valid_from,
  _record__valid_to,
  _record__is_current,
  _record__version
FROM cte__pit_hooks
WHERE
  1 = 1 AND _record__updated_at BETWEEN @start_ts AND @end_ts