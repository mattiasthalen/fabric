MODEL (
  enabled TRUE,
  kind VIEW
);

SELECT
  _pit_hook__employee__territory,
  _get_northwindapiv_1_employees_employee_id AS employee_territory__employee_id,
  territory_id AS employee_territory__territory_id,
  territory_description AS employee_territory__territory_description,
  region_id AS employee_territory__region_id,
  _dlt_load_id AS employee_territory__dlt_load_id,
  _dlt_id AS employee_territory__dlt_id,
  _record__loaded_at AS employee_territory__record__loaded_at,
  _record__updated_at AS employee_territory__record__updated_at,
  _record__version AS employee_territory__record__version,
  _record__valid_from AS employee_territory__record__valid_from,
  _record__valid_to AS employee_territory__record__valid_to,
  _record__is_current AS employee_territory__record__is_current
FROM dab.hook.frame__northwind__employee_territories