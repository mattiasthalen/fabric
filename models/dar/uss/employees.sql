MODEL (
  enabled TRUE,
  kind VIEW
);

SELECT
  _pit_hook__employee__id,
  employee_id AS employee__employee_id,
  last_name AS employee__last_name,
  first_name AS employee__first_name,
  title AS employee__title,
  title_of_courtesy AS employee__title_of_courtesy,
  birth_date AS employee__birth_date,
  hire_date AS employee__hire_date,
  address AS employee__address,
  city AS employee__city,
  region AS employee__region,
  postal_code AS employee__postal_code,
  country AS employee__country,
  home_phone AS employee__home_phone,
  extension AS employee__extension,
  photo AS employee__photo,
  notes AS employee__notes,
  reports_to AS employee__reports_to,
  photo_path AS employee__photo_path,
  _dlt_load_id AS employee__dlt_load_id,
  _dlt_id AS employee__dlt_id,
  _record__loaded_at AS employee__record__loaded_at,
  _record__updated_at AS employee__record__updated_at,
  _record__version AS employee__record__version,
  _record__valid_from AS employee__record__valid_from,
  _record__valid_to AS employee__record__valid_to,
  _record__is_current AS employee__record__is_current
FROM dab.hook.frame__northwind__employees