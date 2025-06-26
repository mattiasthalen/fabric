MODEL (
  enabled TRUE,
  kind VIEW
);

SELECT
  _pit_hook__customer__id,
  customer_id AS customer__customer_id,
  company_name AS customer__company_name,
  contact_name AS customer__contact_name,
  contact_title AS customer__contact_title,
  address AS customer__address,
  city AS customer__city,
  postal_code AS customer__postal_code,
  country AS customer__country,
  phone AS customer__phone,
  fax AS customer__fax,
  region AS customer__region,
  _dlt_load_id AS customer__dlt_load_id,
  _dlt_id AS customer__dlt_id,
  _record__loaded_at AS customer__record__loaded_at,
  _record__updated_at AS customer__record__updated_at,
  _record__version AS customer__record__version,
  _record__valid_from AS customer__record__valid_from,
  _record__valid_to AS customer__record__valid_to,
  _record__is_current AS customer__record__is_current
FROM dab.hook.frame__northwind__customers