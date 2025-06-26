MODEL (
  enabled TRUE,
  kind VIEW
);

SELECT
  _pit_hook__supplier__id,
  supplier_id AS supplier__supplier_id,
  company_name AS supplier__company_name,
  contact_name AS supplier__contact_name,
  contact_title AS supplier__contact_title,
  address AS supplier__address,
  city AS supplier__city,
  postal_code AS supplier__postal_code,
  country AS supplier__country,
  phone AS supplier__phone,
  region AS supplier__region,
  home_page AS supplier__home_page,
  fax AS supplier__fax,
  _dlt_load_id AS supplier__dlt_load_id,
  _dlt_id AS supplier__dlt_id,
  _record__loaded_at AS supplier__record__loaded_at,
  _record__updated_at AS supplier__record__updated_at,
  _record__version AS supplier__record__version,
  _record__valid_from AS supplier__record__valid_from,
  _record__valid_to AS supplier__record__valid_to,
  _record__is_current AS supplier__record__is_current
FROM dab.hook.frame__northwind__suppliers