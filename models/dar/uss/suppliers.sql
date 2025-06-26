MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (supplier__record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
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
  _record__loaded_at AS supplier___record__loaded_at,
  record_updated_at AS supplier__record_updated_at,
  record_version AS supplier__record_version,
  _record__valid_from AS supplier___record__valid_from,
  _record__valid_to AS supplier___record__valid_to,
  is_current_record AS supplier__is_current_record
FROM dab.hook.frame__northwind__suppliers
WHERE
  1 = 1 AND supplier__record_updated_at BETWEEN @start_ts AND @end_ts