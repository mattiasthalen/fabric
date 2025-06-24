MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (supplier__record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

SELECT
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
  record_loaded_at AS supplier__record_loaded_at,
  record_updated_at AS supplier__record_updated_at,
  record_version AS supplier__record_version,
  record_valid_from AS supplier__record_valid_from,
  record_valid_to AS supplier__record_valid_to,
  is_current_record AS supplier__is_current_record
FROM data_according_to_business.hook.frame__northwind__suppliers
WHERE
  1 = 1 AND supplier__record_updated_at BETWEEN @start_ts AND @end_ts