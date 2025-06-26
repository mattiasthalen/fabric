MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (customer__record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
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
  _record__loaded_at AS customer___record__loaded_at,
  record_updated_at AS customer__record_updated_at,
  record_version AS customer__record_version,
  _record__valid_from AS customer___record__valid_from,
  _record__valid_to AS customer___record__valid_to,
  is_current_record AS customer__is_current_record
FROM dab.hook.frame__northwind__customers
WHERE
  1 = 1 AND customer__record_updated_at BETWEEN @start_ts AND @end_ts