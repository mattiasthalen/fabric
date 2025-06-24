MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (customer__record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

SELECT
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
  record_loaded_at AS customer__record_loaded_at,
  record_updated_at AS customer__record_updated_at,
  record_version AS customer__record_version,
  record_valid_from AS customer__record_valid_from,
  record_valid_to AS customer__record_valid_to,
  is_current_record AS customer__is_current_record
FROM data_according_to_business.hook.frame__northwind__customers
WHERE
  1 = 1 AND customer__record_updated_at BETWEEN @start_ts AND @end_ts