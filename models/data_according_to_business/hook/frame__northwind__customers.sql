MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.customer.id|', customer_id::TEXT) AS _hook__customer__id,
    CONCAT('northwind.region.id|', region::TEXT) AS _hook__region__id,
    *
  FROM data_according_to_system.cdc.cdc__northwind__customers
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', record_valid_from::TEXT, '~', _hook__customer__id) AS _pit_hook__customer__id,
    *
  FROM cte__hooks
)
SELECT
  _pit_hook__customer__id,
  _hook__customer__id,
  _hook__region__id,
  customer_id,
  company_name,
  contact_name,
  contact_title,
  address,
  city,
  postal_code,
  country,
  phone,
  fax,
  region,
  _dlt_load_id,
  _dlt_id,
  record_loaded_at,
  record_updated_at,
  record_version,
  record_valid_from,
  record_valid_to,
  is_current_record
FROM cte__pit_hooks
WHERE
  1 = 1 AND record_updated_at BETWEEN @start_ts AND @end_ts