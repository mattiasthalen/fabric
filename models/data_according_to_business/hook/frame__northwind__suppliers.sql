MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

WITH cte__hooks AS (
  SELECT
    CONCAT('northwind.supplier.id|', supplier_id::TEXT) AS _hook__supplier__id,
    CONCAT('northwind.region.id|', region::TEXT) AS _hook__region__id,
    *
  FROM data_according_to_system.cdc.cdc__northwind__suppliers
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', record_valid_from::TEXT, '~', _hook__supplier__id) AS _pit_hook__supplier__id,
    *
  FROM cte__hooks
)
SELECT
  _pit_hook__supplier__id,
  _hook__supplier__id,
  _hook__region__id,
  supplier_id,
  company_name,
  contact_name,
  contact_title,
  address,
  city,
  postal_code,
  country,
  phone,
  region,
  home_page,
  fax,
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