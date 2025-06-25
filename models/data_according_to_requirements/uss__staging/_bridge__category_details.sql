MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  )
);

SELECT
  'northwind__category_details' AS peripheral,
  _pit_hook__category__id AS _pit_hook__category_detail__id,
  record_updated_at,
  record_valid_from,
  record_valid_to,
  is_current_record
FROM data_according_to_business.hook.frame__northwind__category_details