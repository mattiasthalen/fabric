MODEL (
  enabled FALSE,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (category_detail__record_updated_at, '%Y-%m-%d %H:%M:%S.%f')
  )
);

SELECT
  _pit_hook__category__id AS _pit_hook__category_detail__id,
  category_id AS category_detail__category_id,
  category_name AS category_detail__category_name,
  description AS category_detail__description,
  picture AS category_detail__picture,
  product_names AS category_detail__product_names,
  _dlt_load_id AS category_detail__dlt_load_id,
  _dlt_id AS category_detail__dlt_id,
  _record__loaded_at AS category_detail___record__loaded_at,
  record_updated_at AS category_detail__record_updated_at,
  record_version AS category_detail__record_version,
  _record__valid_from AS category_detail___record__valid_from,
  _record__valid_to AS category_detail___record__valid_to,
  is_current_record AS category_detail__is_current_record
FROM dab.hook.frame__northwind__category_details
WHERE
  1 = 1 AND category_detail__record_updated_at BETWEEN @start_ts AND @end_ts