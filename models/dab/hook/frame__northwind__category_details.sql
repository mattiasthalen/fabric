MODEL (
  enabled TRUE,
  kind VIEW
);

WITH cte__record_validity AS (
  SELECT
    @STAR(
      relation := das.scd.scd__northwind__category_details,
      exclude := [_record__loaded_at, _record__valid_from, _record__valid_to]
    ),
    _record__loaded_at,
    GREATEST(_record__valid_to, _record__loaded_at) AS _record__updated_at,
    _record__valid_from,
    COALESCE(_record__valid_to, @max_ts::TIMESTAMP) AS _record__valid_to,
    CASE WHEN _record__valid_to IS NULL THEN 1 ELSE 0 END AS _record__is_current,
    ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY _record__valid_from ASC) AS _record__version
  FROM das.scd.scd__northwind__category_details
), cte__hooks AS (
  SELECT
    CONCAT('northwind.category_detail.id|', category_id::TEXT) AS _hook__category__id,
    *
  FROM cte__record_validity
), cte__pit_hooks AS (
  SELECT
    CONCAT('epoch.timestamp|', _record__valid_from::TEXT, '~', _hook__category__id) AS _pit_hook__category__id,
    *
  FROM cte__hooks
)
SELECT
  _pit_hook__category__id,
  _hook__category__id,
  @STAR__LIST(
    table_name := das.scd.scd__northwind__category_details,
    exclude := [_record__loaded_at, _record__valid_from, _record__valid_to],
  ),
  _record__loaded_at,
  _record__updated_at,
  _record__valid_from,
  _record__valid_to,
  _record__is_current,
  _record__version
FROM cte__pit_hooks