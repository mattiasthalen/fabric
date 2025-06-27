MODEL (
  name das.scd.scd__@source,
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _record__hash
  ),
  blueprints (
    (source := northwind__categories, @unique_key := category_id),
    (source := northwind__category_details, @unique_key := category_id),
    (source := northwind__customers, @unique_key := customer_id),
    (source := northwind__employees, @unique_key := employee_id),
    (
      source := northwind__employee_territories,
      @unique_key := CONCAT(_get_northwindapiv_1_employees_employee_id::TEXT, '|', territory_id::TEXT)
    ),
    (
      source := northwind__order_details,
      @unique_key := CONCAT(order_id::TEXT, '|', product_id::TEXT)
    ),
    (source := northwind__orders, @unique_key := order_id),
    (source := northwind__products, @unique_key := product_id),
    (source := northwind__regions, @unique_key := region_id),
    (source := northwind__shippers, @unique_key := shipper_id),
    (source := northwind__suppliers, @unique_key := supplier_id),
    (source := northwind__territories, @unique_key := territory_id)
  )
);

WITH cte__source AS (
  SELECT
    *
  FROM das.raw.raw__@source
), cte__changed_ids AS (
  SELECT DISTINCT
    @unique_key
  FROM cte__source
  WHERE
    _record__loaded_at BETWEEN @start_ts AND @end_ts
), cte__changed_data AS (
  SELECT
    *
  FROM cte__source
  INNER JOIN cte__changed_ids
    USING (@unique_key)
), cte__before_window AS (
  SELECT
    *
  FROM cte__changed_data
  WHERE
    _record__loaded_at < @start_ts
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY @unique_key ORDER BY _record__loaded_at DESC) = 1
), cte__current_window AS (
  SELECT
    *
  FROM cte__changed_data
  WHERE
    _record__loaded_at BETWEEN @start_ts AND @end_ts
), cte__after_windows AS (
  SELECT
    *
  FROM cte__changed_data
  WHERE
    _record__loaded_at > @end_ts
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY @unique_key ORDER BY _record__loaded_at ASC) = 1
), cte__union AS (
  SELECT
    *
  FROM cte__before_window
  UNION ALL
  SELECT
    *
  FROM cte__current_window
  UNION ALL
  SELECT
    *
  FROM cte__after_windows
), cte__scd AS (
  SELECT
    *,
    COALESCE(
      LEAD(_record__loaded_at) OVER (PARTITION BY @unique_key ORDER BY _record__loaded_at),
      _record__loaded_at
    ) AS _record__updated_at,
    COALESCE(
     LAG(_record__loaded_at) OVER (PARTITION BY @unique_key ORDER BY _record__loaded_at),
     '1970-01-01 00:00:00'::TIMESTAMP
    ) AS _record__valid_from,
    COALESCE(
     LEAD(_record__loaded_at) OVER (PARTITION BY @unique_key ORDER BY _record__loaded_at),
     '9999-12-31 23:59:59'::TIMESTAMP
    ) AS _record__valid_to,
    ROW_NUMBER() OVER (PARTITION BY @unique_key ORDER BY _record__loaded_at DESC) AS _record__version,
    CASE
     WHEN LEAD(_record__loaded_at) OVER (PARTITION BY @unique_key ORDER BY _record__loaded_at) IS NULL
     THEN 1
     ELSE 0
    END AS _record__is_current
  FROM cte__union
)
SELECT
  @STAR__LIST(table_name := das.raw.raw__@source),
  _record__updated_at,
  _record__valid_from,
  _record__valid_to,
  _record__version,
  _record__is_current
FROM cte__scd
WHERE
  1 = 1 AND _record__updated_at BETWEEN @start_ts AND @end_ts