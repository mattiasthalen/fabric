MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (record_updated_at, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')
  ),
  enabled FALSE
);

WITH cte__union AS (
  @union(
    data_according_to_requirements.uss__staging._bridge__categories,
    data_according_to_requirements.uss__staging._bridge__category_details,
    data_according_to_requirements.uss__staging._bridge__products
  )
)
SELECT
  *
FROM cte__union