MODEL (
  enabled TRUE,
  kind VIEW
);

@UNION_ALL_BY_NAME(
  dar.uss__staging._bridge__categories,
  dar.uss__staging._bridge__category_details,
  dar.uss__staging._bridge__products
)