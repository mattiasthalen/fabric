MODEL (
  enabled TRUE,
  kind VIEW
);

SELECT
  _pit_hook__category__detail,
  @STAR(
    relation := dab.hook.frame__northwind__category_details,
    exclude := _pit_hook__category__detail,
    prefix := 'category_details__'
  )
FROM dab.hook.frame__northwind__category_details