MODEL (
  enabled TRUE,
  kind VIEW
);

SELECT
  _pit_hook__category__id AS _pit_hook__category_detail__id,
  @STAR(
    relation := dab.hook.frame__northwind__category_details,
    exclude := _pit_hook__category__id,
    prefix := 'category_details__'
  )
FROM dab.hook.frame__northwind__category_details