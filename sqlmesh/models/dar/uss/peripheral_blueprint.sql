MODEL (
  name dar.uss.@source,
  enabled TRUE,
  kind VIEW,
  blueprints (
    (source := northwind__categories, pit_hook := _pit_hook__category__id, prefix := 'category__'),
    (source := northwind__category_details, pit_hook := _pit_hook__category_detail__id, prefix := 'category_detail__'),
    (source := northwind__customers, pit_hook := _pit_hook__customer__id, prefix := 'customer__'),
    (source := northwind__employees, pit_hook := _pit_hook__employee__id, prefix := 'employee__'),
    (source := northwind__employee_territories, pit_hook := _pit_hook__employee__territory, prefix := 'employee_territory__'),
    (source := northwind__order_details, pit_hook := _pit_hook__order__product, prefix := 'order_detail__'),
    (source := northwind__orders, pit_hook := _pit_hook__order__id, prefix := 'order__'),
    (source := northwind__products, pit_hook := _pit_hook__product__id, prefix := 'product__'),
    (source := northwind__regions, pit_hook := _pit_hook__region__id, prefix := 'region__'),
    (source := northwind__shippers, pit_hook := _pit_hook__shipper__id, prefix := 'shipper__'),
    (source := northwind__suppliers, pit_hook := _pit_hook__supplier__id, prefix := 'supplier__'),
    (source := northwind__territories, pit_hook := _pit_hook__territory__id, prefix := 'territory__')
  )
);

SELECT
  @pit_hook,
  @STAR(
    relation := dab.hook.frame__@source,
    exclude := @pit_hook,
    prefix := @prefix
  )
FROM dab.hook.frame__@source