MODEL (
  name @target_table,
  enabled TRUE,
  kind VIEW,
  blueprints (
    (source_table := dab.hook.frame__northwind__categories,           target_table := dar.uss.categories,           pit_hook := _pit_hook__category__id,        prefix := 'category__'),
    (source_table := dab.hook.frame__northwind__customers,            target_table := dar.uss.customers,            pit_hook := _pit_hook__customer__id,        prefix := 'customer__'),
    (source_table := dab.hook.frame__northwind__employees,            target_table := dar.uss.employees,            pit_hook := _pit_hook__employee__id,        prefix := 'employee__'),
    (source_table := dab.hook.frame__northwind__employee_territories, target_table := dar.uss.employee_territories, pit_hook := _pit_hook__employee__territory, prefix := 'employee_territory__'),
    (source_table := dab.hook.frame__northwind__order_details,        target_table := dar.uss.order_details,        pit_hook := _pit_hook__order__product,      prefix := 'order_detail__'),
    (source_table := dab.hook.frame__northwind__orders,               target_table := dar.uss.orders,               pit_hook := _pit_hook__order__id,           prefix := 'order__'),
    (source_table := dab.hook.frame__northwind__products,             target_table := dar.uss.products,             pit_hook := _pit_hook__product__id,         prefix := 'product__'),
    (source_table := dab.hook.frame__northwind__regions,              target_table := dar.uss.regions,              pit_hook := _pit_hook__region__id,          prefix := 'region__'),
    (source_table := dab.hook.frame__northwind__shippers,             target_table := dar.uss.shippers,             pit_hook := _pit_hook__shipper__id,         prefix := 'shipper__'),
    (source_table := dab.hook.frame__northwind__suppliers,            target_table := dar.uss.suppliers,            pit_hook := _pit_hook__supplier__id,        prefix := 'supplier__'),
    (source_table := dab.hook.frame__northwind__territories,          target_table := dar.uss.territories,          pit_hook := _pit_hook__territory__id,       prefix := 'territory__')
  )
);

SELECT
  @pit_hook,
  @STAR(
    relation := @source_table,
    exclude := @pit_hook,
    prefix := @prefix
  )
FROM @source_table