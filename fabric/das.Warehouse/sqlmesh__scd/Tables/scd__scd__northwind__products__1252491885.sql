CREATE TABLE [sqlmesh__scd].[scd__scd__northwind__products__1252491885] (

	[product_id] bigint NULL, 
	[product_name] varchar(max) NULL, 
	[supplier_id] bigint NULL, 
	[category_id] bigint NULL, 
	[quantity_per_unit] varchar(max) NULL, 
	[unit_price] float NULL, 
	[units_in_stock] bigint NULL, 
	[units_on_order] bigint NULL, 
	[reorder_level] bigint NULL, 
	[discontinued] bit NULL, 
	[_dlt_load_id] varchar(max) NULL, 
	[_dlt_id] varchar(max) NULL, 
	[_record__hash] varbinary(max) NULL, 
	[_record__loaded_at] datetime2(6) NULL, 
	[_record__updated_at] datetime2(6) NULL, 
	[_record__valid_from] datetime2(6) NULL, 
	[_record__valid_to] datetime2(6) NULL, 
	[_record__version] bigint NULL, 
	[_record__is_current] int NULL
);