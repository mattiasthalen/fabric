CREATE TABLE [sqlmesh__scd].[scd__scd__northwind__orders__635368499__dev] (

	[order_id] bigint NULL, 
	[customer_id] varchar(8000) NULL, 
	[employee_id] bigint NULL, 
	[order_date] datetime2(6) NULL, 
	[required_date] datetime2(6) NULL, 
	[shipped_date] datetime2(6) NULL, 
	[ship_via] bigint NULL, 
	[freight] float NULL, 
	[ship_name] varchar(8000) NULL, 
	[ship_address] varchar(8000) NULL, 
	[ship_city] varchar(8000) NULL, 
	[ship_postal_code] varchar(8000) NULL, 
	[ship_country] varchar(8000) NULL, 
	[_dlt_load_id] varchar(8000) NULL, 
	[_dlt_id] varchar(8000) NULL, 
	[ship_region] varchar(8000) NULL, 
	[_record__hash] varbinary(8000) NULL, 
	[_record__loaded_at] datetime2(6) NULL, 
	[_record__valid_from] datetime2(6) NULL, 
	[_record__valid_to] datetime2(6) NULL
);