CREATE TABLE [sqlmesh__raw].[raw__raw__northwind__orders__446930732__dev] (

	[order_id] bigint NULL, 
	[customer_id] varchar(max) NULL, 
	[employee_id] bigint NULL, 
	[order_date] datetime2(6) NULL, 
	[required_date] datetime2(6) NULL, 
	[shipped_date] datetime2(6) NULL, 
	[ship_via] bigint NULL, 
	[freight] float NULL, 
	[ship_name] varchar(max) NULL, 
	[ship_address] varchar(max) NULL, 
	[ship_city] varchar(max) NULL, 
	[ship_postal_code] varchar(max) NULL, 
	[ship_country] varchar(max) NULL, 
	[_dlt_load_id] varchar(max) NULL, 
	[_dlt_id] varchar(max) NULL, 
	[ship_region] varchar(max) NULL, 
	[_record__hash] varbinary(max) NULL, 
	[_record__loaded_at] datetime2(6) NULL
);