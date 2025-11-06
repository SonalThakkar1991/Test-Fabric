CREATE TABLE [dbo].[column_list_cache] (

	[master_GUID] varchar(50) NOT NULL, 
	[column_name] varchar(200) NULL, 
	[master_table_id] int NOT NULL, 
	[column_id] bigint NOT NULL, 
	[GUID] varchar(50) NOT NULL
);