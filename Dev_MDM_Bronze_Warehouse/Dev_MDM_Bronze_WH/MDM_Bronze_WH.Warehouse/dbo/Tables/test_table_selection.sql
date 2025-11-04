CREATE TABLE [dbo].[test_table_selection] (

	[selection_id] bigint NULL, 
	[GUID] varchar(36) NULL, 
	[master_table_GUID] varchar(50) NULL, 
	[table_name] varchar(200) NULL, 
	[source_system] varchar(50) NULL, 
	[description] varchar(4000) NULL, 
	[table_type] varchar(50) NULL, 
	[selected] int NULL, 
	[suggested_table_name] varchar(200) NULL, 
	[master_table_id] bigint NULL, 
	[status] varchar(50) NULL, 
	[created_by] varchar(50) NULL, 
	[created_on] varchar(100) NULL, 
	[modified_by] varchar(50) NULL, 
	[modified_on] varchar(100) NULL, 
	[database_name] varchar(200) NULL, 
	[workspace_name] varchar(200) NULL
);