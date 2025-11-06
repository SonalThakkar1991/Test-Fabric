CREATE TABLE [dbo].[metacontrol_v3] (

	[master_GUID] varchar(50) NOT NULL, 
	[table_name] varchar(200) NULL, 
	[table_description] varchar(4000) NULL, 
	[column_name] varchar(200) NULL, 
	[column_description] varchar(4000) NULL, 
	[master_table_id] bigint NOT NULL, 
	[source_system] varchar(200) NULL, 
	[workspace_name] varchar(200) NULL, 
	[database_name] varchar(200) NULL, 
	[column_id] bigint NOT NULL, 
	[GUID] varchar(50) NOT NULL
);